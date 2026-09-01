//! Arrow output support for query results.
//!
//! The bridge is read-only: it maps an already decoded [`DataChunk`] onto
//! [`arrow_array::RecordBatch`] using the column [`LogicalType`]s, so the wire
//! codecs are untouched. It is gated behind the `arrow` feature.

use std::iter::zip;
use std::sync::Arc;

use arrow_array::builder::{BinaryBuilder, BooleanBuilder, PrimitiveBuilder, StringBuilder};
use arrow_array::temporal_conversions::{MICROSECONDS_IN_DAY, NANOSECONDS_IN_DAY};
use arrow_array::types::{
    Date32Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int8Type, Int16Type,
    Int32Type, Int64Type, IntervalMonthDayNanoType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow_array::{
    ArrayRef, ArrowPrimitiveType, BooleanArray, FixedSizeListArray, ListArray, NullArray,
    PrimitiveArray, RecordBatch, RecordBatchOptions, StructArray,
};
use arrow_buffer::{IntervalMonthDayNano, NullBuffer, OffsetBuffer, i256};
use arrow_schema::{
    ArrowError, DataType, Field, FieldRef, Fields, IntervalUnit, Schema, SchemaRef,
    TimeUnit as ArrowTimeUnit,
};
use futures_util::StreamExt;
use futures_util::stream::BoxStream;

use crate::builders::ColumnDefinition;
use crate::client::QuackResultStream;
use crate::errors::{QuackError, Result};
use crate::logical_types::{
    LogicalType, LogicalTypeId, get_array_size, get_child_type, get_decimal_width_and_scale,
    get_struct_children,
};
use crate::vector::{DataChunk, DecimalValue, TimeUnit, TimestampUnit, Value};

pub use arrow_array;
pub use arrow_buffer;
pub use arrow_schema;

const NULL_VALUE: Value = Value::Null;
const HUGE_INT_PRECISION: u8 = 39;
const UTC_TIMEZONE: &str = "UTC";
const NANOS_PER_MICRO: i64 = 1_000;

/// Builds the Arrow schema of a query result from its column definitions.
///
/// The definitions come from prepare time, so the schema is available even for
/// results that carry no chunks at all.
pub fn schema(columns: &[ColumnDefinition]) -> Result<SchemaRef> {
    let fields = columns
        .iter()
        .map(|column| field(&column.name, &column.logical_type))
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

/// Converts a decoded chunk into a [`RecordBatch`] with the given schema.
///
/// Returns an error if the chunk's column types do not match the schema.
pub fn to_record_batch(chunk: &DataChunk, schema: &SchemaRef) -> Result<RecordBatch> {
    if chunk.types.len() != schema.fields().len() {
        return Err(QuackError::protocol(format!(
            "DataChunk has {} columns but the Arrow schema declares {}",
            chunk.types.len(),
            schema.fields().len()
        )));
    }
    let mut arrays = Vec::with_capacity(chunk.types.len());
    for (index, (logical_type, field)) in zip(&chunk.types, schema.fields()).enumerate() {
        let values = chunk.column_values(index).ok_or_else(|| {
            QuackError::protocol(format!("DataChunk is missing column vector {index}"))
        })?;
        if values.len() != chunk.row_count {
            return Err(QuackError::protocol(format!(
                "column {index} has {} values, expected {}",
                values.len(),
                chunk.row_count
            )));
        }
        arrays.push(build_array(
            logical_type,
            field.data_type(),
            &values.iter().collect::<Vec<_>>(),
        )?);
    }
    RecordBatch::try_new_with_options(
        schema.clone(),
        arrays,
        &RecordBatchOptions::new().with_row_count(Some(chunk.row_count)),
    )
    .map_err(arrow_error)
}

/// Maps a Quack logical type onto the Arrow type the bridge produces for it.
///
/// Two mappings are provisional and may change in a future release: `MAP` is
/// encoded as `List<Struct<key, value>>` rather than Arrow's native `Map`, and
/// `ENUM` as plain `Utf8` rather than a `Dictionary`.
pub fn arrow_type(logical_type: &LogicalType) -> Result<DataType> {
    Ok(match logical_type.id {
        LogicalTypeId::SqlNull => DataType::Null,
        LogicalTypeId::Boolean => DataType::Boolean,
        LogicalTypeId::TinyInt => DataType::Int8,
        LogicalTypeId::SmallInt => DataType::Int16,
        LogicalTypeId::Integer => DataType::Int32,
        LogicalTypeId::BigInt => DataType::Int64,
        LogicalTypeId::UTinyInt => DataType::UInt8,
        LogicalTypeId::USmallInt => DataType::UInt16,
        LogicalTypeId::UInteger => DataType::UInt32,
        LogicalTypeId::UBigInt => DataType::UInt64,
        LogicalTypeId::HugeInt | LogicalTypeId::UHugeInt => {
            DataType::Decimal256(HUGE_INT_PRECISION, 0)
        }
        LogicalTypeId::Float => DataType::Float32,
        LogicalTypeId::Double => DataType::Float64,
        LogicalTypeId::Decimal => {
            let (precision, scale) = decimal_precision_and_scale(logical_type)?;
            DataType::Decimal128(precision, scale)
        }
        LogicalTypeId::Varchar
        | LogicalTypeId::Char
        | LogicalTypeId::Enum
        | LogicalTypeId::Uuid => DataType::Utf8,
        LogicalTypeId::Blob | LogicalTypeId::Geometry | LogicalTypeId::Bit => DataType::Binary,
        LogicalTypeId::Date => DataType::Date32,
        LogicalTypeId::Time => DataType::Time64(ArrowTimeUnit::Microsecond),
        LogicalTypeId::TimeNs => DataType::Time64(ArrowTimeUnit::Nanosecond),
        LogicalTypeId::TimestampSec => DataType::Timestamp(ArrowTimeUnit::Second, None),
        LogicalTypeId::TimestampMs => DataType::Timestamp(ArrowTimeUnit::Millisecond, None),
        LogicalTypeId::Timestamp => DataType::Timestamp(ArrowTimeUnit::Microsecond, None),
        LogicalTypeId::TimestampNs => DataType::Timestamp(ArrowTimeUnit::Nanosecond, None),
        LogicalTypeId::TimestampTz => {
            DataType::Timestamp(ArrowTimeUnit::Microsecond, Some(UTC_TIMEZONE.into()))
        }
        LogicalTypeId::Interval => DataType::Interval(IntervalUnit::MonthDayNano),
        LogicalTypeId::List | LogicalTypeId::Map => DataType::List(list_item_field(logical_type)?),
        LogicalTypeId::Array => {
            DataType::FixedSizeList(list_item_field(logical_type)?, array_size(logical_type)?)
        }
        LogicalTypeId::Struct => DataType::Struct(struct_fields(logical_type)?),
        other => return Err(unsupported(other)),
    })
}

impl QuackResultStream {
    /// Consumes the stream as Arrow record batches sharing a single schema.
    pub fn into_record_batches(
        self,
    ) -> Result<(SchemaRef, BoxStream<'static, Result<RecordBatch>>)> {
        let (columns, chunks) = self.into_chunks();
        let schema = schema(&columns)?;
        let batch_schema = schema.clone();
        let batches = chunks
            .map(move |chunk| chunk.and_then(|chunk| to_record_batch(&chunk, &batch_schema)))
            .boxed();
        Ok((schema, batches))
    }
}

fn field(name: &str, logical_type: &LogicalType) -> Result<FieldRef> {
    Ok(Arc::new(Field::new(name, arrow_type(logical_type)?, true)))
}

fn list_item_field(logical_type: &LogicalType) -> Result<FieldRef> {
    Ok(Arc::new(Field::new_list_field(
        arrow_type(get_child_type(logical_type)?)?,
        true,
    )))
}

fn struct_fields(logical_type: &LogicalType) -> Result<Fields> {
    Ok(get_struct_children(logical_type)?
        .iter()
        .map(|child| field(&child.name, &child.logical_type))
        .collect::<Result<Vec<_>>>()?
        .into())
}

fn array_size(logical_type: &LogicalType) -> Result<i32> {
    let size = get_array_size(logical_type)?;
    i32::try_from(size)
        .map_err(|_| QuackError::protocol(format!("ARRAY size {size} exceeds the Arrow limit")))
}

fn decimal_precision_and_scale(logical_type: &LogicalType) -> Result<(u8, i8)> {
    let (width, scale) = get_decimal_width_and_scale(logical_type)?;
    match (u8::try_from(width), i8::try_from(scale)) {
        (Ok(width), Ok(scale)) => Ok((width, scale)),
        _ => Err(QuackError::protocol(format!(
            "DECIMAL({width}, {scale}) exceeds the Arrow decimal limits"
        ))),
    }
}

/// Builds one column. `data_type` is the schema's type for the column, so
/// nested builders reuse the schema's own field objects instead of rebuilding
/// an identical field tree for every chunk.
fn build_array(
    logical_type: &LogicalType,
    data_type: &DataType,
    values: &[&Value],
) -> Result<ArrayRef> {
    let id = logical_type.id;
    Ok(match id {
        LogicalTypeId::SqlNull => Arc::new(NullArray::new(values.len())),
        LogicalTypeId::Boolean => Arc::new(boolean(values, |value| as_bool(id, value))?),
        LogicalTypeId::TinyInt => Arc::new(primitive::<Int8Type>(values, |value| {
            narrow_int(id, value)
        })?),
        LogicalTypeId::SmallInt => Arc::new(primitive::<Int16Type>(values, |value| {
            narrow_int(id, value)
        })?),
        LogicalTypeId::Integer => Arc::new(primitive::<Int32Type>(values, |value| {
            narrow_int(id, value)
        })?),
        LogicalTypeId::BigInt => {
            Arc::new(primitive::<Int64Type>(values, |value| as_int(id, value))?)
        }
        LogicalTypeId::UTinyInt => Arc::new(primitive::<UInt8Type>(values, |value| {
            narrow_uint(id, value)
        })?),
        LogicalTypeId::USmallInt => Arc::new(primitive::<UInt16Type>(values, |value| {
            narrow_uint(id, value)
        })?),
        LogicalTypeId::UInteger => Arc::new(primitive::<UInt32Type>(values, |value| {
            narrow_uint(id, value)
        })?),
        LogicalTypeId::UBigInt => {
            Arc::new(primitive::<UInt64Type>(values, |value| as_uint(id, value))?)
        }
        LogicalTypeId::HugeInt => Arc::new(
            primitive::<Decimal256Type>(values, |value| as_huge_int(id, value))?
                .with_precision_and_scale(HUGE_INT_PRECISION, 0)
                .map_err(arrow_error)?,
        ),
        LogicalTypeId::UHugeInt => Arc::new(
            primitive::<Decimal256Type>(values, |value| as_unsigned_huge_int(id, value))?
                .with_precision_and_scale(HUGE_INT_PRECISION, 0)
                .map_err(arrow_error)?,
        ),
        LogicalTypeId::Float => Arc::new(primitive::<Float32Type>(values, |value| {
            as_float(id, value)
        })?),
        LogicalTypeId::Double => Arc::new(primitive::<Float64Type>(values, |value| {
            as_double(id, value)
        })?),
        LogicalTypeId::Decimal => {
            let (precision, scale) = decimal_precision_and_scale(logical_type)?;
            Arc::new(
                primitive::<Decimal128Type>(values, |value| {
                    as_decimal(id, value).map(|decimal| decimal.value)
                })?
                .with_precision_and_scale(precision, scale)
                .map_err(arrow_error)?,
            )
        }
        LogicalTypeId::Varchar
        | LogicalTypeId::Char
        | LogicalTypeId::Enum
        | LogicalTypeId::Uuid => {
            let strings = collect(values, |value| as_str(id, value))?;
            let mut builder =
                StringBuilder::with_capacity(strings.len(), byte_capacity(id, &strings)?);
            for value in &strings {
                builder.append_option(*value);
            }
            Arc::new(builder.finish())
        }
        LogicalTypeId::Blob | LogicalTypeId::Geometry | LogicalTypeId::Bit => {
            let blobs = collect(values, |value| as_bytes(id, value))?;
            let mut builder = BinaryBuilder::with_capacity(blobs.len(), byte_capacity(id, &blobs)?);
            for value in &blobs {
                builder.append_option(*value);
            }
            Arc::new(builder.finish())
        }
        LogicalTypeId::Date => {
            Arc::new(primitive::<Date32Type>(values, |value| as_date(id, value))?)
        }
        LogicalTypeId::Time => Arc::new(primitive::<Time64MicrosecondType>(values, |value| {
            as_time(id, value, TimeUnit::Micros)
        })?),
        LogicalTypeId::TimeNs => Arc::new(primitive::<Time64NanosecondType>(values, |value| {
            as_time(id, value, TimeUnit::Nanos)
        })?),
        LogicalTypeId::TimestampSec => {
            Arc::new(primitive::<TimestampSecondType>(values, |value| {
                as_timestamp(id, value, TimestampUnit::Seconds, false)
            })?)
        }
        LogicalTypeId::TimestampMs => {
            Arc::new(primitive::<TimestampMillisecondType>(values, |value| {
                as_timestamp(id, value, TimestampUnit::Millis, false)
            })?)
        }
        LogicalTypeId::Timestamp => {
            Arc::new(primitive::<TimestampMicrosecondType>(values, |value| {
                as_timestamp(id, value, TimestampUnit::Micros, false)
            })?)
        }
        LogicalTypeId::TimestampNs => {
            Arc::new(primitive::<TimestampNanosecondType>(values, |value| {
                as_timestamp(id, value, TimestampUnit::Nanos, false)
            })?)
        }
        LogicalTypeId::TimestampTz => Arc::new(
            primitive::<TimestampMicrosecondType>(values, |value| {
                as_timestamp(id, value, TimestampUnit::Micros, true)
            })?
            .with_timezone(UTC_TIMEZONE),
        ),
        LogicalTypeId::Interval => {
            Arc::new(primitive::<IntervalMonthDayNanoType>(values, |value| {
                as_interval(id, value)
            })?)
        }
        LogicalTypeId::List | LogicalTypeId::Map => {
            build_list(logical_type, list_item(data_type)?, values)?
        }
        LogicalTypeId::Array => {
            let (item, size) = fixed_size_list_item(data_type)?;
            build_fixed_size_list(logical_type, item, size, values)?
        }
        LogicalTypeId::Struct => build_struct(logical_type, struct_children(data_type)?, values)?,
        other => return Err(unsupported(other)),
    })
}

fn build_list(logical_type: &LogicalType, item: &FieldRef, values: &[&Value]) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(values.len() + 1);
    let mut validity = Vec::with_capacity(values.len());
    let mut items = Vec::with_capacity(values.len());
    offsets.push(0i32);
    for value in values {
        match *value {
            Value::Null => validity.push(false),
            Value::List(entries) => {
                items.extend(entries.iter());
                validity.push(true);
            }
            other => return Err(mismatch(logical_type.id, other)),
        }
        offsets.push(i32::try_from(items.len()).map_err(|_| {
            QuackError::protocol("LIST column exceeds the Arrow 32-bit offset limit")
        })?);
    }
    let child = build_array(get_child_type(logical_type)?, item.data_type(), &items)?;
    Ok(Arc::new(
        ListArray::try_new(
            item.clone(),
            OffsetBuffer::new(offsets.into()),
            child,
            Some(NullBuffer::from(validity)),
        )
        .map_err(arrow_error)?,
    ))
}

fn build_fixed_size_list(
    logical_type: &LogicalType,
    item: &FieldRef,
    size: i32,
    values: &[&Value],
) -> Result<ArrayRef> {
    let width = usize::try_from(size)
        .map_err(|_| QuackError::protocol(format!("Arrow schema declares ARRAY size {size}")))?;
    let mut validity = Vec::with_capacity(values.len());
    let mut items = Vec::with_capacity(values.len() * width);
    for value in values {
        match *value {
            Value::Null => {
                items.extend(std::iter::repeat_n(&NULL_VALUE, width));
                validity.push(false);
            }
            Value::List(entries) => {
                if entries.len() != width {
                    return Err(QuackError::protocol(format!(
                        "ARRAY value has {} entries, expected {width}",
                        entries.len()
                    )));
                }
                items.extend(entries.iter());
                validity.push(true);
            }
            other => return Err(mismatch(logical_type.id, other)),
        }
    }
    let child = build_array(get_child_type(logical_type)?, item.data_type(), &items)?;
    Ok(Arc::new(
        FixedSizeListArray::try_new(item.clone(), size, child, Some(NullBuffer::from(validity)))
            .map_err(arrow_error)?,
    ))
}

fn build_struct(
    logical_type: &LogicalType,
    fields: &Fields,
    values: &[&Value],
) -> Result<ArrayRef> {
    let mut validity = Vec::with_capacity(values.len());
    for value in values {
        match *value {
            Value::Null => validity.push(false),
            Value::Struct(_) => validity.push(true),
            other => return Err(mismatch(logical_type.id, other)),
        }
    }
    let children = get_struct_children(logical_type)?;
    if children.len() != fields.len() {
        return Err(QuackError::protocol(format!(
            "STRUCT has {} children but the Arrow schema declares {}",
            children.len(),
            fields.len()
        )));
    }
    let mut arrays = Vec::with_capacity(children.len());
    for (child_index, (child, field)) in zip(children, fields).enumerate() {
        // The decoder inserts struct fields in child order, so the positional
        // lookup hits and the lookup by name is only a fallback.
        let child_values = values
            .iter()
            .map(|value| match *value {
                Value::Struct(row) => match row.get_index(child_index) {
                    Some((name, value)) if name == &child.name => value,
                    _ => row.get(&child.name).unwrap_or(&NULL_VALUE),
                },
                _ => &NULL_VALUE,
            })
            .collect::<Vec<_>>();
        arrays.push(build_array(
            &child.logical_type,
            field.data_type(),
            &child_values,
        )?);
    }
    Ok(Arc::new(
        StructArray::try_new_with_length(
            fields.clone(),
            arrays,
            Some(NullBuffer::from(validity)),
            values.len(),
        )
        .map_err(arrow_error)?,
    ))
}

fn list_item(data_type: &DataType) -> Result<&FieldRef> {
    match data_type {
        DataType::List(item) => Ok(item),
        other => Err(schema_shape("List", other)),
    }
}

fn fixed_size_list_item(data_type: &DataType) -> Result<(&FieldRef, i32)> {
    match data_type {
        DataType::FixedSizeList(item, size) => Ok((item, *size)),
        other => Err(schema_shape("FixedSizeList", other)),
    }
}

fn struct_children(data_type: &DataType) -> Result<&Fields> {
    match data_type {
        DataType::Struct(fields) => Ok(fields),
        other => Err(schema_shape("Struct", other)),
    }
}

fn primitive<'a, T: ArrowPrimitiveType>(
    values: &[&'a Value],
    extract: impl Fn(&'a Value) -> Result<T::Native>,
) -> Result<PrimitiveArray<T>> {
    let mut builder = PrimitiveBuilder::<T>::with_capacity(values.len());
    for value in values {
        match *value {
            Value::Null => builder.append_null(),
            value => builder.append_value(extract(value)?),
        }
    }
    Ok(builder.finish())
}

fn boolean<'a>(
    values: &[&'a Value],
    extract: impl Fn(&'a Value) -> Result<bool>,
) -> Result<BooleanArray> {
    let mut builder = BooleanBuilder::with_capacity(values.len());
    for value in values {
        match *value {
            Value::Null => builder.append_null(),
            value => builder.append_value(extract(value)?),
        }
    }
    Ok(builder.finish())
}

fn collect<'a, T>(
    values: &[&'a Value],
    extract: impl Fn(&'a Value) -> Result<T>,
) -> Result<Vec<Option<T>>> {
    values
        .iter()
        .map(|value| match value {
            Value::Null => Ok(None),
            value => extract(value).map(Some),
        })
        .collect()
}

/// Total byte length of a string or blob column, rejecting columns that would
/// overflow Arrow's 32-bit offsets (the builders panic rather than error).
fn byte_capacity<T: AsRef<[u8]>>(id: LogicalTypeId, values: &[Option<T>]) -> Result<usize> {
    let total: usize = values
        .iter()
        .flatten()
        .map(|value| value.as_ref().len())
        .sum();
    if total > i32::MAX as usize {
        return Err(QuackError::protocol(format!(
            "{id:?} column of {total} bytes exceeds the Arrow 32-bit offset limit"
        )));
    }
    Ok(total)
}

fn as_bool(id: LogicalTypeId, value: &Value) -> Result<bool> {
    match value {
        Value::Bool(value) => Ok(*value),
        other => Err(mismatch(id, other)),
    }
}

fn as_int(id: LogicalTypeId, value: &Value) -> Result<i64> {
    match value {
        Value::Int(value) => Ok(*value),
        other => Err(mismatch(id, other)),
    }
}

fn as_uint(id: LogicalTypeId, value: &Value) -> Result<u64> {
    match value {
        Value::UInt(value) => Ok(*value),
        other => Err(mismatch(id, other)),
    }
}

fn as_huge_int(id: LogicalTypeId, value: &Value) -> Result<i256> {
    match value {
        Value::HugeInt(value) => Ok(i256::from_i128(*value)),
        other => Err(mismatch(id, other)),
    }
}

fn as_unsigned_huge_int(id: LogicalTypeId, value: &Value) -> Result<i256> {
    match value {
        Value::UHugeInt(value) => Ok(i256::from_parts(*value, 0)),
        other => Err(mismatch(id, other)),
    }
}

fn as_float(id: LogicalTypeId, value: &Value) -> Result<f32> {
    match value {
        Value::Float(value) => Ok(*value),
        other => Err(mismatch(id, other)),
    }
}

fn as_double(id: LogicalTypeId, value: &Value) -> Result<f64> {
    match value {
        Value::Double(value) => Ok(*value),
        other => Err(mismatch(id, other)),
    }
}

fn as_decimal(id: LogicalTypeId, value: &Value) -> Result<&DecimalValue> {
    match value {
        Value::Decimal(value) => Ok(value),
        other => Err(mismatch(id, other)),
    }
}

fn as_str(id: LogicalTypeId, value: &Value) -> Result<&str> {
    match value {
        Value::String(value) => Ok(value.as_str()),
        other => Err(mismatch(id, other)),
    }
}

fn as_bytes(id: LogicalTypeId, value: &Value) -> Result<&[u8]> {
    match value {
        Value::Bytes(value) => Ok(value.as_slice()),
        other => Err(mismatch(id, other)),
    }
}

fn as_date(id: LogicalTypeId, value: &Value) -> Result<i32> {
    match value {
        Value::Date(value) => Ok(value.days),
        other => Err(mismatch(id, other)),
    }
}

fn narrow_int<T: TryFrom<i64>>(id: LogicalTypeId, value: &Value) -> Result<T> {
    let value = as_int(id, value)?;
    T::try_from(value).map_err(|_| out_of_range(id, value))
}

fn narrow_uint<T: TryFrom<u64>>(id: LogicalTypeId, value: &Value) -> Result<T> {
    let value = as_uint(id, value)?;
    T::try_from(value).map_err(|_| out_of_range(id, value))
}

fn as_time(id: LogicalTypeId, value: &Value, unit: TimeUnit) -> Result<i64> {
    match value {
        Value::Time(value) if value.unit == unit => {
            // DuckDB accepts TIME '24:00:00', which Arrow's "elapsed time since
            // midnight" bound excludes.
            let limit = match unit {
                TimeUnit::Micros => MICROSECONDS_IN_DAY,
                TimeUnit::Nanos => NANOSECONDS_IN_DAY,
            };
            if !(0..limit).contains(&value.value) {
                return Err(out_of_range(id, value.value));
            }
            Ok(value.value)
        }
        other => Err(mismatch(id, other)),
    }
}

fn as_timestamp(
    id: LogicalTypeId,
    value: &Value,
    unit: TimestampUnit,
    timezone_utc: bool,
) -> Result<i64> {
    match value {
        Value::Timestamp(value) if value.unit == unit && value.timezone_utc == timezone_utc => {
            Ok(value.value)
        }
        other => Err(mismatch(id, other)),
    }
}

fn as_interval(id: LogicalTypeId, value: &Value) -> Result<IntervalMonthDayNano> {
    match value {
        Value::Interval(value) => {
            let nanoseconds = value.micros.checked_mul(NANOS_PER_MICRO).ok_or_else(|| {
                QuackError::protocol(format!(
                    "INTERVAL of {} microseconds overflows Arrow nanosecond resolution",
                    value.micros
                ))
            })?;
            Ok(IntervalMonthDayNano::new(
                value.months,
                value.days,
                nanoseconds,
            ))
        }
        other => Err(mismatch(id, other)),
    }
}

fn unsupported(id: LogicalTypeId) -> QuackError {
    QuackError::unsupported(format!("logical type {id:?} has no Arrow mapping"))
}

fn mismatch(id: LogicalTypeId, value: &Value) -> QuackError {
    QuackError::protocol(format!(
        "decoded value {value:?} does not match logical type {id:?}"
    ))
}

fn out_of_range(id: LogicalTypeId, value: impl std::fmt::Display) -> QuackError {
    QuackError::protocol(format!(
        "decoded value {value} is out of range for logical type {id:?}"
    ))
}

fn schema_shape(expected: &str, actual: &DataType) -> QuackError {
    QuackError::protocol(format!(
        "Arrow schema declares {actual} where {expected} is required"
    ))
}

fn arrow_error(error: ArrowError) -> QuackError {
    QuackError::protocol(format!("arrow conversion failed: {error}"))
}

#[cfg(test)]
mod tests {
    use arrow_array::cast::AsArray;
    use arrow_array::{
        Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
        Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
        IntervalMonthDayNanoArray, NullArray, StringArray, Time64MicrosecondArray,
        Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array,
        UInt64Array,
    };
    use indexmap::IndexMap;

    use super::*;
    use crate::builders::{column, data_chunk};
    use crate::logical_types::{ChildType, LogicalTypes};
    use crate::values::{date_value, decimal_value, interval_value, time_value, timestamp_value};

    fn definitions(chunk: &DataChunk) -> Vec<ColumnDefinition> {
        chunk
            .column_names
            .clone()
            .unwrap_or_default()
            .into_iter()
            .zip(chunk.types.iter())
            .map(|(name, logical_type)| ColumnDefinition {
                name,
                logical_type: logical_type.clone(),
            })
            .collect()
    }

    fn record_batch(chunk: &DataChunk) -> RecordBatch {
        let schema = schema(&definitions(chunk)).unwrap();
        to_record_batch(chunk, &schema).unwrap()
    }

    fn struct_value(entries: Vec<(&str, Value)>) -> Value {
        let mut row = IndexMap::new();
        for (name, value) in entries {
            row.insert(name.to_string(), value);
        }
        Value::Struct(row)
    }

    fn named(name: &str) -> Option<String> {
        Some(name.to_string())
    }

    fn enum_type() -> LogicalType {
        LogicalTypes::r#enum(vec![
            "sad".to_string(),
            "ok".to_string(),
            "happy".to_string(),
        ])
    }

    fn point_type() -> LogicalType {
        LogicalTypes::r#struct(vec![
            ChildType {
                name: "x".to_string(),
                logical_type: LogicalTypes::integer(),
            },
            ChildType {
                name: "y".to_string(),
                logical_type: LogicalTypes::varchar(),
            },
        ])
    }

    /// Every scalar type, as (column name, logical type, one value, the Arrow
    /// array expected for that value followed by a null).
    fn scalar_cases() -> Vec<(&'static str, LogicalType, Value, ArrayRef)> {
        vec![
            (
                "null_v",
                LogicalTypes::null(),
                Value::Null,
                Arc::new(NullArray::new(2)),
            ),
            (
                "bool_v",
                LogicalTypes::boolean(),
                Value::Bool(true),
                Arc::new(BooleanArray::from(vec![Some(true), None])),
            ),
            (
                "tiny_v",
                LogicalTypes::tinyint(),
                Value::Int(127),
                Arc::new(Int8Array::from(vec![Some(127), None])),
            ),
            (
                "small_v",
                LogicalTypes::smallint(),
                Value::Int(32767),
                Arc::new(Int16Array::from(vec![Some(32767), None])),
            ),
            (
                "int_v",
                LogicalTypes::integer(),
                Value::Int(2147483647),
                Arc::new(Int32Array::from(vec![Some(2147483647), None])),
            ),
            (
                "big_v",
                LogicalTypes::bigint(),
                Value::Int(9007199254740993),
                Arc::new(Int64Array::from(vec![Some(9007199254740993), None])),
            ),
            (
                "utiny_v",
                LogicalTypes::utinyint(),
                Value::UInt(255),
                Arc::new(UInt8Array::from(vec![Some(255), None])),
            ),
            (
                "usmall_v",
                LogicalTypes::usmallint(),
                Value::UInt(65535),
                Arc::new(UInt16Array::from(vec![Some(65535), None])),
            ),
            (
                "uint_v",
                LogicalTypes::uinteger(),
                Value::UInt(4294967295),
                Arc::new(UInt32Array::from(vec![Some(4294967295), None])),
            ),
            (
                "ubig_v",
                LogicalTypes::ubigint(),
                Value::UInt(u64::MAX),
                Arc::new(UInt64Array::from(vec![Some(u64::MAX), None])),
            ),
            (
                "huge_v",
                LogicalTypes::hugeint(),
                Value::HugeInt(-123456789012345678901234567890),
                Arc::new(
                    Decimal256Array::from(vec![
                        Some(i256::from_i128(-123456789012345678901234567890)),
                        None,
                    ])
                    .with_precision_and_scale(HUGE_INT_PRECISION, 0)
                    .unwrap(),
                ),
            ),
            (
                "uhuge_v",
                LogicalTypes::uhugeint(),
                Value::UHugeInt(u128::MAX),
                Arc::new(
                    Decimal256Array::from(vec![Some(i256::from_parts(u128::MAX, 0)), None])
                        .with_precision_and_scale(HUGE_INT_PRECISION, 0)
                        .unwrap(),
                ),
            ),
            (
                "float_v",
                LogicalTypes::float(),
                Value::Float(1.5),
                Arc::new(Float32Array::from(vec![Some(1.5), None])),
            ),
            (
                "double_v",
                LogicalTypes::double(),
                Value::Double(2.25),
                Arc::new(Float64Array::from(vec![Some(2.25), None])),
            ),
            (
                "dec_v",
                LogicalTypes::decimal(9, 2),
                decimal_value("1234567.89", 9, 2).unwrap(),
                Arc::new(
                    Decimal128Array::from(vec![Some(123456789), None])
                        .with_precision_and_scale(9, 2)
                        .unwrap(),
                ),
            ),
            (
                "string_v",
                LogicalTypes::varchar(),
                Value::String("hello".to_string()),
                Arc::new(StringArray::from(vec![Some("hello"), None])),
            ),
            (
                "blob_v",
                LogicalTypes::blob(),
                Value::Bytes(b"hi".to_vec()),
                Arc::new(BinaryArray::from(vec![Some(b"hi".as_slice()), None])),
            ),
            (
                "uuid_v",
                LogicalTypes::uuid(),
                Value::String("00112233-4455-6677-8899-aabbccddeeff".to_string()),
                Arc::new(StringArray::from(vec![
                    Some("00112233-4455-6677-8899-aabbccddeeff"),
                    None,
                ])),
            ),
            (
                "enum_v",
                enum_type(),
                Value::String("ok".to_string()),
                Arc::new(StringArray::from(vec![Some("ok"), None])),
            ),
            (
                "date_v",
                LogicalTypes::date(),
                date_value(18263),
                Arc::new(Date32Array::from(vec![Some(18263), None])),
            ),
            (
                "time_v",
                LogicalTypes::time(),
                time_value(1234567, TimeUnit::Micros),
                Arc::new(Time64MicrosecondArray::from(vec![Some(1234567), None])),
            ),
            (
                "time_ns_v",
                LogicalTypes::time_ns(),
                time_value(1234567890, TimeUnit::Nanos),
                Arc::new(Time64NanosecondArray::from(vec![Some(1234567890), None])),
            ),
            (
                "ts_s_v",
                LogicalTypes::timestamp_seconds(),
                timestamp_value(1, TimestampUnit::Seconds, false),
                Arc::new(TimestampSecondArray::from(vec![Some(1), None])),
            ),
            (
                "ts_ms_v",
                LogicalTypes::timestamp_millis(),
                timestamp_value(1234, TimestampUnit::Millis, false),
                Arc::new(TimestampMillisecondArray::from(vec![Some(1234), None])),
            ),
            (
                "ts_v",
                LogicalTypes::timestamp(),
                timestamp_value(1234567, TimestampUnit::Micros, false),
                Arc::new(TimestampMicrosecondArray::from(vec![Some(1234567), None])),
            ),
            (
                "ts_ns_v",
                LogicalTypes::timestamp_nanos(),
                timestamp_value(1234567890, TimestampUnit::Nanos, false),
                Arc::new(TimestampNanosecondArray::from(vec![Some(1234567890), None])),
            ),
            (
                "ts_tz_v",
                LogicalTypes::timestamp_tz(),
                timestamp_value(1234567, TimestampUnit::Micros, true),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![Some(1234567), None])
                        .with_timezone(UTC_TIMEZONE),
                ),
            ),
            (
                "interval_v",
                LogicalTypes::interval(),
                interval_value(1, 2, 3),
                Arc::new(IntervalMonthDayNanoArray::from(vec![
                    Some(IntervalMonthDayNano::new(1, 2, 3000)),
                    None,
                ])),
            ),
        ]
    }

    #[test]
    fn scalar_columns_map_to_their_arrow_types() {
        let cases = scalar_cases();
        let chunk = data_chunk(
            cases
                .iter()
                .map(|(name, logical_type, value, _)| {
                    column(
                        logical_type.clone(),
                        [value.clone(), Value::Null],
                        named(name),
                    )
                })
                .collect(),
        )
        .unwrap();

        let batch = record_batch(&chunk);

        assert_eq!(batch.num_rows(), 2);
        for (name, _, _, expected) in &cases {
            let index = batch.schema().index_of(name).unwrap();
            assert_eq!(batch.column(index), expected, "column {name}");
        }
    }

    #[test]
    fn build_array_and_arrow_type_agree_for_every_mapped_type() {
        let mut types: Vec<LogicalType> = scalar_cases()
            .into_iter()
            .map(|(_, logical_type, _, _)| logical_type)
            .collect();
        types.extend([
            LogicalTypes::bit(),
            LogicalTypes::geometry(None),
            LogicalTypes::list(LogicalTypes::integer()),
            LogicalTypes::list(point_type()),
            LogicalTypes::array(LogicalTypes::integer(), 2),
            LogicalTypes::map(LogicalTypes::varchar(), LogicalTypes::integer()),
            point_type(),
        ]);

        for logical_type in types {
            let declared = arrow_type(&logical_type).unwrap();
            let built = build_array(&logical_type, &declared, &[]).unwrap();
            assert_eq!(
                built.data_type(),
                &declared,
                "{:?} builds an array that disagrees with arrow_type()",
                logical_type.id
            );
        }
    }

    #[test]
    fn nested_columns_preserve_null_containers_and_entries() {
        let chunk = data_chunk(vec![
            column(
                LogicalTypes::list(LogicalTypes::integer()),
                [
                    Value::List(vec![Value::Int(1), Value::Null, Value::Int(3)]),
                    Value::Null,
                    Value::List(vec![]),
                ],
                named("list_v"),
            ),
            column(
                LogicalTypes::array(LogicalTypes::integer(), 2),
                [
                    Value::List(vec![Value::Int(7), Value::Int(8)]),
                    Value::Null,
                    Value::List(vec![Value::Null, Value::Int(9)]),
                ],
                named("array_v"),
            ),
            column(
                point_type(),
                [
                    struct_value(vec![
                        ("x", Value::Int(1)),
                        ("y", Value::String("a".to_string())),
                    ]),
                    Value::Null,
                    struct_value(vec![("x", Value::Int(3)), ("y", Value::Null)]),
                ],
                named("struct_v"),
            ),
            column(
                LogicalTypes::map(LogicalTypes::varchar(), LogicalTypes::integer()),
                [
                    Value::List(vec![struct_value(vec![
                        ("key", Value::String("a".to_string())),
                        ("value", Value::Int(1)),
                    ])]),
                    Value::Null,
                    Value::List(vec![]),
                ],
                named("map_v"),
            ),
            column(
                LogicalTypes::list(point_type()),
                [
                    Value::List(vec![struct_value(vec![
                        ("x", Value::Int(5)),
                        ("y", Value::String("b".to_string())),
                    ])]),
                    Value::Null,
                    Value::List(vec![Value::Null]),
                ],
                named("list_of_struct_v"),
            ),
        ])
        .unwrap();

        let batch = record_batch(&chunk);
        let point_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, true),
            Field::new("y", DataType::Utf8, true),
        ]);

        assert_eq!(
            batch
                .schema()
                .field_with_name("list_v")
                .unwrap()
                .data_type(),
            &DataType::List(Arc::new(Field::new_list_field(DataType::Int32, true)))
        );
        assert_eq!(
            batch
                .schema()
                .field_with_name("array_v")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeList(Arc::new(Field::new_list_field(DataType::Int32, true)), 2)
        );
        assert_eq!(
            batch.schema().field_with_name("map_v").unwrap().data_type(),
            &DataType::List(Arc::new(Field::new_list_field(
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, true),
                    Field::new("value", DataType::Int32, true),
                ])),
                true
            )))
        );

        let lists = batch.column_by_name("list_v").unwrap().as_list::<i32>();
        assert!(lists.is_null(1));
        assert_eq!(lists.value_length(0), 3);
        assert_eq!(lists.value_length(2), 0);
        assert_eq!(
            lists.value(0).as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(1), None, Some(3)])
        );

        let arrays = batch
            .column_by_name("array_v")
            .unwrap()
            .as_fixed_size_list();
        assert!(arrays.is_null(1));
        assert_eq!(
            arrays.value(2).as_primitive::<Int32Type>(),
            &Int32Array::from(vec![None, Some(9)])
        );

        let structs = batch.column_by_name("struct_v").unwrap().as_struct();
        assert!(structs.is_null(1));
        assert_eq!(
            structs.column(0).as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(1), None, Some(3)])
        );
        assert_eq!(
            structs.column(1).as_string::<i32>(),
            &StringArray::from(vec![Some("a"), None, None])
        );

        let list_of_struct = batch
            .column_by_name("list_of_struct_v")
            .unwrap()
            .as_list::<i32>();
        let entries = list_of_struct.value(0);
        let entries = entries.as_struct();
        assert_eq!(entries.fields(), &point_fields);
        assert_eq!(
            entries.column(0).as_primitive::<Int32Type>(),
            &Int32Array::from(vec![Some(5)])
        );
        let null_entry = list_of_struct.value(2);
        assert!(null_entry.is_null(0));
    }

    #[test]
    fn schema_is_built_without_any_chunk() {
        let columns = vec![
            ColumnDefinition {
                name: "id".to_string(),
                logical_type: LogicalTypes::integer(),
            },
            ColumnDefinition {
                name: "label".to_string(),
                logical_type: LogicalTypes::varchar(),
            },
        ];

        let schema = schema(&columns).unwrap();

        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            vec!["id", "label"]
        );
        assert_eq!(schema.field(0).data_type(), &DataType::Int32);
        assert!(schema.field(0).is_nullable());
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
    }

    #[test]
    fn empty_chunks_keep_the_schema_and_row_count() {
        let chunk = data_chunk(vec![column(LogicalTypes::integer(), [], named("id"))]).unwrap();
        let schema = schema(&[ColumnDefinition {
            name: "id".to_string(),
            logical_type: LogicalTypes::integer(),
        }])
        .unwrap();

        let batch = to_record_batch(&chunk, &schema).unwrap();

        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.schema(), schema);
    }

    #[test]
    fn unsupported_types_are_reported_by_name() {
        let error = arrow_type(&LogicalTypes::time_tz()).unwrap_err();

        assert!(
            matches!(&error, QuackError::UnsupportedType(message) if message.contains("TimeTz")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn times_outside_the_arrow_day_bound_are_rejected() {
        let chunk = data_chunk(vec![column(
            LogicalTypes::time(),
            [time_value(MICROSECONDS_IN_DAY, TimeUnit::Micros)],
            named("time_v"),
        )])
        .unwrap();
        let schema = schema(&definitions(&chunk)).unwrap();

        let error = to_record_batch(&chunk, &schema).unwrap_err();

        assert!(
            matches!(&error, QuackError::Protocol(message) if message.contains("out of range")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn out_of_range_narrowing_is_a_protocol_error() {
        let chunk = data_chunk(vec![column(
            LogicalTypes::tinyint(),
            [Value::Int(1000)],
            named("tiny_v"),
        )])
        .unwrap();
        let schema = schema(&definitions(&chunk)).unwrap();

        let error = to_record_batch(&chunk, &schema).unwrap_err();

        assert!(
            matches!(&error, QuackError::Protocol(message) if message.contains("out of range")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn value_variants_that_contradict_the_logical_type_are_rejected() {
        let chunk = data_chunk(vec![column(
            LogicalTypes::integer(),
            [Value::String("nope".to_string())],
            named("int_v"),
        )])
        .unwrap();
        let schema = schema(&definitions(&chunk)).unwrap();

        let error = to_record_batch(&chunk, &schema).unwrap_err();

        assert!(
            matches!(&error, QuackError::Protocol(message) if message.contains("does not match")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn nested_columns_honour_the_schema_field_names() {
        let chunk = data_chunk(vec![column(
            LogicalTypes::list(LogicalTypes::integer()),
            [Value::List(vec![Value::Int(1), Value::Int(2)])],
            named("list_v"),
        )])
        .unwrap();
        // Parquet and Spark name the list child "element" rather than "item";
        // building against the schema's own field keeps either name working.
        let item = Arc::new(Field::new("element", DataType::Int32, true));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "list_v",
            DataType::List(item.clone()),
            true,
        )]));

        let batch = to_record_batch(&chunk, &schema).unwrap();

        assert_eq!(batch.schema(), schema);
        assert_eq!(
            batch.column(0).data_type(),
            &DataType::List(item),
            "the list child field must come from the schema"
        );
    }

    #[test]
    fn nested_columns_whose_schema_shape_disagrees_are_rejected() {
        let chunk = data_chunk(vec![column(
            LogicalTypes::list(LogicalTypes::integer()),
            [Value::List(vec![Value::Int(1)])],
            named("list_v"),
        )])
        .unwrap();
        let schema = schema(&[ColumnDefinition {
            name: "list_v".to_string(),
            logical_type: LogicalTypes::varchar(),
        }])
        .unwrap();

        let error = to_record_batch(&chunk, &schema).unwrap_err();

        assert!(
            matches!(&error, QuackError::Protocol(message) if message.contains("where List is required")),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn chunks_that_disagree_with_the_schema_are_rejected() {
        let chunk = data_chunk(vec![column(
            LogicalTypes::varchar(),
            [Value::String("hello".to_string())],
            named("id"),
        )])
        .unwrap();
        let schema = schema(&[ColumnDefinition {
            name: "id".to_string(),
            logical_type: LogicalTypes::integer(),
        }])
        .unwrap();

        let error = to_record_batch(&chunk, &schema).unwrap_err();

        assert!(
            matches!(&error, QuackError::Protocol(message) if message.contains("column types must match schema types")),
            "unexpected error: {error}"
        );
    }
}
