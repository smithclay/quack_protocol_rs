//! Arrow output support for query results.
//!
//! The bridge is read-only: it maps an already decoded [`DataChunk`] onto
//! [`arrow_array::RecordBatch`] using the column [`LogicalType`]s, so the wire
//! codecs are untouched. It is gated behind the `arrow` feature.

use std::iter::zip;
use std::sync::Arc;

use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    FixedSizeListArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array, Int64Array,
    IntervalMonthDayNanoArray, ListArray, NullArray, RecordBatch, RecordBatchOptions, StringArray,
    StructArray, Time64MicrosecondArray, Time64NanosecondArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
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
    ExtraTypeInfo, LogicalType, LogicalTypeId, get_array_size, get_child_type, get_struct_children,
};
use crate::vector::{DataChunk, TimeUnit, TimestampUnit, Value};

pub use arrow_array;
pub use arrow_buffer;
pub use arrow_schema;

const NULL_VALUE: Value = Value::Null;
const LIST_ITEM_NAME: &str = "item";
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
        let data_type = arrow_type(logical_type)?;
        if &data_type != field.data_type() {
            return Err(QuackError::protocol(format!(
                "column {index} maps to Arrow type {data_type} but the schema declares {}",
                field.data_type()
            )));
        }
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
        other => {
            return Err(QuackError::unsupported(format!(
                "logical type {other:?} has no Arrow mapping"
            )));
        }
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
    field(LIST_ITEM_NAME, get_child_type(logical_type)?)
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
    let (width, scale) = match logical_type.type_info.as_ref() {
        Some(ExtraTypeInfo::Decimal { width, scale, .. }) => (*width, *scale),
        _ => {
            return Err(QuackError::protocol(
                "DECIMAL type is missing DecimalTypeInfo",
            ));
        }
    };
    match (u8::try_from(width), i8::try_from(scale)) {
        (Ok(width), Ok(scale)) => Ok((width, scale)),
        _ => Err(QuackError::protocol(format!(
            "DECIMAL({width}, {scale}) exceeds the Arrow decimal limits"
        ))),
    }
}

fn build_array(logical_type: &LogicalType, values: &[&Value]) -> Result<ArrayRef> {
    Ok(match logical_type.id {
        LogicalTypeId::SqlNull => Arc::new(NullArray::new(values.len())),
        LogicalTypeId::Boolean => Arc::new(
            collect(values, |value| as_bool(logical_type, value))?
                .into_iter()
                .collect::<BooleanArray>(),
        ),
        LogicalTypeId::TinyInt => Arc::new(
            collect(values, |value| narrow_int::<i8>(logical_type, value))?
                .into_iter()
                .collect::<Int8Array>(),
        ),
        LogicalTypeId::SmallInt => Arc::new(
            collect(values, |value| narrow_int::<i16>(logical_type, value))?
                .into_iter()
                .collect::<Int16Array>(),
        ),
        LogicalTypeId::Integer => Arc::new(
            collect(values, |value| narrow_int::<i32>(logical_type, value))?
                .into_iter()
                .collect::<Int32Array>(),
        ),
        LogicalTypeId::BigInt => Arc::new(
            collect(values, |value| as_int(logical_type, value))?
                .into_iter()
                .collect::<Int64Array>(),
        ),
        LogicalTypeId::UTinyInt => Arc::new(
            collect(values, |value| narrow_uint::<u8>(logical_type, value))?
                .into_iter()
                .collect::<UInt8Array>(),
        ),
        LogicalTypeId::USmallInt => Arc::new(
            collect(values, |value| narrow_uint::<u16>(logical_type, value))?
                .into_iter()
                .collect::<UInt16Array>(),
        ),
        LogicalTypeId::UInteger => Arc::new(
            collect(values, |value| narrow_uint::<u32>(logical_type, value))?
                .into_iter()
                .collect::<UInt32Array>(),
        ),
        LogicalTypeId::UBigInt => Arc::new(
            collect(values, |value| as_uint(logical_type, value))?
                .into_iter()
                .collect::<UInt64Array>(),
        ),
        LogicalTypeId::HugeInt => Arc::new(
            collect(values, |value| match value {
                Value::HugeInt(value) => Ok(i256::from_i128(*value)),
                other => Err(mismatch(logical_type, other)),
            })?
            .into_iter()
            .collect::<Decimal256Array>()
            .with_precision_and_scale(HUGE_INT_PRECISION, 0)
            .map_err(arrow_error)?,
        ),
        LogicalTypeId::UHugeInt => Arc::new(
            collect(values, |value| match value {
                Value::UHugeInt(value) => Ok(i256::from_parts(*value, 0)),
                other => Err(mismatch(logical_type, other)),
            })?
            .into_iter()
            .collect::<Decimal256Array>()
            .with_precision_and_scale(HUGE_INT_PRECISION, 0)
            .map_err(arrow_error)?,
        ),
        LogicalTypeId::Float => Arc::new(
            collect(values, |value| match value {
                Value::Float(value) => Ok(*value),
                other => Err(mismatch(logical_type, other)),
            })?
            .into_iter()
            .collect::<Float32Array>(),
        ),
        LogicalTypeId::Double => Arc::new(
            collect(values, |value| match value {
                Value::Double(value) => Ok(*value),
                other => Err(mismatch(logical_type, other)),
            })?
            .into_iter()
            .collect::<Float64Array>(),
        ),
        LogicalTypeId::Decimal => {
            let (precision, scale) = decimal_precision_and_scale(logical_type)?;
            Arc::new(
                collect(values, |value| match value {
                    Value::Decimal(value) => Ok(value.value),
                    other => Err(mismatch(logical_type, other)),
                })?
                .into_iter()
                .collect::<Decimal128Array>()
                .with_precision_and_scale(precision, scale)
                .map_err(arrow_error)?,
            )
        }
        LogicalTypeId::Varchar
        | LogicalTypeId::Char
        | LogicalTypeId::Enum
        | LogicalTypeId::Uuid => Arc::new(
            collect(values, |value| match value {
                Value::String(value) => Ok(value.as_str()),
                other => Err(mismatch(logical_type, other)),
            })?
            .into_iter()
            .collect::<StringArray>(),
        ),
        LogicalTypeId::Blob | LogicalTypeId::Geometry | LogicalTypeId::Bit => Arc::new(
            collect(values, |value| match value {
                Value::Bytes(value) => Ok(value.as_slice()),
                other => Err(mismatch(logical_type, other)),
            })?
            .into_iter()
            .collect::<BinaryArray>(),
        ),
        LogicalTypeId::Date => Arc::new(
            collect(values, |value| match value {
                Value::Date(value) => Ok(value.days),
                other => Err(mismatch(logical_type, other)),
            })?
            .into_iter()
            .collect::<Date32Array>(),
        ),
        LogicalTypeId::Time => Arc::new(
            collect(values, |value| {
                as_time(logical_type, value, TimeUnit::Micros)
            })?
            .into_iter()
            .collect::<Time64MicrosecondArray>(),
        ),
        LogicalTypeId::TimeNs => Arc::new(
            collect(values, |value| {
                as_time(logical_type, value, TimeUnit::Nanos)
            })?
            .into_iter()
            .collect::<Time64NanosecondArray>(),
        ),
        LogicalTypeId::TimestampSec => Arc::new(
            collect(values, |value| {
                as_timestamp(logical_type, value, TimestampUnit::Seconds, false)
            })?
            .into_iter()
            .collect::<TimestampSecondArray>(),
        ),
        LogicalTypeId::TimestampMs => Arc::new(
            collect(values, |value| {
                as_timestamp(logical_type, value, TimestampUnit::Millis, false)
            })?
            .into_iter()
            .collect::<TimestampMillisecondArray>(),
        ),
        LogicalTypeId::Timestamp => Arc::new(
            collect(values, |value| {
                as_timestamp(logical_type, value, TimestampUnit::Micros, false)
            })?
            .into_iter()
            .collect::<TimestampMicrosecondArray>(),
        ),
        LogicalTypeId::TimestampNs => Arc::new(
            collect(values, |value| {
                as_timestamp(logical_type, value, TimestampUnit::Nanos, false)
            })?
            .into_iter()
            .collect::<TimestampNanosecondArray>(),
        ),
        LogicalTypeId::TimestampTz => Arc::new(
            collect(values, |value| {
                as_timestamp(logical_type, value, TimestampUnit::Micros, true)
            })?
            .into_iter()
            .collect::<TimestampMicrosecondArray>()
            .with_timezone(UTC_TIMEZONE),
        ),
        LogicalTypeId::Interval => Arc::new(
            collect(values, |value| as_interval(logical_type, value))?
                .into_iter()
                .collect::<IntervalMonthDayNanoArray>(),
        ),
        LogicalTypeId::List | LogicalTypeId::Map => build_list(logical_type, values)?,
        LogicalTypeId::Array => build_fixed_size_list(logical_type, values)?,
        LogicalTypeId::Struct => build_struct(logical_type, values)?,
        other => {
            return Err(QuackError::unsupported(format!(
                "logical type {other:?} has no Arrow mapping"
            )));
        }
    })
}

fn build_list(logical_type: &LogicalType, values: &[&Value]) -> Result<ArrayRef> {
    let mut offsets = Vec::with_capacity(values.len() + 1);
    let mut validity = Vec::with_capacity(values.len());
    let mut items = Vec::new();
    offsets.push(0i32);
    for value in values {
        match *value {
            Value::Null => validity.push(false),
            Value::List(entries) => {
                items.extend(entries.iter());
                validity.push(true);
            }
            other => return Err(mismatch(logical_type, other)),
        }
        offsets.push(i32::try_from(items.len()).map_err(|_| {
            QuackError::protocol("LIST column exceeds the Arrow 32-bit offset limit")
        })?);
    }
    let child = build_array(get_child_type(logical_type)?, &items)?;
    Ok(Arc::new(
        ListArray::try_new(
            list_item_field(logical_type)?,
            OffsetBuffer::new(offsets.into()),
            child,
            Some(NullBuffer::from(validity)),
        )
        .map_err(arrow_error)?,
    ))
}

fn build_fixed_size_list(logical_type: &LogicalType, values: &[&Value]) -> Result<ArrayRef> {
    let size = array_size(logical_type)?;
    let mut validity = Vec::with_capacity(values.len());
    let mut items = Vec::with_capacity(values.len() * size as usize);
    for value in values {
        match *value {
            Value::Null => {
                items.extend(std::iter::repeat_n(&NULL_VALUE, size as usize));
                validity.push(false);
            }
            Value::List(entries) => {
                if entries.len() != size as usize {
                    return Err(QuackError::protocol(format!(
                        "ARRAY value has {} entries, expected {size}",
                        entries.len()
                    )));
                }
                items.extend(entries.iter());
                validity.push(true);
            }
            other => return Err(mismatch(logical_type, other)),
        }
    }
    let child = build_array(get_child_type(logical_type)?, &items)?;
    Ok(Arc::new(
        FixedSizeListArray::try_new(
            list_item_field(logical_type)?,
            size,
            child,
            Some(NullBuffer::from(validity)),
        )
        .map_err(arrow_error)?,
    ))
}

fn build_struct(logical_type: &LogicalType, values: &[&Value]) -> Result<ArrayRef> {
    let mut validity = Vec::with_capacity(values.len());
    for value in values {
        match *value {
            Value::Null => validity.push(false),
            Value::Struct(_) => validity.push(true),
            other => return Err(mismatch(logical_type, other)),
        }
    }
    let children = get_struct_children(logical_type)?;
    let mut arrays = Vec::with_capacity(children.len());
    for child in children {
        let child_values = values
            .iter()
            .map(|value| match *value {
                Value::Struct(row) => row.get(&child.name).unwrap_or(&NULL_VALUE),
                _ => &NULL_VALUE,
            })
            .collect::<Vec<_>>();
        arrays.push(build_array(&child.logical_type, &child_values)?);
    }
    Ok(Arc::new(
        StructArray::try_new_with_length(
            struct_fields(logical_type)?,
            arrays,
            Some(NullBuffer::from(validity)),
            values.len(),
        )
        .map_err(arrow_error)?,
    ))
}

fn collect<'a, T>(
    values: &[&'a Value],
    extract: impl Fn(&'a Value) -> Result<T>,
) -> Result<Vec<Option<T>>> {
    values
        .iter()
        .map(|value| match *value {
            Value::Null => Ok(None),
            value => extract(value).map(Some),
        })
        .collect()
}

fn as_bool(logical_type: &LogicalType, value: &Value) -> Result<bool> {
    match value {
        Value::Bool(value) => Ok(*value),
        other => Err(mismatch(logical_type, other)),
    }
}

fn as_int(logical_type: &LogicalType, value: &Value) -> Result<i64> {
    match value {
        Value::Int(value) => Ok(*value),
        other => Err(mismatch(logical_type, other)),
    }
}

fn as_uint(logical_type: &LogicalType, value: &Value) -> Result<u64> {
    match value {
        Value::UInt(value) => Ok(*value),
        other => Err(mismatch(logical_type, other)),
    }
}

fn narrow_int<T: TryFrom<i64>>(logical_type: &LogicalType, value: &Value) -> Result<T> {
    let value = as_int(logical_type, value)?;
    T::try_from(value).map_err(|_| out_of_range(logical_type, value))
}

fn narrow_uint<T: TryFrom<u64>>(logical_type: &LogicalType, value: &Value) -> Result<T> {
    let value = as_uint(logical_type, value)?;
    T::try_from(value).map_err(|_| out_of_range(logical_type, value))
}

fn as_time(logical_type: &LogicalType, value: &Value, unit: TimeUnit) -> Result<i64> {
    match value {
        Value::Time(value) if value.unit == unit => Ok(value.value),
        other => Err(mismatch(logical_type, other)),
    }
}

fn as_timestamp(
    logical_type: &LogicalType,
    value: &Value,
    unit: TimestampUnit,
    timezone_utc: bool,
) -> Result<i64> {
    match value {
        Value::Timestamp(value) if value.unit == unit && value.timezone_utc == timezone_utc => {
            Ok(value.value)
        }
        other => Err(mismatch(logical_type, other)),
    }
}

fn as_interval(logical_type: &LogicalType, value: &Value) -> Result<IntervalMonthDayNano> {
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
        other => Err(mismatch(logical_type, other)),
    }
}

fn mismatch(logical_type: &LogicalType, value: &Value) -> QuackError {
    QuackError::protocol(format!(
        "decoded value {value:?} does not match logical type {:?}",
        logical_type.id
    ))
}

fn out_of_range(logical_type: &LogicalType, value: impl std::fmt::Display) -> QuackError {
    QuackError::protocol(format!(
        "decoded value {value} is out of range for logical type {:?}",
        logical_type.id
    ))
}

fn arrow_error(error: ArrowError) -> QuackError {
    QuackError::protocol(format!("arrow conversion failed: {error}"))
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
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

    fn assert_column(batch: &RecordBatch, name: &str, expected: impl Array + 'static) {
        let index = batch.schema().index_of(name).unwrap();
        assert_eq!(
            batch.column(index).as_ref(),
            &expected as &dyn Array,
            "column {name}"
        );
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

    #[test]
    fn scalar_columns_map_to_their_arrow_types() {
        let enum_type = LogicalTypes::r#enum(vec![
            "sad".to_string(),
            "ok".to_string(),
            "happy".to_string(),
        ]);
        let chunk = data_chunk(vec![
            column(
                LogicalTypes::null(),
                [Value::Null, Value::Null],
                named("null_v"),
            ),
            column(
                LogicalTypes::boolean(),
                [Value::Bool(true), Value::Null],
                named("bool_v"),
            ),
            column(
                LogicalTypes::tinyint(),
                [Value::Int(127), Value::Null],
                named("tiny_v"),
            ),
            column(
                LogicalTypes::smallint(),
                [Value::Int(32767), Value::Null],
                named("small_v"),
            ),
            column(
                LogicalTypes::integer(),
                [Value::Int(2147483647), Value::Null],
                named("int_v"),
            ),
            column(
                LogicalTypes::bigint(),
                [Value::Int(9007199254740993), Value::Null],
                named("big_v"),
            ),
            column(
                LogicalTypes::utinyint(),
                [Value::UInt(255), Value::Null],
                named("utiny_v"),
            ),
            column(
                LogicalTypes::usmallint(),
                [Value::UInt(65535), Value::Null],
                named("usmall_v"),
            ),
            column(
                LogicalTypes::uinteger(),
                [Value::UInt(4294967295), Value::Null],
                named("uint_v"),
            ),
            column(
                LogicalTypes::ubigint(),
                [Value::UInt(u64::MAX), Value::Null],
                named("ubig_v"),
            ),
            column(
                LogicalTypes::hugeint(),
                [Value::HugeInt(-123456789012345678901234567890), Value::Null],
                named("huge_v"),
            ),
            column(
                LogicalTypes::uhugeint(),
                [Value::UHugeInt(123456789012345678901234567890), Value::Null],
                named("uhuge_v"),
            ),
            column(
                LogicalTypes::float(),
                [Value::Float(1.5), Value::Null],
                named("float_v"),
            ),
            column(
                LogicalTypes::double(),
                [Value::Double(2.25), Value::Null],
                named("double_v"),
            ),
            column(
                LogicalTypes::decimal(9, 2),
                [decimal_value("1234567.89", 9, 2).unwrap(), Value::Null],
                named("dec_v"),
            ),
            column(
                LogicalTypes::varchar(),
                [Value::String("hello".to_string()), Value::Null],
                named("string_v"),
            ),
            column(
                LogicalTypes::blob(),
                [Value::Bytes(b"hi".to_vec()), Value::Null],
                named("blob_v"),
            ),
            column(
                LogicalTypes::uuid(),
                [
                    Value::String("00112233-4455-6677-8899-aabbccddeeff".to_string()),
                    Value::Null,
                ],
                named("uuid_v"),
            ),
            column(
                enum_type,
                [Value::String("ok".to_string()), Value::Null],
                named("enum_v"),
            ),
            column(
                LogicalTypes::date(),
                [date_value(18263), Value::Null],
                named("date_v"),
            ),
            column(
                LogicalTypes::time(),
                [time_value(1234567, TimeUnit::Micros), Value::Null],
                named("time_v"),
            ),
            column(
                LogicalTypes::time_ns(),
                [time_value(1234567890, TimeUnit::Nanos), Value::Null],
                named("time_ns_v"),
            ),
            column(
                LogicalTypes::timestamp_seconds(),
                [
                    timestamp_value(1, TimestampUnit::Seconds, false),
                    Value::Null,
                ],
                named("ts_s_v"),
            ),
            column(
                LogicalTypes::timestamp_millis(),
                [
                    timestamp_value(1234, TimestampUnit::Millis, false),
                    Value::Null,
                ],
                named("ts_ms_v"),
            ),
            column(
                LogicalTypes::timestamp(),
                [
                    timestamp_value(1234567, TimestampUnit::Micros, false),
                    Value::Null,
                ],
                named("ts_v"),
            ),
            column(
                LogicalTypes::timestamp_nanos(),
                [
                    timestamp_value(1234567890, TimestampUnit::Nanos, false),
                    Value::Null,
                ],
                named("ts_ns_v"),
            ),
            column(
                LogicalTypes::timestamp_tz(),
                [
                    timestamp_value(1234567, TimestampUnit::Micros, true),
                    Value::Null,
                ],
                named("ts_tz_v"),
            ),
            column(
                LogicalTypes::interval(),
                [interval_value(1, 2, 3), Value::Null],
                named("interval_v"),
            ),
        ])
        .unwrap();

        let batch = record_batch(&chunk);

        assert_eq!(batch.num_rows(), 2);
        assert_column(&batch, "null_v", NullArray::new(2));
        assert_column(&batch, "bool_v", BooleanArray::from(vec![Some(true), None]));
        assert_column(&batch, "tiny_v", Int8Array::from(vec![Some(127), None]));
        assert_column(&batch, "small_v", Int16Array::from(vec![Some(32767), None]));
        assert_column(
            &batch,
            "int_v",
            Int32Array::from(vec![Some(2147483647), None]),
        );
        assert_column(
            &batch,
            "big_v",
            Int64Array::from(vec![Some(9007199254740993), None]),
        );
        assert_column(&batch, "utiny_v", UInt8Array::from(vec![Some(255), None]));
        assert_column(
            &batch,
            "usmall_v",
            UInt16Array::from(vec![Some(65535), None]),
        );
        assert_column(
            &batch,
            "uint_v",
            UInt32Array::from(vec![Some(4294967295), None]),
        );
        assert_column(
            &batch,
            "ubig_v",
            UInt64Array::from(vec![Some(u64::MAX), None]),
        );
        assert_column(
            &batch,
            "huge_v",
            Decimal256Array::from(vec![
                Some(i256::from_i128(-123456789012345678901234567890)),
                None,
            ])
            .with_precision_and_scale(HUGE_INT_PRECISION, 0)
            .unwrap(),
        );
        assert_column(
            &batch,
            "uhuge_v",
            Decimal256Array::from(vec![
                Some(i256::from_parts(123456789012345678901234567890, 0)),
                None,
            ])
            .with_precision_and_scale(HUGE_INT_PRECISION, 0)
            .unwrap(),
        );
        assert_column(&batch, "float_v", Float32Array::from(vec![Some(1.5), None]));
        assert_column(
            &batch,
            "double_v",
            Float64Array::from(vec![Some(2.25), None]),
        );
        assert_column(
            &batch,
            "dec_v",
            Decimal128Array::from(vec![Some(123456789), None])
                .with_precision_and_scale(9, 2)
                .unwrap(),
        );
        assert_column(
            &batch,
            "string_v",
            StringArray::from(vec![Some("hello"), None]),
        );
        assert_column(
            &batch,
            "blob_v",
            BinaryArray::from(vec![Some(b"hi".as_slice()), None]),
        );
        assert_column(
            &batch,
            "uuid_v",
            StringArray::from(vec![Some("00112233-4455-6677-8899-aabbccddeeff"), None]),
        );
        assert_column(&batch, "enum_v", StringArray::from(vec![Some("ok"), None]));
        assert_column(&batch, "date_v", Date32Array::from(vec![Some(18263), None]));
        assert_column(
            &batch,
            "time_v",
            Time64MicrosecondArray::from(vec![Some(1234567), None]),
        );
        assert_column(
            &batch,
            "time_ns_v",
            Time64NanosecondArray::from(vec![Some(1234567890), None]),
        );
        assert_column(
            &batch,
            "ts_s_v",
            TimestampSecondArray::from(vec![Some(1), None]),
        );
        assert_column(
            &batch,
            "ts_ms_v",
            TimestampMillisecondArray::from(vec![Some(1234), None]),
        );
        assert_column(
            &batch,
            "ts_v",
            TimestampMicrosecondArray::from(vec![Some(1234567), None]),
        );
        assert_column(
            &batch,
            "ts_ns_v",
            TimestampNanosecondArray::from(vec![Some(1234567890), None]),
        );
        assert_column(
            &batch,
            "ts_tz_v",
            TimestampMicrosecondArray::from(vec![Some(1234567), None]).with_timezone(UTC_TIMEZONE),
        );
        assert_column(
            &batch,
            "interval_v",
            IntervalMonthDayNanoArray::from(vec![
                Some(IntervalMonthDayNano::new(1, 2, 3000)),
                None,
            ]),
        );
    }

    #[test]
    fn nested_columns_preserve_null_containers_and_entries() {
        let point_type = LogicalTypes::r#struct(vec![
            ChildType {
                name: "x".to_string(),
                logical_type: LogicalTypes::integer(),
            },
            ChildType {
                name: "y".to_string(),
                logical_type: LogicalTypes::varchar(),
            },
        ]);
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
                point_type.clone(),
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
                LogicalTypes::list(point_type),
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
            &DataType::List(Arc::new(Field::new(LIST_ITEM_NAME, DataType::Int32, true)))
        );
        assert_eq!(
            batch
                .schema()
                .field_with_name("array_v")
                .unwrap()
                .data_type(),
            &DataType::FixedSizeList(
                Arc::new(Field::new(LIST_ITEM_NAME, DataType::Int32, true)),
                2
            )
        );
        assert_eq!(
            batch.schema().field_with_name("map_v").unwrap().data_type(),
            &DataType::List(Arc::new(Field::new(
                LIST_ITEM_NAME,
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, true),
                    Field::new("value", DataType::Int32, true),
                ])),
                true
            )))
        );

        let lists = batch
            .column_by_name("list_v")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert!(lists.is_null(1));
        assert_eq!(lists.value_length(0), 3);
        assert_eq!(lists.value_length(2), 0);
        assert_eq!(
            lists.value(0).as_ref(),
            &Int32Array::from(vec![Some(1), None, Some(3)]) as &dyn Array
        );

        let arrays = batch
            .column_by_name("array_v")
            .unwrap()
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert!(arrays.is_null(1));
        assert_eq!(
            arrays.value(2).as_ref(),
            &Int32Array::from(vec![None, Some(9)]) as &dyn Array
        );

        let structs = batch
            .column_by_name("struct_v")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert!(structs.is_null(1));
        assert_eq!(
            structs.column(0).as_ref(),
            &Int32Array::from(vec![Some(1), None, Some(3)]) as &dyn Array
        );
        assert_eq!(
            structs.column(1).as_ref(),
            &StringArray::from(vec![Some("a"), None, None]) as &dyn Array
        );

        let list_of_struct = batch
            .column_by_name("list_of_struct_v")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        let entries = list_of_struct.value(0);
        let entries = entries.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(entries.fields(), &point_fields);
        assert_eq!(
            entries.column(0).as_ref(),
            &Int32Array::from(vec![Some(5)]) as &dyn Array
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
                .map(|f| f.name().as_str())
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
            matches!(&error, QuackError::Protocol(message) if message.contains("schema declares")),
            "unexpected error: {error}"
        );
    }
}
