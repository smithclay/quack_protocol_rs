use indexmap::IndexMap;
use quack_protocol::*;

fn some_name(name: &str) -> Option<String> {
    Some(name.to_string())
}

fn row(entries: Vec<(&str, Value)>) -> Row {
    let mut row = Row::new();
    for (key, value) in entries {
        row.insert(key.to_string(), value);
    }
    row
}

#[test]
fn binary_round_trips_objects_and_primitives() {
    let mut writer = binary::BinaryWriter::new();
    writer
        .write_object(|object| {
            object.write_field(1, |object| object.write_string("hello"))?;
            object.write_field(2, |object| object.write_sleb(-12345i64))?;
            object.write_field(3, |object| object.write_uleb(987654321u64))?;
            object.write_field(4, |object| object.write_huge_i128(-42))?;
            Ok(())
        })
        .unwrap();

    let bytes = writer.into_bytes();
    let mut reader = binary::BinaryReader::new(&bytes);
    let decoded = reader
        .read_object(|object| {
            Ok((
                object.read_required_field(1, |object| object.read_string())?,
                object.read_required_field(2, |object| object.read_sleb_i64())?,
                object.read_required_field(3, |object| object.read_uleb_u64())?,
                object.read_required_field(4, |object| object.read_huge_int_parts())?,
            ))
        })
        .unwrap();
    reader.assert_eof().unwrap();

    assert_eq!(decoded.0, "hello");
    assert_eq!(decoded.1, -12345);
    assert_eq!(decoded.2, 987654321);
    assert_eq!(binary::combine_signed_huge_int(decoded.3), -42);
}

#[test]
fn data_chunk_round_trips_flat_nested_and_decimal_values() {
    let struct_type = LogicalTypes::r#struct(vec![
        ChildType {
            name: "x".to_string(),
            logical_type: LogicalTypes::integer(),
        },
        ChildType {
            name: "y".to_string(),
            logical_type: LogicalTypes::varchar(),
        },
    ]);
    let mut point0 = IndexMap::new();
    point0.insert("x".to_string(), Value::Int(1));
    point0.insert("y".to_string(), Value::String("a".to_string()));
    let mut point2 = IndexMap::new();
    point2.insert("x".to_string(), Value::Int(3));
    point2.insert("y".to_string(), Value::Null);

    let chunk = data_chunk(vec![
        column(
            LogicalTypes::boolean(),
            vec![Value::Bool(true), Value::Bool(false), Value::Null],
            some_name("flag"),
        ),
        column(
            LogicalTypes::bigint(),
            vec![Value::Int(1), Value::Int(2), Value::Null],
            some_name("amount"),
        ),
        column(
            LogicalTypes::decimal(10, 2),
            vec![
                Value::String("12.34".to_string()),
                Value::Decimal(DecimalValue {
                    value: -500,
                    width: 10,
                    scale: 2,
                }),
                Value::Null,
            ],
            some_name("price"),
        ),
        column(
            LogicalTypes::list(LogicalTypes::integer()),
            vec![
                Value::List(vec![Value::Int(1), Value::Int(2)]),
                Value::Null,
                Value::List(vec![]),
            ],
            some_name("items"),
        ),
        column(
            LogicalTypes::array(LogicalTypes::varchar(), 2),
            vec![
                Value::List(vec![Value::String("a".into()), Value::String("b".into())]),
                Value::Null,
                Value::List(vec![Value::String("c".into()), Value::String("d".into())]),
            ],
            some_name("pair"),
        ),
        column(
            struct_type,
            vec![Value::Struct(point0), Value::Null, Value::Struct(point2)],
            some_name("point"),
        ),
    ])
    .unwrap();

    let mut writer = binary::BinaryWriter::new();
    encode_data_chunk(&mut writer, &chunk).unwrap();
    let bytes = writer.into_bytes();
    let mut reader = binary::BinaryReader::new(&bytes);
    let decoded = decode_data_chunk(&mut reader).unwrap();
    let rows = rows_from_chunk_with_names(&decoded, chunk.column_names.as_ref().unwrap()).unwrap();

    assert_eq!(rows[0]["flag"], Value::Bool(true));
    assert_eq!(rows[1]["flag"], Value::Bool(false));
    assert_eq!(rows[2]["flag"], Value::Null);
    assert_eq!(rows[0]["amount"], Value::Int(1));
    match &rows[0]["price"] {
        Value::Decimal(value) => assert_eq!(decimal_to_string(value), "12.34"),
        other => panic!("unexpected decimal value {other:?}"),
    }
    match &rows[1]["price"] {
        Value::Decimal(value) => assert_eq!(decimal_to_string(value), "-5.00"),
        other => panic!("unexpected decimal value {other:?}"),
    }
    assert_eq!(
        rows[0]["items"],
        Value::List(vec![Value::Int(1), Value::Int(2)])
    );
    assert_eq!(rows[1]["items"], Value::Null);
    assert_eq!(
        rows[2]["pair"],
        Value::List(vec![Value::String("c".into()), Value::String("d".into())])
    );
}

#[test]
fn messages_round_trip_prepare_response_with_results() {
    let chunk = data_chunk(vec![
        column(
            LogicalTypes::integer(),
            vec![Value::Int(1), Value::Null, Value::Int(3)],
            some_name("id"),
        ),
        column(
            LogicalTypes::varchar(),
            vec![
                Value::String("one".into()),
                Value::Null,
                Value::String("three".into()),
            ],
            some_name("label"),
        ),
    ])
    .unwrap();
    let message = QuackMessage::PrepareResponse {
        header: MessageHeader::new(MessageType::PrepareResponse)
            .with_connection("conn-1")
            .with_client_query_id(7),
        result_types: chunk.types.clone(),
        result_names: vec!["id".to_string(), "label".to_string()],
        needs_more_fetch: false,
        results: vec![chunk],
        result_uuid: binary::HugeIntParts {
            upper: 123,
            lower: 456,
        },
    };

    let decoded = decode_message(&encode_message(&message).unwrap()).unwrap();

    match decoded {
        QuackMessage::PrepareResponse {
            header,
            result_names,
            results,
            result_uuid,
            ..
        } => {
            assert_eq!(header.connection_id.as_deref(), Some("conn-1"));
            assert_eq!(header.client_query_id, Some(7));
            assert_eq!(result_names, vec!["id", "label"]);
            assert_eq!(
                result_uuid,
                binary::HugeIntParts {
                    upper: 123,
                    lower: 456
                }
            );
            let rows = rows_from_chunk_with_names(&results[0], &result_names).unwrap();
            assert_eq!(
                rows,
                vec![
                    row(vec![
                        ("id", Value::Int(1)),
                        ("label", Value::String("one".into()))
                    ]),
                    row(vec![("id", Value::Null), ("label", Value::Null)]),
                    row(vec![
                        ("id", Value::Int(3)),
                        ("label", Value::String("three".into()))
                    ]),
                ]
            );
        }
        other => panic!("unexpected message {other:?}"),
    }
}

#[test]
fn parses_documented_quack_uri_forms() {
    assert_eq!(
        parse_quack_uri("localhost:9494", None).unwrap(),
        ParsedQuackUri {
            base_url: "http://localhost:9494".to_string(),
            host: "localhost".to_string(),
            port: 9494,
            ssl: false,
        }
    );
    assert_eq!(
        parse_quack_uri("localhost", None).unwrap().base_url,
        "http://localhost:9494"
    );
    assert_eq!(
        parse_quack_uri("quack:localhost", None).unwrap().base_url,
        "http://localhost:9494"
    );
    assert_eq!(
        parse_quack_uri("quack://localhost:9000", None)
            .unwrap()
            .base_url,
        "http://localhost:9000"
    );
    assert_eq!(
        parse_quack_uri("quack:[::1]:9494", None).unwrap().base_url,
        "http://[::1]:9494"
    );
}

#[test]
fn formats_sql_literals_and_placeholders() {
    assert_eq!(
        sql_literal(&SqlParameter::from("can't")).unwrap(),
        "'can''t'"
    );
    assert_eq!(sql_literal(&SqlParameter::from(true)).unwrap(), "TRUE");
    assert_eq!(
        sql_literal(&SqlParameter::from(Value::HugeInt(42))).unwrap(),
        "42"
    );
    assert_eq!(
        sql_literal(&SqlParameter::List(vec![
            SqlParameter::from(1),
            SqlParameter::from("two"),
            SqlParameter::from(Value::Null),
        ]))
        .unwrap(),
        "[1, 'two', NULL]"
    );
    assert_eq!(
        sql_literal(&SqlParameter::from(
            date_from_iso_date("2020-01-02").unwrap()
        ))
        .unwrap(),
        "DATE '2020-01-02'"
    );
    assert_eq!(
        sql_literal(&SqlParameter::from(timestamp_value(
            1,
            TimestampUnit::Seconds,
            false,
        )))
        .unwrap(),
        "TIMESTAMP '1970-01-01 00:00:01.000'"
    );
    assert_eq!(
        sql_literal(&SqlParameter::from(interval_value(1, 2, 3))).unwrap(),
        "INTERVAL '1 months 2 days 3 microseconds'"
    );

    assert_eq!(
        format_sql(
            "SELECT '?' AS s, ? AS v -- ?\n",
            Some(&SqlParameters::Positional(vec![SqlParameter::from(42)])),
        )
        .unwrap(),
        "SELECT '?' AS s, 42 AS v -- ?\n"
    );

    let mut named = IndexMap::new();
    named.insert("id".to_string(), SqlParameter::from(7));
    named.insert("label".to_string(), SqlParameter::from("seven"));
    assert_eq!(
        format_sql(
            "SELECT :id::INTEGER AS id, :label AS label",
            Some(&SqlParameters::Named(named)),
        )
        .unwrap(),
        "SELECT 7::INTEGER AS id, 'seven' AS label"
    );

    assert!(
        format_sql(
            "SELECT ?, ?",
            Some(&SqlParameters::Positional(vec![SqlParameter::from(1)])),
        )
        .is_err()
    );
}

#[test]
fn builds_chunks_from_rows() {
    let rows = vec![
        row(vec![
            ("id", Value::Int(1)),
            ("label", Value::String("one".into())),
            ("amount", Value::String("12.34".into())),
        ]),
        row(vec![
            ("id", Value::Int(2)),
            ("label", Value::String("two".into())),
            ("amount", Value::Null),
        ]),
    ];
    let chunk = data_chunk_from_rows(
        &rows,
        Some(vec![
            ColumnDefinition {
                name: "id".to_string(),
                logical_type: LogicalTypes::integer(),
            },
            ColumnDefinition {
                name: "label".to_string(),
                logical_type: LogicalTypes::varchar(),
            },
            ColumnDefinition {
                name: "amount".to_string(),
                logical_type: LogicalTypes::decimal(10, 2),
            },
        ]),
    )
    .unwrap();

    assert_eq!(
        chunk.types.iter().map(|typ| typ.id).collect::<Vec<_>>(),
        vec![
            LogicalTypeId::Integer,
            LogicalTypeId::Varchar,
            LogicalTypeId::Decimal,
        ]
    );
    assert_eq!(rows_from_chunk(&chunk).unwrap(), rows);
}

#[test]
fn converts_quack_values_to_json() {
    assert_eq!(
        to_json_value(&Value::HugeInt(9007199254740993), JsonOptions::default()).unwrap(),
        serde_json::json!("9007199254740993")
    );
    assert_eq!(
        to_json_value(&Value::Bytes(vec![104, 105]), JsonOptions::default()).unwrap(),
        serde_json::json!("aGk=")
    );
    assert_eq!(
        to_json_value(
            &decimal_value("12.34", 10, 2).unwrap(),
            JsonOptions::default()
        )
        .unwrap(),
        serde_json::json!("12.34")
    );
    assert_eq!(
        to_json_value(
            &date_from_iso_date("2020-01-02").unwrap(),
            JsonOptions::default()
        )
        .unwrap(),
        serde_json::json!("2020-01-02")
    );
    assert_eq!(
        to_json_value(
            &time_value(1_234_567, TimeUnit::Micros),
            JsonOptions::default()
        )
        .unwrap(),
        serde_json::json!("00:00:01.234567")
    );
    assert_eq!(
        to_json_value(
            &timestamp_value(1_234_567, TimestampUnit::Micros, false),
            JsonOptions::default(),
        )
        .unwrap(),
        serde_json::json!("1970-01-01T00:00:01.234567Z")
    );
}
