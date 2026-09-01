# quack_protocol

Rust client-side SDK for DuckDB's experimental Quack remote protocol.

The crate implements:

- DuckDB `BinarySerializer`-compatible primitive, object, logical type, vector, and `DataChunk` codecs.
- Quack connection, prepare/query, fetch, append, disconnect, success, and error messages.
- Async HTTP `POST /quack` transport using `application/duckdb`.
- URI parsing for `localhost:9494`, `quack:host:port`, bracketed IPv6, and direct HTTP(S) URLs.
- SQL literal formatting for positional and named parameters.

```rust
use futures_util::TryStreamExt;
use quack_protocol::{QuackClient, QuackClientOptions, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = QuackClient::connect(
        "localhost:9494",
        QuackClientOptions {
            auth_token: Some("super_secret".to_string()),
            ..Default::default()
        },
    )
    .await?;

    let (_columns, rows) = client
        .query("SELECT 42::INTEGER AS answer", None)
        .await?
        .into_rows();
    let rows: Vec<_> = rows.try_collect().await?;
    println!("{:?}", rows);

    client.disconnect().await?;
    Ok(())
}
```

## Arrow output

The optional `arrow` feature adds a read-only bridge that turns query results
into `arrow_array::RecordBatch` streams. The Arrow schema is built from the
column definitions returned at prepare time, so it is correct even for results
that carry no rows.

```toml
[dependencies]
quack_protocol = { version = "0.2", features = ["arrow"] }
```

```rust
use futures_util::TryStreamExt;
use quack_protocol::arrow::arrow_array::RecordBatch;
use quack_protocol::{QuackClient, QuackClientOptions, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let client = QuackClient::connect("localhost:9494", QuackClientOptions::default()).await?;

    let (schema, batches) = client
        .query("SELECT 42::INTEGER AS answer", None)
        .await?
        .into_record_batches()?;
    let batches: Vec<RecordBatch> = batches.try_collect().await?;
    println!("{schema}: {} rows", batches.iter().map(RecordBatch::num_rows).sum::<usize>());

    client.disconnect().await?;
    Ok(())
}
```

`quack_protocol::arrow` also exposes `schema()` and `to_record_batch()` for
converting chunks by hand, plus `arrow_type()` for the `LogicalType` mapping.
The `arrow-array`, `arrow-buffer`, and `arrow-schema` crates are re-exported so
downstream code can match versions. `TIMETZ`, `UNION`, and `VARINT` have no
Arrow mapping yet and produce `QuackError::UnsupportedType`.

Quack is still experimental upstream and not yet covered by a stable official wire spec. This implementation follows DuckDB's `duckdb-quack` extension.
