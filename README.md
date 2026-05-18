# quack_protocol

Rust client-side SDK for DuckDB's experimental Quack remote protocol.

The crate implements:

- DuckDB `BinarySerializer`-compatible primitive, object, logical type, vector, and `DataChunk` codecs.
- Quack connection, prepare/query, fetch, append, disconnect, success, and error messages.
- Async HTTP `POST /quack` transport using `application/duckdb`.
- URI parsing for `localhost:9494`, `quack:host:port`, bracketed IPv6, and direct HTTP(S) URLs.
- SQL literal formatting for positional and named parameters.

```rust
use quack_protocol::{QuackClient, QuackClientOptions, Result};

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = QuackClient::connect(
        "localhost:9494",
        QuackClientOptions {
            auth_token: Some("super_secret".to_string()),
            ..Default::default()
        },
    )
    .await?;

    let result = client.query("SELECT 42::INTEGER AS answer").await?;
    println!("{:?}", result.rows()?);

    client.disconnect().await?;
    Ok(())
}
```

Quack is still experimental upstream and not yet covered by a stable official wire spec. This implementation follows DuckDB's `duckdb-quack` extension and the reverse-engineered TypeScript SDK behavior available at the time it was written.

The integration test is opt-in:

```sh
QUACK_SERVER_URI=quack:127.0.0.1:9494 QUACK_AUTH_TOKEN=super_secret cargo test --test integration
```

The CI workflow starts a local DuckDB Quack server and runs the integration tests against it on pull requests. The manual release workflow publishes to crates.io using the `CARGO_REGISTRY_TOKEN` repository secret, then creates a GitHub release for the crate version.
