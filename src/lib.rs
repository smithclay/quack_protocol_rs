//! Rust client-side SDK for DuckDB's experimental Quack remote protocol.
//!
//! The crate implements the HTTP client transport plus the DuckDB
//! `BinarySerializer`-compatible message, logical type, and `DataChunk` codecs
//! needed by a Quack client.

pub mod binary;
pub mod builders;
pub mod client;
pub mod constants;
pub mod errors;
pub mod json;
pub mod logical_types;
pub mod messages;
pub mod sql;
pub mod values;
pub mod vector;

pub use builders::*;
pub use client::*;
pub use errors::{QuackError, Result};
pub use json::*;
pub use logical_types::*;
pub use messages::*;
pub use sql::*;
pub use values::*;
pub use vector::*;
