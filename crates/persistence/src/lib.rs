// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

pub use error::Error;
pub use lmdb::{Lmdb, LmdbBatch};
pub use memory::{Memory, MemoryScanIter};
pub use persistence::{BeginBatch, Persistence, PersistenceBatch};
use std::result;

mod error;
mod lmdb;
mod memory;
mod persistence;
pub mod test;

pub type Result<T> = result::Result<T, Error>;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

/// An reifydb_engine operation emitted by the Emit reifydb_engine.
pub enum Operation {
    Set { key: Key, value: Value },
    Remove { key: Key },
}

/// A scan iterator over key-value pairs, returned by [`reifydb_persistence::scan()`].
pub trait ScanIterator: Iterator<Item = Result<(Key, Value)>> {}

impl<T> ScanIterator for T where T: Iterator<Item = Result<(Key, Value)>> {}
