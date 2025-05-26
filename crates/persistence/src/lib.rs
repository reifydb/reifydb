// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// #![cfg_attr(not(debug_assertions), deny(missing_docs))]
// #![cfg_attr(not(debug_assertions), deny(warnings))]
// #![cfg_attr(not(debug_assertions), deny(clippy::unwrap_used))]
// #![cfg_attr(not(debug_assertions), deny(clippy::expect_used))]

use base::encoding::keycode;
pub use buffer::{Buffer, BufferScanIter};
pub use error::Error;
pub use lmdb::{Lmdb, LmdbBatch};
pub use memory::{Memory, MemoryScanIter};
use std::ops::RangeBounds;
use std::result;

mod buffer;
mod error;
mod lmdb;
mod memory;
pub mod test;

pub type Result<T> = result::Result<T, Error>;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

/// An engine operation emitted by the Emit engine.
pub enum Operation {
    Set { key: Key, value: Value },
    Remove { key: Key },
}

/// A scan iterator over key-value pairs, returned by [`Persistence::scan()`].
pub trait ScanIterator: DoubleEndedIterator<Item = Result<(Key, Value)>> {}

/// Blanket implementation for all iterators that can act as a scan iterator.
impl<I: DoubleEndedIterator<Item = Result<(Key, Value)>>> ScanIterator for I {}

pub trait BeginBatch {
    type Batch<'a>: PersistenceBatch + 'a
    where
        Self: 'a;

    fn begin_batch(&self) -> Result<Self::Batch<'_>>;
}

pub trait Persistence: Send + Sync {
    /// An associated type representing the iterator returned by `scan` and `scan_prefix`.
    ///
    /// The iterator yields ordered key-value pairs and must implement [`ScanIterator`].
    /// The lifetime `'a` ensures the iterator does not outlive the engine reference.
    type ScanIter<'a>: ScanIterator + 'a
    where
        Self: 'a;

    /// Retrieves the value associated with the given `key`, if it exists.
    ///
    /// # Returns
    /// - `Ok(Some(value))` if the key exists.
    /// - `Ok(None)` if the key is not present.
    /// - `Err(e)` if an error occurs during retrieval.
    fn get(&self, key: &Key) -> Result<Option<Value>>;

    /// Iterates over an ordered range of key-value pairs within the given bounds.
    ///
    /// The range can be inclusive, exclusive, or unbounded at either end.
    ///
    /// # Returns
    /// A [`ScanIter`] that yields `Result<(Key, Value)>` in sorted order.
    fn scan(&self, range: impl RangeBounds<Key> + Clone) -> Self::ScanIter<'_>;

    /// Iterates over all key-value pairs that begin with the given `prefix`.
    ///
    /// This is a convenience wrapper around `scan()` using a prefix-based range.
    ///
    /// # Returns
    /// A [`ScanIter`] over key-value pairs whose keys start with the prefix.
    fn scan_prefix(&self, prefix: &Key) -> Self::ScanIter<'_> {
        self.scan(keycode::prefix_range(prefix))
    }
    /// Inserts or updates the given `value` at the specified `key`.
    ///
    /// If the key already exists, its value is replaced.
    ///
    /// # Errors
    /// Returns an error if the operation fails (e.g., due to I/O or internal state).
    fn set(&mut self, key: &Key, value: Value) -> Result<()>;

    /// Removes the value associated with the given `key`, if it exists.
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    fn remove(&mut self, key: &Key) -> Result<()>;

    /// Flushes all pending writes to durable store, if applicable.
    ///
    /// This may be a no-op for in-memory engines.
    ///
    /// # Errors
    /// Returns an error if syncing fails (e.g., I/O error).
    fn sync(&mut self) -> Result<()>;
}

pub trait PersistenceBatch {
    /// Inserts or updates the given `value` at the specified `key`.
    ///
    /// If the key already exists, its value is replaced.
    ///
    /// # Errors
    /// Returns an error if the operation fails (e.g., due to I/O or internal state).
    fn set(&mut self, key: &Key, value: Value) -> Result<()>;

    /// Removes the value associated with the given `key`, if it exists.
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    fn remove(&mut self, key: &Key) -> Result<()>;

    fn complete(self) -> Result<()>;

    fn abort(self) -> Result<()>;
}
