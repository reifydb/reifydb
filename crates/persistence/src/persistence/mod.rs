// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Key, Value};
use std::ops::RangeBounds;

pub trait BeginBatch {
    type Batch<'a>: PersistenceBatch + 'a
    where
        Self: 'a;

    fn begin_batch(&self) -> crate::Result<Self::Batch<'_>>;
}

pub trait Persistence: Send + Sync {
    /// An associated type representing the iterator returned by `scan` and `scan_range`.
    ///
    /// The iterator yields ordered key-value pairs and must implement [`ScanIterator`].
    /// The lifetime `'a` ensures the iterator does not outlive the engine reference.
    type ScanIter<'a>: Iterator<Item = crate::Result<(Key, Value)>> + 'a
    where
        Self: 'a;

    /// Retrieves the value associated with the given `key`, if it exists.
    ///
    /// # Returns
    /// - `Ok(Some(value))` if the key exists.
    /// - `Ok(None)` if the key is not present.
    /// - `Err(e)` if an error occurs during retrieval.
    fn get(&self, key: &Key) -> crate::Result<Option<Value>>;

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
    fn scan_range(&self, prefix: &Key) -> Self::ScanIter<'_> {
        // self.scan(keycode::prefix_range(prefix))
        unimplemented!()
    }
    /// Inserts or updates the given `value` at the specified `key`.
    ///
    /// If the key already exists, its value is replaced.
    ///
    /// # Errors
    /// Returns an error if the operation fails (e.g., due to I/O or internal state).
    fn set(&mut self, key: &Key, value: Value) -> crate::Result<()>;

    /// Removes the value associated with the given `key`, if it exists.
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    fn remove(&mut self, key: &Key) -> crate::Result<()>;

    /// Flushes all pending writes to durable store, if applicable.
    ///
    /// This may be a no-op for in-memory engines.
    ///
    /// # Errors
    /// Returns an error if syncing fails (e.g., I/O error).
    fn sync(&mut self) -> crate::Result<()>;
}

pub trait PersistenceBatch {
    /// Inserts or updates the given `value` at the specified `key`.
    ///
    /// If the key already exists, its value is replaced.
    ///
    /// # Errors
    /// Returns an error if the operation fails (e.g., due to I/O or internal state).
    fn set(&mut self, key: &Key, value: Value) -> crate::Result<()>;

    /// Removes the value associated with the given `key`, if it exists.
    ///
    /// # Errors
    /// Returns an error if the operation fails.
    fn remove(&mut self, key: &Key) -> crate::Result<()>;

    fn complete(self) -> crate::Result<()>;

    fn abort(self) -> crate::Result<()>;
}
