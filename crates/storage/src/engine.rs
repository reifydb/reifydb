// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the toydb project (https://github.com/erikgrinaker/toydb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Erik Grinaker
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0
use crate::Result;
use base::encoding::keycode;
use std::ops::RangeBounds;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

/// A scan iterator over key-value pairs, returned by [`StorageEngine::scan()`].
pub trait ScanIterator: DoubleEndedIterator<Item = Result<(Key, Value)>> {}

/// Blanket implementation for all iterators that can act as a scan iterator.
impl<I: DoubleEndedIterator<Item = Result<(Key, Value)>>> ScanIterator for I {}

pub trait StorageEngine: Send + Sync {
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
    fn scan(&self, range: impl RangeBounds<Key>) -> Self::ScanIter<'_>;

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

    /// Flushes all pending writes to durable storage, if applicable.
    ///
    /// This may be a no-op for in-memory engines.
    ///
    /// # Errors
    /// Returns an error if syncing fails (e.g., I/O error).
    fn sync(&mut self) -> Result<()>;
}
