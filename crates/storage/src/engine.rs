// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes portions of code from https://github.com/erikgrinaker/toydb (Apache 2 License).
// Original Apache 2 License Copyright (c) erikgrinaker 2024.

use crate::Result;
use std::ops::RangeBounds;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

/// A scan iterator over key-value pairs, returned by [`Engine::scan()`].
pub trait ScanIterator: DoubleEndedIterator<Item = Result<(Key, Value)>> {}

/// Blanket implementation for all iterators that can act as a scan iterator.
impl<I: DoubleEndedIterator<Item = Result<(Key, Value)>>> ScanIterator for I {}

pub trait Engine: Send {
    type ScanIterator<'a>: ScanIterator + 'a
    where
        Self: 'a;

    /// Gets a value for a key, if exists
    fn get(&self, key: &Key) -> Result<Option<Value>>;

    /// Iterates over an ordered range of key-value pairs
    fn scan(&self, range: impl RangeBounds<Key>) -> Self::ScanIterator<'_>;

    /// Iterates over all key-value pairs starting with the given prefix.
    fn scan_prefix(&mut self, prefix: &Key) -> Self::ScanIterator<'_> {
        unimplemented!()
    }
}

pub trait EngineMut: Engine + Send {
    /// Sets or replaces value for a key
    fn set(&mut self, key: &Key, value: Value) -> Result<()>;

    /// Removes value for a key
    fn remove(&mut self, key: &Key) -> Result<()>;
}
