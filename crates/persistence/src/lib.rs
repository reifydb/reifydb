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
use reifydb_core::AsyncCowVec;
use std::{cmp, result};

mod error;
mod lmdb;
mod memory;
mod persistence;
pub mod test;

pub type Result<T> = result::Result<T, Error>;

pub type Key = AsyncCowVec<u8>;
pub type Value = AsyncCowVec<u8>;

/// A scan iterator over key-value pairs, returned by [`reifydb_persistence::scan()`].
pub trait ScanIterator: Iterator<Item = Result<(Key, Value)>> {}

impl<T> ScanIterator for T where T: Iterator<Item = Result<(Key, Value)>> {}

/// Operation on a key-value pair.
#[derive(Debug, PartialEq, Eq, Hash)]
pub enum Action {
    Set { key: Key, value: Value },
    Remove { key: Key },
}

impl PartialOrd for Action {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Action {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.key().cmp(other.key())
    }
}

impl Action {
    /// Returns the key
    pub fn key(&self) -> &Key {
        match self {
            Self::Set { key, .. } => key,
            Self::Remove { key } => key,
        }
    }

    /// Returns the value, if None, it means the entry is marked as remove.
    pub fn value(&self) -> Option<&Value> {
        match self {
            Self::Set { value, .. } => Some(value),
            Self::Remove { .. } => None,
        }
    }
}

impl Clone for Action {
    fn clone(&self) -> Self {
        match self {
            Self::Set { key, value } => Self::Set { key: key.clone(), value: value.clone() },
            Self::Remove { key } => Self::Remove { key: key.clone() },
        }
    }
}
