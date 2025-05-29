// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::skipdbcore::types::Values;
use std::sync::Arc;

pub use write::*;
pub use read::*;

mod read;
#[allow(clippy::module_inception)]
mod write;

use crate::mvcc::conflict::BTreeCm;
use crate::mvcc::pending::BTreePwm;
use crate::mvcc::skipdbcore::{AsSkipCore, SkipCore};
use crate::mvcc::transaction::Tm;
use crate::mvcc::transaction::optimistic::read::ReadTransaction;

struct Inner<K, V> {
    tm: Tm<K, V, BTreeCm<K>, BTreePwm<K, V>>,
    map: SkipCore<K, V>,
}

impl<K, V> Inner<K, V> {
    fn new(name: &str) -> Self {
        let tm = Tm::new(name, 0);
        Self { tm, map: SkipCore::new() }
    }

    fn version(&self) -> u64 {
        self.tm.version()
    }
}

/// A concurrent MVCC in-memory key-value database.
///
/// `SerializableDb` requires key to be [`Ord`] and [`Clone`].
/// The [`Clone`] bound here hints the user that the key should be cheap to clone,
/// because it will be cloned at least one time during the write transaction.
///
/// Comparing to [`OptimisticDb`](crate::optimistic::OptimisticDb):
/// 1. `SerializableDb` support full serializable snapshot isolation, which can detect both direct dependencies and indirect dependencies.
/// 2. `SerializableDb` does not require key to implement [`Hash`](core::hash::Hash).
/// 3. But, [`OptimisticDb`](crate::optimistic::OptimisticDb) has more flexible write transaction APIs.
#[repr(transparent)]
pub struct SerializableDb<K, V> {
    inner: Arc<Inner<K, V>>,
}

#[doc(hidden)]
impl<K, V> AsSkipCore<K, V> for SerializableDb<K, V> {
    fn as_inner(&self) -> &SkipCore<K, V> {
        &self.inner.map
    }
}

impl<K, V> Clone for SerializableDb<K, V> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<K, V> Default for SerializableDb<K, V> {
    /// Creates a new `SerializableDb` with the default options.

    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> SerializableDb<K, V> {
    /// Creates a new `SerializableDb`
    pub fn new() -> Self {
        Self { inner: Arc::new(Inner::new(core::any::type_name::<Self>())) }
    }
}

impl<K, V> SerializableDb<K, V> {
    /// Returns the current read version of the database.

    pub fn version(&self) -> u64 {
        self.inner.version()
    }

    /// Create a read transaction.

    pub fn read(&self) -> ReadTransaction<K, V, SerializableDb<K, V>, BTreeCm<K>> {
        ReadTransaction::new(self.clone(), self.inner.tm.read())
    }
}

impl<K, V> SerializableDb<K, V>
where
    K: Clone + Ord + 'static,
    V: 'static,
{
    /// Create a serializable write transaction.
    ///
    /// Serializable write transaction is a totally Serializable Snapshot Isolation transaction.
    /// It can handle all kinds of write skew anomaly, including indirect dependencies (logical dependencies).
    /// If in your code, you do not care about indirect dependencies (logical dependencies), you can use
    /// [`SerializableDb::optimistic_write`](SerializableDb::optimistic_write) instead.

    pub fn write(&self) -> SerializableTransaction<K, V> {
        SerializableTransaction::new(self.clone())
    }
}

impl<K, V> SerializableDb<K, V>
where
    K: Clone + Ord + Send + 'static,
    V: Send + 'static,
    Values<V>: Send,
{
    /// Compact the database.

    pub fn compact(&self) {
        self.inner.map.compact(self.inner.tm.discard_hint());
    }
}
