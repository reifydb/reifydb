// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use super::*;
use crate::skipdb::skipdbcore::types::Values;
use std::{collections::hash_map::RandomState, hash::Hash};

mod write;
use crate::skipdb::conflict::HashCm;
use crate::skipdb::pending::BTreePwm;
use crate::skipdb::skipdbcore::{AsSkipCore, SkipCore};
pub use write::*;

#[cfg(test)]
mod tests;

struct Inner<K, V, S = RandomState> {
    tm: Tm<K, V, HashCm<K, S>, BTreePwm<K, V>>,
    map: SkipCore<K, V>,
    hasher: S,
}

impl<K, V, S> Inner<K, V, S> {
    fn new(name: &str, hasher: S) -> Self {
        let tm = Tm::new(name, 0);
        Self { tm, map: SkipCore::new(), hasher }
    }

    fn version(&self) -> u64 {
        self.tm.version()
    }
}

/// A concurrent MVCC in-memory key-value database.
///
/// `OptimisticDb` requires key to be [`Ord`] and [`Hash`](Hash).
///
/// Comparing to [`SerializableDb`](crate::serializable::SerializableDb):
/// 1. `SerializableDb` support full serializable snapshot isolation, which can detect both direct dependencies and indirect dependencies.
/// 2. `SerializableDb` does not require key to implement [`Hash`](core::hash::Hash).
/// 3. But, [`OptimisticDb`](crate::optimistic::OptimisticDb) has more flexible write transaction APIs and no clone happen.
pub struct OptimisticDb<K, V, S = RandomState> {
    inner: Arc<Inner<K, V, S>>,
}

#[doc(hidden)]
impl<K, V, S> AsSkipCore<K, V> for OptimisticDb<K, V, S> {
    #[allow(private_interfaces)]
    fn as_inner(&self) -> &SkipCore<K, V> {
        &self.inner.map
    }
}

impl<K, V, S> Clone for OptimisticDb<K, V, S> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<K, V> Default for OptimisticDb<K, V> {
    /// Creates a new `OptimisticDb` with the default options.

    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> OptimisticDb<K, V> {
    /// Creates a new `OptimisticDb` with the given options.
    pub fn new() -> Self {
        Self::with_hasher(Default::default())
    }
}

impl<K, V, S> OptimisticDb<K, V, S> {
    /// Creates a new `OptimisticDb` with the given hasher.

    pub fn with_hasher(hasher: S) -> Self {
        let inner = Arc::new(Inner::new(core::any::type_name::<Self>(), hasher));
        Self { inner }
    }

    /// Returns the current read version of the database.

    pub fn version(&self) -> u64 {
        self.inner.version()
    }

    /// Create a read transaction.

    pub fn read(&self) -> ReadTransaction<K, V, OptimisticDb<K, V, S>, HashCm<K, S>> {
        ReadTransaction::new(self.clone(), self.inner.tm.read())
    }
}

impl<K, V, S> OptimisticDb<K, V, S>
where
    K: Ord + Eq + Hash,
    V: 'static,
    S: BuildHasher + Clone,
{
    /// Create a optimistic write transaction.
    ///
    /// Optimistic write transaction is not a totally Serializable Snapshot Isolation transaction.
    /// It can handle most of write skew anomaly, but not all. Basically, all directly dependencies
    /// can be handled, but indirect dependencies (logical dependencies) can not be handled.
    /// If you need a totally Serializable Snapshot Isolation transaction, you should use
    /// [`SerializableDb`](crate::serializable::SerializableDb) instead.

    pub fn write(&self) -> OptimisticTransaction<K, V, S> {
        OptimisticTransaction::new(self.clone(), None)
    }

    /// Create a optimistic write transaction with the given capacity hint.

    pub fn write_with_capacity(&self, capacity: usize) -> OptimisticTransaction<K, V, S> {
        OptimisticTransaction::new(self.clone(), Some(capacity))
    }
}

impl<K, V, S> OptimisticDb<K, V, S>
where
    K: Ord + Eq + Hash + Send + 'static,
    V: Send + 'static,
    Values<V>: Send,
    S: BuildHasher + Clone,
{
    /// Compact the database.

    pub fn compact(&self) {
        self.inner.map.compact(self.inner.tm.discard_hint());
    }
}
