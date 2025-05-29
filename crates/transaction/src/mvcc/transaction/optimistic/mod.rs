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
use std::{collections::hash_map::RandomState, hash::Hash};

use crate::mvcc::DefaultHasher;
use crate::mvcc::conflict::BTreeConflict;
use crate::mvcc::pending::BTreePendingWrites;
use crate::mvcc::skipdbcore::{AsSkipCore, SkipCore};
use crate::mvcc::transaction::TransactionManager;

pub use read::TransactionRx;
pub use write::TransactionTx;

mod read;
mod write;

#[cfg(test)]
mod tests;

struct Inner<K, V> {
    tm: TransactionManager<K, V, BTreeConflict<K>, BTreePendingWrites<K, V>>,
    mem_table: SkipCore<K, V>,
    hasher: RandomState,
}

impl<K, V> Inner<K, V> {
    fn new(name: &str) -> Self {
        let tm = TransactionManager::new(name, 0);
        Self { tm, mem_table: SkipCore::new(), hasher: DefaultHasher::default() }
    }

    fn version(&self) -> u64 {
        self.tm.version()
    }
}

pub struct Optimistic<K, V> {
    inner: Arc<Inner<K, V>>,
}

#[doc(hidden)]
impl<K, V> AsSkipCore<K, V> for Optimistic<K, V> {
    #[allow(private_interfaces)]
    fn as_inner(&self) -> &SkipCore<K, V> {
        &self.inner.mem_table
    }
}

impl<K, V> Clone for Optimistic<K, V> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<K, V> Default for Optimistic<K, V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Optimistic<K, V> {
    pub fn new() -> Self {
        let inner = Arc::new(Inner::new(core::any::type_name::<Self>()));
        Self { inner }
    }
}

impl<K, V> Optimistic<K, V> {
    /// Returns the current read version of the database.
    pub fn version(&self) -> u64 {
        self.inner.version()
    }

    /// Create a read transaction.
    pub fn read(&self) -> TransactionRx<K, V> {
        TransactionRx::new(self.clone())
    }
}

impl<K, V> Optimistic<K, V>
where
    K: Clone + Ord + Eq + Hash,
    V: 'static,
{
    pub fn write(&self) -> TransactionTx<K, V> {
        TransactionTx::new(self.clone())
    }
}

impl<K, V> Optimistic<K, V>
where
    K: Ord + Eq + Hash + Send + 'static,
    V: Send + 'static,
    Values<V>: Send,
{
    pub fn compact(&self) {
        self.inner.mem_table.compact(self.inner.tm.discard_hint());
    }
}

pub enum Transaction<K, V> {
    Rx(TransactionRx<K, V>),
    Tx(TransactionTx<K, V>),
}
