// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::sync::Arc;

pub use read::*;
use reifydb_storage::memory::Memory;
pub use write::*;

pub(crate) mod read;
#[allow(clippy::module_inception)]
mod write;

use crate::mvcc::conflict::BTreeConflict;
use crate::mvcc::pending::BTreePendingWrites;
use crate::mvcc::transaction::TransactionManager;
use crate::mvcc::transaction::serializable::read::ReadTransaction;

struct Inner {
    tm: TransactionManager<BTreeConflict, BTreePendingWrites>,
    map: Memory,
}

impl Inner {
    fn new(name: &str) -> Self {
        let tm = TransactionManager::new(name, 0);
        Self { tm, map: Memory::new() }
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
pub struct Serializable {
    inner: Arc<Inner>,
}


impl Clone for Serializable {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl Default for Serializable {
    /// Creates a new `SerializableDb` with the default options.
    fn default() -> Self {
        Self::new()
    }
}

impl Serializable {
    /// Creates a new `SerializableDb`
    pub fn new() -> Self {
        Self { inner: Arc::new(Inner::new(core::any::type_name::<Self>())) }
    }
}

impl Serializable {
    /// Returns the current read version of the database.
    pub fn version(&self) -> u64 {
        self.inner.version()
    }

    /// Create a read transaction.
    pub fn read(&self) -> ReadTransaction<BTreeConflict> {
        ReadTransaction::new(self.clone(), self.inner.tm.read())
    }
}

impl Serializable {
    /// Create a serializable write transaction.
    ///
    /// Serializable write transaction is a totally Serializable Snapshot Isolation transaction.
    /// It can handle all kinds of write skew anomaly, including indirect dependencies (logical dependencies).
    /// If in your code, you do not care about indirect dependencies (logical dependencies), you can use
    /// [`SerializableDb::optimistic_write`](Serializable::optimistic_write) instead.

    pub fn write(&self) -> SerializableTransaction {
        SerializableTransaction::new(self.clone())
    }
}
