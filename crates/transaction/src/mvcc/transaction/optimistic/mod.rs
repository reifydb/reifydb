// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::ops::{Deref, RangeBounds};
use std::sync::Arc;

use crate::mvcc::conflict::BTreeConflict;
use crate::mvcc::pending::BTreePendingWrites;
use crate::mvcc::transaction::TransactionManager;

use crate::mvcc::types::Committed;
pub use read::TransactionRx;
use reifydb_persistence::Key;
use reifydb_storage::memory::{Iter, IterRev, Memory, Range, RangeRev};
use reifydb_storage::{Contains, Get, Scan, ScanRange, ScanRangeRev, ScanRev, Version};
pub use write::TransactionTx;

mod read;
mod write;

pub struct Inner {
    tm: TransactionManager<BTreeConflict, BTreePendingWrites>,
    storage: Memory,
}

impl Inner {
    fn new(name: &str) -> Self {
        let tm = TransactionManager::new(name, 0);
        Self { tm, storage: Memory::new() }
    }

    fn version(&self) -> u64 {
        self.tm.version()
    }
}

pub struct Optimistic {
    inner: Arc<Inner>,
}

impl Deref for Optimistic {
    type Target = Inner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Clone for Optimistic {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl Default for Optimistic {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimistic {
    pub fn new() -> Self {
        let inner = Arc::new(Inner::new(core::any::type_name::<Self>()));
        Self { inner }
    }
}

impl Optimistic {
    /// Returns the current read version of the database.
    pub fn version(&self) -> u64 {
        self.inner.version()
    }

    /// Create a read transaction.
    pub fn begin_read_only(&self) -> TransactionRx {
        TransactionRx::new(self.clone())
    }
}

impl Optimistic {
    pub fn begin(&self) -> TransactionTx {
        TransactionTx::new(self.clone())
    }
}

pub enum Transaction {
    Rx(TransactionRx),
    Tx(TransactionTx),
}

impl Optimistic {
    pub fn get(&self, key: &Key, version: Version) -> Option<Committed> {
        self.storage.get(key, version).map(|sv| sv.into())
    }

    pub fn contains_key(&self, key: &Key, version: Version) -> bool {
        self.storage.contains(key, version)
    }

    pub fn scan(&self, version: Version) -> Iter<'_> {
        self.storage.scan(version)
    }

    pub fn scan_rev(&self, version: Version) -> IterRev<'_> {
        self.storage.scan_rev(version)
    }

    pub fn scan_range<R>(&self, range: R, version: Version) -> Range<'_, R>
    where
        R: RangeBounds<Key>,
    {
        self.storage.scan_range(range, version)
    }

    pub fn scan_range_rev<R>(&self, range: R, version: Version) -> RangeRev<'_, R>
    where
        R: RangeBounds<Key>,
    {
        self.storage.scan_range_rev(range, version)
    }
}
