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
use crate::mvcc::store::Store;
use crate::mvcc::transaction::TransactionManager;

use crate::Version;
use crate::mvcc::types::Committed;
use crate::mvcc::transaction::scan::iter::Iter;
use crate::mvcc::transaction::scan::range::Range;
use crate::mvcc::transaction::scan::rev_iter::RevIter;
use crate::mvcc::transaction::scan::rev_range::RevRange;
pub use read::TransactionRx;
use reifydb_persistence::Key;
pub use write::TransactionTx;

mod read;
mod write;

pub struct Inner {
    tm: TransactionManager<BTreeConflict, BTreePendingWrites>,
    store: Store,
}

impl Inner {
    fn new(name: &str) -> Self {
        let tm = TransactionManager::new(name, 0);
        Self { tm, store: Store::new() }
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
        self.store.get(key, version)
    }

    pub fn contains_key(&self, key: &Key, version: Version) -> bool {
        self.store.contains_key(key, version)
    }

    pub fn iter(&self, version: Version) -> Iter<'_> {
        self.store.iter(version)
    }

    pub fn iter_rev(&self, version: Version) -> RevIter<'_> {
        self.store.iter_rev(version)
    }

    pub fn range<R>(&self, range: R, version: Version) -> Range<'_, R>
    where
        R: RangeBounds<Key>,
    {
        self.store.range(range, version)
    }

    pub fn range_rev<R>(&self, range: R, version: Version) -> RevRange<'_, R>
    where
        R: RangeBounds<Key>,
    {
        self.store.range_rev(range, version)
    }
}
