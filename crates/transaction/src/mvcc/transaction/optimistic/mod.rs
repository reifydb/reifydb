// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::ops::Deref;
use std::sync::Arc;

use crate::mvcc::conflict::BTreeConflict;
use crate::mvcc::pending::BTreePendingWrites;
use crate::mvcc::transaction::TransactionManager;

use crate::mvcc::types::Committed;
pub use read::TransactionRx;
use reifydb_storage::LocalClock;
use reifydb_storage::Storage;
use reifydb_storage::Version;
use reifydb_storage::{Key, KeyRange};
pub use write::TransactionTx;

mod read;
mod write;

pub struct Optimistic<S: Storage>(Arc<Inner<S>>);

impl<S: Storage> Deref for Optimistic<S> {
    type Target = Inner<S>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<S: Storage> Clone for Optimistic<S> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub struct Inner<S: Storage> {
    tm: TransactionManager<BTreeConflict, LocalClock, BTreePendingWrites>,
    storage: S,
}

impl<S: Storage> Inner<S> {
    fn new(name: &str, storage: S) -> Self {
        let tm = TransactionManager::new(name, LocalClock::new());
        Self { tm, storage }
    }

    fn version(&self) -> Version {
        self.tm.version()
    }
}

impl<S: Storage> Optimistic<S> {
    pub fn new(storage: S) -> Self {
        Self(Arc::new(Inner::new(core::any::type_name::<Self>(), storage)))
    }
}

impl<S: Storage> Optimistic<S> {
    pub fn version(&self) -> Version {
        self.0.version()
    }
    pub fn begin_read_only(&self) -> TransactionRx<S> {
        TransactionRx::new(self.clone())
    }
}

impl<S: Storage> Optimistic<S> {
    pub fn begin(&self) -> TransactionTx<S> {
        TransactionTx::new(self.clone())
    }
}

pub enum Transaction<S: Storage> {
    Rx(TransactionRx<S>),
    Tx(TransactionTx<S>),
}

impl<S: Storage> Optimistic<S> {
    pub fn get(&self, key: &Key, version: Version) -> Option<Committed> {
        self.storage.get(key, version).map(|sv| sv.into())
    }

    pub fn contains_key(&self, key: &Key, version: Version) -> bool {
        self.storage.contains(key, version)
    }

    pub fn scan(&self, version: Version) -> S::ScanIter<'_> {
        self.storage.scan(version)
    }

    pub fn scan_rev(&self, version: Version) -> S::ScanIterRev<'_> {
        self.storage.scan_rev(version)
    }

    pub fn scan_range(&self, range: KeyRange, version: Version) -> S::ScanRangeIter<'_> {
        self.storage.scan_range(range, version)
    }

    pub fn scan_range_rev(&self, range: KeyRange, version: Version) -> S::ScanRangeIterRev<'_> {
        self.storage.scan_range_rev(range, version)
    }
}
