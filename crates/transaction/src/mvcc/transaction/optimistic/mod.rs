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
use reifydb_core::clock::LocalClock;
use reifydb_core::hook::Hooks;
use reifydb_core::{EncodedKey, EncodedKeyRange, Version};
use reifydb_storage::VersionedStorage;
pub use write::TransactionTx;

mod read;
mod write;

pub struct Optimistic<VS: VersionedStorage>(Arc<Inner<VS>>);

impl<VS: VersionedStorage> Deref for Optimistic<VS> {
    type Target = Inner<VS>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<VS: VersionedStorage> Clone for Optimistic<VS> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub struct Inner<VS: VersionedStorage> {
    pub(crate) tm: TransactionManager<BTreeConflict, LocalClock, BTreePendingWrites>,
    pub(crate) storage: VS,
    pub(crate) hooks: Hooks,
}

impl<VS: VersionedStorage> Inner<VS> {
    fn new(name: &str, storage: VS, hooks: Hooks) -> Self {
        let tm = TransactionManager::new(name, LocalClock::new());
        Self { tm, storage, hooks }
    }

    fn version(&self) -> Version {
        self.tm.version()
    }
}

impl<VS: VersionedStorage> Optimistic<VS> {
    pub fn new(storage: VS) -> Self {
        let hooks = storage.hooks();
        Self(Arc::new(Inner::new(core::any::type_name::<Self>(), storage, hooks)))
    }
}

impl<VS: VersionedStorage> Optimistic<VS> {
    pub fn version(&self) -> Version {
        self.0.version()
    }
    pub fn begin_read_only(&self) -> TransactionRx<VS> {
        TransactionRx::new(self.clone(), None)
    }
}

impl<VS: VersionedStorage> Optimistic<VS> {
    pub fn begin(&self) -> TransactionTx<VS> {
        TransactionTx::new(self.clone())
    }
}

pub enum Transaction<VS: VersionedStorage> {
    Rx(TransactionRx<VS>),
    Tx(TransactionTx<VS>),
}

impl<VS: VersionedStorage> Optimistic<VS> {
    pub fn get(&self, key: &EncodedKey, version: Version) -> Option<Committed> {
        self.storage.get(key, version).map(|sv| sv.into())
    }

    pub fn contains_key(&self, key: &EncodedKey, version: Version) -> bool {
        self.storage.contains(key, version)
    }

    pub fn scan(&self, version: Version) -> VS::ScanIter<'_> {
        self.storage.scan(version)
    }

    pub fn scan_rev(&self, version: Version) -> VS::ScanIterRev<'_> {
        self.storage.scan_rev(version)
    }

    pub fn scan_range(&self, range: EncodedKeyRange, version: Version) -> VS::ScanRangeIter<'_> {
        self.storage.scan_range(range, version)
    }

    pub fn scan_range_rev(
        &self,
        range: EncodedKeyRange,
        version: Version,
    ) -> VS::ScanRangeIterRev<'_> {
        self.storage.scan_range_rev(range, version)
    }
}
