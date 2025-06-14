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

pub use read::*;
use reifydb_core::clock::LocalClock;
use reifydb_core::hook::Hooks;
use reifydb_core::{EncodedKey, EncodedKeyRange, Version};
use reifydb_storage::VersionedStorage;
pub use write::*;

pub(crate) mod read;
#[allow(clippy::module_inception)]
mod write;

use crate::mvcc::conflict::BTreeConflict;
use crate::mvcc::pending::BTreePendingWrites;
use crate::mvcc::transaction::{Committed, TransactionManager};

pub struct Serializable<VS: VersionedStorage>(Arc<Inner<VS>>);

pub struct Inner<VS: VersionedStorage> {
    pub(crate) tm: TransactionManager<BTreeConflict, LocalClock, BTreePendingWrites>,
    pub(crate) storage: VS,
    pub(crate) hooks: Hooks,
}

impl<VS: VersionedStorage> Deref for Serializable<VS> {
    type Target = Inner<VS>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<VS: VersionedStorage> Clone for Serializable<VS> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<VS: VersionedStorage> Inner<VS> {
    fn new(name: &str, storage: VS) -> Self {
        let tm = TransactionManager::new(name, LocalClock::new());
        let hooks = storage.hooks();
        Self { tm, storage, hooks }
    }

    fn version(&self) -> Version {
        self.tm.version()
    }
}

impl<VS: VersionedStorage> Serializable<VS> {
    pub fn new(storage: VS) -> Self {
        Self(Arc::new(Inner::new(core::any::type_name::<Self>(), storage)))
    }
}

impl<VS: VersionedStorage> Serializable<VS> {
    pub fn version(&self) -> Version {
        self.0.version()
    }
    pub fn begin_read_only(&self) -> TransactionRx<VS> {
        TransactionRx::new(self.clone(), None)
    }
}

impl<VS: VersionedStorage> Serializable<VS> {
    pub fn begin(&self) -> TransactionTx<VS> {
        TransactionTx::new(self.clone())
    }
}

pub enum Transaction<VS: VersionedStorage> {
    Rx(TransactionRx<VS>),
    Tx(TransactionTx<VS>),
}

impl<VS: VersionedStorage> Serializable<VS> {
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
