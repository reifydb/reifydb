// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::ops::Deref;
use std::sync::{Arc, RwLock};

pub use read::*;
use reifydb_core::clock::LocalClock;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{UnversionedStorage, VersionedStorage};
use reifydb_core::{EncodedKey, EncodedKeyRange, Version};
use reifydb_storage::memory::Memory;
pub use write::*;

pub(crate) mod read;
#[allow(clippy::module_inception)]
mod write;

use crate::mvcc::conflict::BTreeConflict;
use crate::mvcc::pending::BTreePendingWrites;
use crate::mvcc::transaction::{Committed, TransactionManager};

pub struct Serializable<VS: VersionedStorage, US: UnversionedStorage>(Arc<Inner<VS, US>>);

pub struct Inner<VS: VersionedStorage, US: UnversionedStorage> {
    pub(crate) tm: TransactionManager<BTreeConflict, LocalClock, BTreePendingWrites>,
    pub(crate) versioned: VS,
    pub(crate) unversioned: RwLock<US>,
    pub(crate) hooks: Hooks,
}

impl<VS: VersionedStorage, US: UnversionedStorage> Deref for Serializable<VS, US> {
    type Target = Inner<VS, US>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Clone for Serializable<VS, US> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Inner<VS, US> {
    fn new(name: &str, versioned: VS, unversioned: US, hooks: Hooks) -> Self {
        let tm = TransactionManager::new(name, LocalClock::new());
        Self { tm, versioned, unversioned: RwLock::new(unversioned), hooks }
    }

    fn version(&self) -> Version {
        self.tm.version()
    }
}

impl Serializable<Memory, Memory> {
    pub fn testing() -> Self {
        Self::new(Memory::default(), Memory::default(), Hooks::default())
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Serializable<VS, US> {
    pub fn new(versioned: VS, unversioned: US, hooks: Hooks) -> Self {
        Self(Arc::new(Inner::new(core::any::type_name::<Self>(), versioned, unversioned, hooks)))
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Serializable<VS, US> {
    pub fn version(&self) -> Version {
        self.0.version()
    }
    pub fn begin_rx(&self) -> TransactionRx<VS, US> {
        TransactionRx::new(self.clone(), None)
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> Serializable<VS, US> {
    pub fn begin_tx(&self) -> TransactionTx<VS, US> {
        TransactionTx::new(self.clone())
    }
}

pub enum Transaction<VS: VersionedStorage, US: UnversionedStorage> {
    Rx(TransactionRx<VS, US>),
    Tx(TransactionTx<VS, US>),
}

impl<VS: VersionedStorage, US: UnversionedStorage> Serializable<VS, US> {
    pub fn get(&self, key: &EncodedKey, version: Version) -> Result<Option<Committed>, reifydb_core::Error> {
        Ok(self.versioned.get(key, version)?.map(|sv| sv.into()))
    }

    pub fn contains_key(&self, key: &EncodedKey, version: Version) -> Result<bool, reifydb_core::Error> {
        self.versioned.contains(key, version)
    }

    pub fn scan(&self, version: Version) -> Result<VS::ScanIter<'_>, reifydb_core::Error> {
        self.versioned.scan(version)
    }

    pub fn scan_rev(&self, version: Version) -> Result<VS::ScanIterRev<'_>, reifydb_core::Error> {
        self.versioned.scan_rev(version)
    }

    pub fn scan_range(&self, range: EncodedKeyRange, version: Version) -> Result<VS::ScanRangeIter<'_>, reifydb_core::Error> {
        self.versioned.scan_range(range, version)
    }

    pub fn scan_range_rev(
        &self,
        range: EncodedKeyRange,
        version: Version,
    ) -> Result<VS::ScanRangeIterRev<'_>, reifydb_core::Error> {
        self.versioned.scan_range_rev(range, version)
    }
}
