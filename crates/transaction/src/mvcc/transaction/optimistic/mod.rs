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
use std::sync::Arc;

use crate::mvcc::conflict::BTreeConflict;
use crate::mvcc::pending::BTreePendingWrites;
use crate::mvcc::transaction::TransactionManager;

use crate::mvcc::transaction::version::StdVersionProvider;
use crate::mvcc::types::Committed;
use crate::svl::SingleVersionLock;
pub use query::QueryTransaction;
use reifydb_core::hook::Hooks;
use reifydb_core::interface::{UnversionedTransaction, VersionedStorage};
use reifydb_core::{EncodedKey, EncodedKeyRange, Version};
use reifydb_storage::memory::Memory;
pub use command::CommandTransaction;

mod query;
mod command;

pub struct Optimistic<VS: VersionedStorage, UT: UnversionedTransaction>(Arc<Inner<VS, UT>>);

impl<VS: VersionedStorage, UT: UnversionedTransaction> Deref for Optimistic<VS, UT> {
    type Target = Inner<VS, UT>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Clone for Optimistic<VS, UT> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

pub struct Inner<VS: VersionedStorage, UT: UnversionedTransaction> {
    pub(crate) tm: TransactionManager<BTreeConflict, StdVersionProvider<UT>, BTreePendingWrites>,
    pub(crate) versioned: VS,
    pub(crate) hooks: Hooks,
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Inner<VS, UT> {
    fn new(name: &str, versioned: VS, unversioned: UT, hooks: Hooks) -> Self {
        let tm =
            TransactionManager::new(name, StdVersionProvider::new(unversioned).unwrap()).unwrap();
        Self { tm, versioned, hooks }
    }

    fn version(&self) -> crate::Result<Version> {
        self.tm.version()
    }
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Optimistic<VS, UT> {
    pub fn new(versioned: VS, unversioned: UT, hooks: Hooks) -> Self {
        Self(Arc::new(Inner::new(core::any::type_name::<Self>(), versioned, unversioned, hooks)))
    }
}

impl Optimistic<Memory, SingleVersionLock<Memory>> {
    pub fn testing() -> Self {
        let memory = Memory::new();
        let hooks = Hooks::new();
        Self::new(Memory::default(), SingleVersionLock::new(memory, hooks.clone()), hooks)
    }
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Optimistic<VS, UT> {
    pub fn version(&self) -> crate::Result<Version> {
        self.0.version()
    }
    pub fn begin_query(&self) -> crate::Result<QueryTransaction<VS, UT>> {
        QueryTransaction::new(self.clone(), None)
    }
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Optimistic<VS, UT> {
    pub fn begin_command(&self) -> crate::Result<CommandTransaction<VS, UT>> {
        CommandTransaction::new(self.clone())
    }
}

pub enum Transaction<VS: VersionedStorage, UT: UnversionedTransaction> {
    Rx(QueryTransaction<VS, UT>),
    Tx(CommandTransaction<VS, UT>),
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> Optimistic<VS, UT> {
    pub fn get(
        &self,
        key: &EncodedKey,
        version: Version,
    ) -> Result<Option<Committed>, reifydb_core::Error> {
        Ok(self.versioned.get(key, version)?.map(|sv| sv.into()))
    }

    pub fn contains_key(
        &self,
        key: &EncodedKey,
        version: Version,
    ) -> Result<bool, reifydb_core::Error> {
        self.versioned.contains(key, version)
    }

    pub fn scan(&self, version: Version) -> Result<VS::ScanIter<'_>, reifydb_core::Error> {
        self.versioned.scan(version)
    }

    pub fn scan_rev(&self, version: Version) -> Result<VS::ScanIterRev<'_>, reifydb_core::Error> {
        self.versioned.scan_rev(version)
    }

    pub fn range(
        &self,
        range: EncodedKeyRange,
        version: Version,
    ) -> Result<VS::RangeIter<'_>, reifydb_core::Error> {
        self.versioned.range(range, version)
    }

    pub fn range_rev(
        &self,
        range: EncodedKeyRange,
        version: Version,
    ) -> Result<VS::RangeIterRev<'_>, reifydb_core::Error> {
        self.versioned.range_rev(range, version)
    }
}
