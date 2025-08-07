// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::conflict::BTreeConflict;
use crate::mvcc::pending::BTreePendingWrites;
use crate::mvcc::transaction::query::TransactionManagerQuery;
use crate::mvcc::transaction::serializable::Serializable;
use crate::mvcc::transaction::version::StdVersionProvider;
use crate::mvcc::types::TransactionValue;
use reifydb_core::interface::{UnversionedTransaction, VersionedStorage};
use reifydb_core::{EncodedKey, EncodedKeyRange, Version};

pub struct QueryTransaction<VS: VersionedStorage, UT: UnversionedTransaction> {
    pub(crate) engine: Serializable<VS, UT>,
    pub(crate) tm: TransactionManagerQuery<BTreeConflict, StdVersionProvider<UT>, BTreePendingWrites>,
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> QueryTransaction<VS, UT> {
    pub fn new(engine: Serializable<VS, UT>, version: Option<Version>) -> crate::Result<Self> {
        let tm = engine.tm.query(version)?;
        Ok(Self { engine, tm })
    }
}

impl<VS: VersionedStorage, UT: UnversionedTransaction> QueryTransaction<VS, UT> {
    pub fn version(&self) -> Version {
        self.tm.version()
    }

    pub fn get(&self, key: &EncodedKey) -> crate::Result<Option<TransactionValue>> {
        let version = self.tm.version();
        Ok(self.engine.get(key, version)?.map(Into::into))
    }

    pub fn contains_key(&self, key: &EncodedKey) -> crate::Result<bool> {
        let version = self.tm.version();
        Ok(self.engine.contains_key(key, version)?)
    }

    pub fn scan(&self) -> crate::Result<VS::ScanIter<'_>> {
        let version = self.tm.version();
        Ok(self.engine.scan(version)?)
    }

    pub fn scan_rev(&self) -> crate::Result<VS::ScanIterRev<'_>> {
        let version = self.tm.version();
        Ok(self.engine.scan_rev(version)?)
    }

    pub fn range(&self, range: EncodedKeyRange) -> crate::Result<VS::RangeIter<'_>> {
        let version = self.tm.version();
        Ok(self.engine.range(range, version)?)
    }

    pub fn range_rev(
        &self,
        range: EncodedKeyRange,
    ) -> crate::Result<VS::RangeIterRev<'_>> {
        let version = self.tm.version();
        Ok(self.engine.range_rev(range, version)?)
    }

    pub fn prefix(&self, prefix: &EncodedKey) -> crate::Result<VS::RangeIter<'_>> {
        self.range(EncodedKeyRange::prefix(prefix))
    }

    pub fn prefix_rev(&self, prefix: &EncodedKey) -> crate::Result<VS::RangeIterRev<'_>> {
        self.range_rev(EncodedKeyRange::prefix(prefix))
    }
}
