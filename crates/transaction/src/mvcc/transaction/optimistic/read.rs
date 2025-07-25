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
use crate::mvcc::transaction::optimistic::Optimistic;
use crate::mvcc::transaction::read::TransactionManagerRx;
use crate::mvcc::types::TransactionValue;
use reifydb_core::clock::LocalClock;
use reifydb_core::{EncodedKey, EncodedKeyRange, Version};
use reifydb_core::interface::{UnversionedStorage, VersionedStorage};

pub struct TransactionRx<VS: VersionedStorage, US: UnversionedStorage> {
    pub(crate) engine: Optimistic<VS, US>,
    pub(crate) tm: TransactionManagerRx<BTreeConflict, LocalClock, BTreePendingWrites>,
}

impl<VS: VersionedStorage, US: UnversionedStorage> TransactionRx<VS, US> {
    pub fn new(engine: Optimistic<VS, US>, version: Option<Version>) -> Self {
        let tm = engine.tm.read(version);
        Self { engine, tm }
    }
}

impl<VS: VersionedStorage, US: UnversionedStorage> TransactionRx<VS, US> {
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

    pub fn scan_range(&self, range: EncodedKeyRange) -> crate::Result<VS::ScanRangeIter<'_>> {
        let version = self.tm.version();
        Ok(self.engine.scan_range(range, version)?)
    }

    pub fn scan_range_rev(&self, range: EncodedKeyRange) -> crate::Result<VS::ScanRangeIterRev<'_>> {
        let version = self.tm.version();
        Ok(self.engine.scan_range_rev(range, version)?)
    }

    pub fn scan_prefix(&self, prefix: &EncodedKey) -> crate::Result<VS::ScanRangeIter<'_>> {
        self.scan_range(EncodedKeyRange::prefix(prefix))
    }

    pub fn scan_prefix_rev(&self, prefix: &EncodedKey) -> crate::Result<VS::ScanRangeIterRev<'_>> {
        self.scan_range_rev(EncodedKeyRange::prefix(prefix))
    }
}
