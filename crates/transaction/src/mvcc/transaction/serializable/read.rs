// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::conflict::BTreeConflict;
use crate::mvcc::pending::BTreePendingWrites;
use crate::mvcc::transaction::read::TransactionManagerRx;
use crate::mvcc::transaction::serializable::Serializable;
use crate::mvcc::types::TransactionValue;
use reifydb_core::clock::LocalClock;
use reifydb_core::{EncodedKey, EncodedKeyRange, Version};
use reifydb_storage::VersionedStorage;

pub struct TransactionRx<VS: VersionedStorage> {
    pub(crate) engine: Serializable<VS>,
    pub(crate) tm: TransactionManagerRx<BTreeConflict, LocalClock, BTreePendingWrites>,
}

impl<VS: VersionedStorage> TransactionRx<VS> {
    pub fn new(engine: Serializable<VS>, version: Option<Version>) -> Self {
        let tm = engine.tm.read(version);
        Self { engine, tm }
    }
}

impl<VS: VersionedStorage> TransactionRx<VS> {
    pub fn version(&self) -> Version {
        self.tm.version()
    }

    pub fn get(&self, key: &EncodedKey) -> Option<TransactionValue> {
        let version = self.tm.version();
        self.engine.get(key, version).map(Into::into)
    }

    pub fn contains_key(&self, key: &EncodedKey) -> bool {
        let version = self.tm.version();
        self.engine.contains_key(key, version)
    }

    pub fn scan(&self) -> VS::ScanIter<'_> {
        let version = self.tm.version();
        self.engine.scan(version)
    }

    pub fn scan_rev(&self) -> VS::ScanIterRev<'_> {
        let version = self.tm.version();
        self.engine.scan_rev(version)
    }

    pub fn scan_range(&self, range: EncodedKeyRange) -> VS::ScanRangeIter<'_> {
        let version = self.tm.version();
        self.engine.scan_range(range, version)
    }

    pub fn scan_range_rev(&self, range: EncodedKeyRange) -> VS::ScanRangeIterRev<'_> {
        let version = self.tm.version();
        self.engine.scan_range_rev(range, version)
    }

    pub fn scan_prefix(&self, prefix: &EncodedKey) -> VS::ScanRangeIter<'_> {
        self.scan_range(EncodedKeyRange::prefix(prefix))
    }

    pub fn scan_prefix_rev(&self, prefix: &EncodedKey) -> VS::ScanRangeIterRev<'_> {
        self.scan_range_rev(EncodedKeyRange::prefix(prefix))
    }
}
