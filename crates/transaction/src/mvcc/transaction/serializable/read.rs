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
use reifydb_storage::{Key, KeyRange};
use reifydb_storage::{LocalClock, Storage, Version};

pub struct TransactionRx<S: Storage> {
    pub(crate) engine: Serializable<S>,
    pub(crate) tm: TransactionManagerRx<BTreeConflict, LocalClock, BTreePendingWrites>,
}

impl<S: Storage> TransactionRx<S> {
    pub fn new(engine: Serializable<S>, version: Option<Version>) -> Self {
        let tm = engine.tm.read(version);
        Self { engine, tm }
    }
}

impl<S: Storage> TransactionRx<S> {
    pub fn version(&self) -> Version {
        self.tm.version()
    }

    pub fn get(&self, key: &Key) -> Option<TransactionValue> {
        let version = self.tm.version();
        self.engine.get(key, version).map(Into::into)
    }

    pub fn contains_key(&self, key: &Key) -> bool {
        let version = self.tm.version();
        self.engine.contains_key(key, version)
    }

    pub fn scan(&self) -> S::ScanIter<'_> {
        let version = self.tm.version();
        self.engine.scan(version)
    }

    pub fn scan_rev(&self) -> S::ScanIterRev<'_> {
        let version = self.tm.version();
        self.engine.scan_rev(version)
    }

    pub fn scan_range(&self, range: KeyRange) -> S::ScanRangeIter<'_> {
        let version = self.tm.version();
        self.engine.scan_range(range, version)
    }

    pub fn scan_range_rev(&self, range: KeyRange) -> S::ScanRangeIterRev<'_> {
        let version = self.tm.version();
        self.engine.scan_range_rev(range, version)
    }

    pub fn scan_prefix(&self, prefix: &Key) -> S::ScanRangeIter<'_> {
        self.scan_range(KeyRange::prefix(prefix))
    }

    pub fn scan_prefix_rev(&self, prefix: &Key) -> S::ScanRangeIterRev<'_> {
        self.scan_range_rev(KeyRange::prefix(prefix))
    }
}
