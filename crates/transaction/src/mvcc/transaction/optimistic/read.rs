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
use crate::mvcc::transaction::optimistic::Optimistic;
use crate::mvcc::transaction::read::TransactionManagerRx;
use crate::mvcc::types::TransactionValue;
use reifydb_storage::{Key, KeyRange};
use reifydb_storage::{LocalClock, Storage, Version};

pub struct TransactionRx<S: Storage> {
    pub(crate) engine: Optimistic<S>,
    pub(crate) rtm: TransactionManagerRx<BTreeConflict, LocalClock, BTreePendingWrites>,
}

impl<S: Storage> TransactionRx<S> {
    pub fn new(engine: Optimistic<S>) -> Self {
        let rtm = engine.inner.tm.read();
        Self { engine, rtm }
    }
}

impl<S: Storage> TransactionRx<S> {
    pub fn version(&self) -> Version {
        self.rtm.version()
    }

    pub fn get(&self, key: &Key) -> Option<TransactionValue> {
        let version = self.rtm.version();
        self.engine.get(key, version).map(Into::into)
    }

    pub fn contains_key(&self, key: &Key) -> bool {
        let version = self.rtm.version();
        self.engine.contains_key(key, version)
    }

    pub fn scan(&self) -> S::ScanIter<'_> {
        let version = self.rtm.version();
        self.engine.scan(version)
    }

    pub fn scan_rev(&self) -> S::ScanIterRev<'_> {
        let version = self.rtm.version();
        self.engine.scan_rev(version)
    }

    pub fn scan_range(&self, range: KeyRange) -> S::ScanRangeIter<'_> {
        let version = self.rtm.version();
        self.engine.scan_range(range, version)
    }

    pub fn scan_range_rev(&self, range: KeyRange) -> S::ScanRangeIterRev<'_> {
        let version = self.rtm.version();
        self.engine.scan_range_rev(range, version)
    }
}
