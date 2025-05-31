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
use reifydb_persistence::Key;
use std::ops::RangeBounds;
use reifydb_storage::memory::{Iter, Range, IterRev, RangeRev};

pub struct TransactionRx {
    pub(crate) engine: Optimistic,
    pub(crate) rtm: TransactionManagerRx<BTreeConflict, BTreePendingWrites>,
}

impl TransactionRx {
    pub fn new(engine: Optimistic) -> Self {
        let rtm = engine.inner.tm.read();
        Self { engine, rtm }
    }
}

impl TransactionRx {
    /// Returns the version of the transaction.
    pub fn version(&self) -> u64 {
        self.rtm.version()
    }

    /// Get a value from the database.
    pub fn get(&self, key: &Key) -> Option<TransactionValue> {
        let version = self.rtm.version();
        self.engine.get(key, version).map(Into::into)
    }

    /// Returns true if the given key exists in the database.
    pub fn contains_key(&self, key: &Key) -> bool {
        let version = self.rtm.version();
        self.engine.contains_key(key, version)
    }

    /// Returns an iterator over the entries of the database.
    pub fn iter(&self) -> Iter<'_> {
        let version = self.rtm.version();
        self.engine.scan(version)
    }

    /// Returns a reverse iterator over the entries of the database.
    pub fn iter_rev(&self) -> IterRev<'_> {
        let version = self.rtm.version();
        self.engine.scan_rev(version)
    }

    /// Returns an iterator over the subset of entries of the database.
    pub fn range<R>(&self, range: R) -> Range<'_, R>
    where
        R: RangeBounds<Key>,
    {
        let version = self.rtm.version();
        self.engine.scan_range(range, version)
    }

    /// Returns an iterator over the subset of entries of the database in reverse order.
    pub fn range_rev<R>(&self, range: R) -> RangeRev<'_, R>
    where
        R: RangeBounds<Key>,
    {
        let version = self.rtm.version();
        self.engine.scan_range_rev(range, version)
    }
}
