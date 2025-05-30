// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::pending::BTreePendingWrites;
use crate::mvcc::store::types::Ref;
use crate::mvcc::transaction::read::TransactionManagerRx;
use crate::mvcc::transaction::scan::iter::Iter;
use crate::mvcc::transaction::scan::range::Range;
use crate::mvcc::transaction::scan::rev_iter::RevIter;
use crate::mvcc::transaction::scan::rev_range::RevRange;
use crate::mvcc::transaction::serializable::Serializable;
use reifydb_persistence::Key;
use std::ops::RangeBounds;

pub struct ReadTransaction<C> {
    pub(crate) db: Serializable,
    pub(crate) rx: TransactionManagerRx<C, BTreePendingWrites>,
}

impl<C> ReadTransaction<C> {
    pub(in crate::mvcc::transaction) fn new(
        db: Serializable,
        rtm: TransactionManagerRx<C, BTreePendingWrites>,
    ) -> Self {
        Self { db, rx: rtm }
    }
}

impl<C> ReadTransaction<C> {
    /// Returns the version of the transaction.
    pub fn version(&self) -> u64 {
        self.rx.version()
    }

    /// Get a value from the database.
    pub fn get(&self, key: &Key) -> Option<Ref<'_>> {
        let version = self.rx.version();
        // self.db.as_inner().get(key, version).map(Into::into)
        unimplemented!()
    }

    /// Returns true if the given key exists in the database.
    pub fn contains_key(&self, key: &Key) -> bool {
        let version = self.rx.version();
        // self.db.as_inner().contains_key(key, version)
        unimplemented!()
    }

    /// Returns an iterator over the entries of the database.
    pub fn iter(&self) -> Iter<'_> {
        let version = self.rx.version();
        // self.db.as_inner().iter(version)
        unimplemented!()
    }

    /// Returns a reverse iterator over the entries of the database.
    pub fn iter_rev(&self) -> RevIter<'_> {
        let version = self.rx.version();
        // self.db.as_inner().iter_rev(version)
        unimplemented!()
    }

    /// Returns an iterator over the subset of entries of the database.
    pub fn range<R>(&self, range: R) -> Range<'_, R>
    where
        R: RangeBounds<Key>,
    {
        let version = self.rx.version();
        // self.db.as_inner().range(range, version)
        unimplemented!()
    }

    /// Returns an iterator over the subset of entries of the database in reverse order.
    pub fn range_rev<R>(&self, range: R) -> RevRange<'_, R>
    where
        R: RangeBounds<Key>,
    {
        let version = self.rx.version();
        // self.db.as_inner().range_rev(range, version)
        unimplemented!()
    }
}
