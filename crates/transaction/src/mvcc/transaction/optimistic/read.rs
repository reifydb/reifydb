// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::pending::BTreePwm;
use crate::mvcc::skipdbcore::Database;
use crate::mvcc::skipdbcore::types::Ref;
use crate::mvcc::transaction::read::Rtm;
use crate::mvcc::transaction::scan::iter::Iter;
use crate::mvcc::transaction::scan::range::Range;
use crate::mvcc::transaction::scan::rev_iter::RevIter;
use crate::mvcc::transaction::scan::rev_range::RevRange;
use crate::mvcc::transaction::*;
use std::borrow::Borrow;
use std::ops::RangeBounds;

pub struct ReadTransaction<K, V, I, C> {
    pub(crate) db: I,
    pub(crate) rtm: Rtm<K, V, C, BTreePwm<K, V>>,
}

impl<K, V, I, C> ReadTransaction<K, V, I, C> {
    pub(in crate::mvcc::transaction) fn new(db: I, rtm: Rtm<K, V, C, BTreePwm<K, V>>) -> Self {
        Self { db, rtm }
    }
}

impl<K, V, I, C> ReadTransaction<K, V, I, C>
where
    K: Ord,
    I: Database<K, V>,
{
    /// Returns the version of the transaction.

    pub fn version(&self) -> u64 {
        self.rtm.version()
    }

    /// Get a value from the database.

    pub fn get<Q>(&self, key: &Q) -> Option<Ref<'_, K, V>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let version = self.rtm.version();
        self.db.as_inner().get(key, version).map(Into::into)
    }

    /// Returns true if the given key exists in the database.

    pub fn contains_key<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        let version = self.rtm.version();
        self.db.as_inner().contains_key(key, version)
    }

    /// Returns an iterator over the entries of the database.

    pub fn iter(&self) -> Iter<'_, K, V> {
        let version = self.rtm.version();
        self.db.as_inner().iter(version)
    }

    /// Returns a reverse iterator over the entries of the database.

    pub fn iter_rev(&self) -> RevIter<'_, K, V> {
        let version = self.rtm.version();
        self.db.as_inner().iter_rev(version)
    }

    /// Returns an iterator over the subset of entries of the database.

    pub fn range<Q, R>(&self, range: R) -> Range<'_, Q, R, K, V>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        let version = self.rtm.version();
        self.db.as_inner().range(range, version)
    }

    /// Returns an iterator over the subset of entries of the database in reverse order.

    pub fn range_rev<Q, R>(&self, range: R) -> RevRange<'_, Q, R, K, V>
    where
        K: Borrow<Q>,
        R: RangeBounds<Q>,
        Q: Ord + ?Sized,
    {
        let version = self.rtm.version();
        self.db.as_inner().range_rev(range, version)
    }
}
