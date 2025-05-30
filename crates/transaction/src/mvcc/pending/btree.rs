// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use super::*;

use std::collections::{
    BTreeMap,
    btree_map::{IntoIter as BTreeMapIntoIter, Iter as BTreeMapIter, Range as BTreeMapRange},
};

/// A type alias for [`PendingWrites`] that based on the [`BTreeMap`].
pub type BTreePendingWrites = BTreeMap<Key, TransactionValue>;

impl PendingWrites for BTreeMap<Key, TransactionValue> {
    type Iter<'a>
        = BTreeMapIter<'a, Key, TransactionValue>
    where
        Self: 'a;

    type IntoIter = BTreeMapIntoIter<Key, TransactionValue>;

    fn new() -> Self {
        Self::default()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn len(&self) -> usize {
        self.len()
    }

    fn max_batch_size(&self) -> u64 {
        u64::MAX
    }

    fn max_batch_entries(&self) -> u64 {
        u64::MAX
    }

    fn estimate_size(&self, _entry: &Pending) -> u64 {
        size_of::<Key>() as u64 + size_of::<Value>() as u64
    }

    fn get(&self, key: &Key) -> Option<&TransactionValue> {
        self.get(key)
    }

    fn get_entry(&self, key: &Key) -> Option<(&Key, &TransactionValue)> {
        self.get_key_value(key)
    }

    fn contains_key(&self, key: &Key) -> bool {
        self.contains_key(key)
    }

    fn insert(&mut self, key: Key, value: TransactionValue) {
        self.insert(key, value);
    }

    fn remove_entry(&mut self, key: &Key) -> Option<(Key, TransactionValue)> {
        self.remove_entry(key)
    }

    fn iter(&self) -> Self::Iter<'_> {
        BTreeMap::iter(self)
    }

    fn into_iter(self) -> Self::IntoIter {
        core::iter::IntoIterator::into_iter(self)
    }

    fn rollback(&mut self) {
        self.clear();
    }
}

impl PendingWritesRange for BTreeMap<Key, TransactionValue> {
    type Range<'a>
        = BTreeMapRange<'a, Key, TransactionValue>
    where
        Self: 'a;

    fn range<R: RangeBounds<Key>>(&self, range: R) -> Self::Range<'_> {
        BTreeMap::range(self, range)
    }
}

impl PendingWritesComparableRange for BTreeMap<Key, TransactionValue> {
    fn range_comparable<R>(&self, range: R) -> Self::Range<'_>
    where
        R: RangeBounds<Key>,
    {
        BTreeMap::range(self, range)
    }
}

impl PendingWritesComparable for BTreeMap<Key, TransactionValue> {
    fn get_comparable(&self, key: &Key) -> Option<&TransactionValue> {
        BTreeMap::get(self, key)
    }

    fn get_entry_comparable(&self, key: &Key) -> Option<(&Key, &TransactionValue)> {
        BTreeMap::get_key_value(self, key)
    }

    fn contains_key_comparable(&self, key: &Key) -> bool {
        BTreeMap::contains_key(self, key)
    }

    fn remove_entry_comparable(&mut self, key: &Key) -> Option<(Key, TransactionValue)> {
        BTreeMap::remove_entry(self, key)
    }
}
