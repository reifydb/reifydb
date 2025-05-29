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
pub type BTreePwm<K, V> = BTreeMap<K, EntryValue<V>>;

impl<K, V> PendingWrites for BTreeMap<K, EntryValue<V>>
where
    K: Ord,
{
    type Key = K;
    type Value = V;

    type Iter<'a>
        = BTreeMapIter<'a, K, EntryValue<V>>
    where
        Self: 'a;

    type IntoIter = BTreeMapIntoIter<K, EntryValue<V>>;

    type Options = ();

    fn new(_: Self::Options) -> Self {
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

    fn estimate_size(&self, _entry: &Entry<Self::Key, Self::Value>) -> u64 {
        size_of::<Self::Key>() as u64 + size_of::<Self::Value>() as u64
    }

    fn get(&self, key: &K) -> Option<&EntryValue<Self::Value>> {
        self.get(key)
    }

    fn get_entry(&self, key: &Self::Key) -> Option<(&Self::Key, &EntryValue<Self::Value>)> {
        self.get_key_value(key)
    }

    fn contains_key(&self, key: &K) -> bool {
        self.contains_key(key)
    }

    fn insert(&mut self, key: K, value: EntryValue<Self::Value>) {
        self.insert(key, value);
    }

    fn remove_entry(&mut self, key: &K) -> Option<(K, EntryValue<Self::Value>)> {
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

impl<K, V> PwmRange for BTreeMap<K, EntryValue<V>>
where
    K: Ord,
{
    type Range<'a>
        = BTreeMapRange<'a, K, EntryValue<V>>
    where
        Self: 'a;

    fn range<R: RangeBounds<Self::Key>>(&self, range: R) -> Self::Range<'_> {
        BTreeMap::range(self, range)
    }
}

impl<K, V> PwmComparableRange for BTreeMap<K, EntryValue<V>>
where
    K: Ord,
{
    fn range_comparable<T, R>(&self, range: R) -> Self::Range<'_>
    where
        T: ?Sized + Ord,
        Self::Key: Borrow<T> + Ord,
        R: RangeBounds<T>,
    {
        BTreeMap::range(self, range)
    }
}

impl<K, V> PwmComparable for BTreeMap<K, EntryValue<V>>
where
    K: Ord,
{
    fn get_comparable<Q>(&self, key: &Q) -> Option<&EntryValue<Self::Value>>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        BTreeMap::get(self, key)
    }

    fn get_entry_comparable<Q>(&self, key: &Q) -> Option<(&Self::Key, &EntryValue<Self::Value>)>
    where
        Self::Key: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        BTreeMap::get_key_value(self, key)
    }

    fn contains_key_comparable<Q>(&self, key: &Q) -> bool
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        BTreeMap::contains_key(self, key)
    }

    fn remove_entry_comparable<Q>(&mut self, key: &Q) -> Option<(K, EntryValue<V>)>
    where
        K: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        BTreeMap::remove_entry(self, key)
    }
}
