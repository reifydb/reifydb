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
use crate::mvcc::DefaultHasher;
use core::hash::BuildHasher;
use indexmap::IndexMap;

/// A type alias for [`PendingWrites`] that based on the [`IndexMap`].
pub type IndexMapPwm<K, V, S = DefaultHasher> = IndexMap<K, EntryValue<V>, S>;

impl<K, V, S> PendingWrites for IndexMap<K, EntryValue<V>, S>
where
    K: Eq + Hash,
    S: BuildHasher + Default,
{
    type Key = K;
    type Value = V;
    type Iter<'a>
        = indexmap::map::Iter<'a, K, EntryValue<V>>
    where
        Self: 'a;
    type IntoIter = indexmap::map::IntoIter<K, EntryValue<V>>;

    type Options = Option<S>;

    fn new(options: Self::Options) -> Self {
        match options {
            Some(hasher) => Self::with_hasher(hasher),
            None => Self::default(),
        }
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

    fn get(&self, key: &K) -> Option<&EntryValue<V>> {
        self.get(key)
    }

    fn get_entry(&self, key: &Self::Key) -> Option<(&Self::Key, &EntryValue<Self::Value>)> {
        self.get_full(key).map(|(_, k, v)| (k, v))
    }

    fn contains_key(&self, key: &K) -> bool {
        self.contains_key(key)
    }

    fn insert(&mut self, key: K, value: EntryValue<V>) {
        self.insert(key, value);
    }

    fn remove_entry(&mut self, key: &K) -> Option<(K, EntryValue<V>)> {
        self.shift_remove_entry(key)
    }

    fn iter(&self) -> Self::Iter<'_> {
        IndexMap::iter(self)
    }

    fn into_iter(self) -> Self::IntoIter {
        core::iter::IntoIterator::into_iter(self)
    }

    fn rollback(&mut self) {
        self.clear();
    }
}

impl<K, V, S> PwmEquivalent for IndexMap<K, EntryValue<V>, S>
where
    K: Eq + Hash,
    S: BuildHasher + Default,
{
    fn get_equivalent<Q>(&self, key: &Q) -> Option<&EntryValue<V>>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get(key)
    }

    fn get_entry_equivalent<Q>(&self, key: &Q) -> Option<(&Self::Key, &EntryValue<Self::Value>)>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.get_full(key).map(|(_, k, v)| (k, v))
    }

    fn contains_key_equivalent<Q>(&self, key: &Q) -> bool
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.contains_key(key)
    }

    fn remove_entry_equivalent<Q>(&mut self, key: &Q) -> Option<(K, EntryValue<V>)>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.shift_remove_entry(key)
    }
}
