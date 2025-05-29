// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::hash::BuildHasher;
use smallvec_wrapper::MediumVec;

use super::*;

use crate::mvcc::DefaultHasher;
use indexmap::IndexSet;

#[derive(Clone, Copy, Debug)]
enum Read {
    Single(u64),
    All,
}

/// Options for the [`HashCm`].
#[derive(Default, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct HashCmOptions<S = DefaultHasher> {
    /// The hasher used by the conflict manager.
    pub hasher: S,
    /// The initialized capacity of the conflict manager.
    pub capacity: Option<usize>,
}

impl<S> HashCmOptions<S> {
    /// Creates a new `HashCmOptions` with the given hasher.

    pub const fn new(hasher: S) -> Self {
        Self { hasher, capacity: None }
    }

    /// Creates a new `HashCmOptions` with the given hasher and capacity.

    pub const fn with_capacity(hasher: S, capacity: usize) -> Self {
        Self { hasher, capacity: Some(capacity) }
    }
}

/// A [`ConflictManager`] conflict manager implementation that based on the [`Hash`](Hash).
pub struct HashCm<K, S = DefaultHasher> {
    reads: MediumVec<Read>,
    conflict_keys: IndexSet<u64, S>,
    _k: core::marker::PhantomData<K>,
}

impl<K, S: Clone> Clone for HashCm<K, S> {
    fn clone(&self) -> Self {
        Self {
            reads: self.reads.clone(),
            conflict_keys: self.conflict_keys.clone(),
            _k: core::marker::PhantomData,
        }
    }
}

impl<K, S> ConflictManager for HashCm<K, S>
where
    S: BuildHasher,
    K: Hash + Eq,
{
    type Key = K;
    type Options = HashCmOptions<S>;

    fn new(options: Self::Options) -> Self {
        match options.capacity {
            Some(capacity) => Self {
                reads: MediumVec::with_capacity(capacity),
                conflict_keys: IndexSet::with_capacity_and_hasher(capacity, options.hasher),
                _k: core::marker::PhantomData,
            },
            None => Self {
                reads: MediumVec::new(),
                conflict_keys: IndexSet::with_hasher(options.hasher),
                _k: core::marker::PhantomData,
            },
        }
    }

    fn mark_read(&mut self, key: &K) {
        let fp = self.conflict_keys.hasher().hash_one(key);
        self.reads.push(Read::Single(fp));
    }

    fn mark_conflict(&mut self, key: &Self::Key) {
        let fp = self.conflict_keys.hasher().hash_one(key);
        self.conflict_keys.insert(fp);
    }

    fn has_conflict(&self, other: &Self) -> bool {
        if self.reads.is_empty() {
            return false;
        }

        // check if there is any direct conflict
        for ro in self.reads.iter() {
            match ro {
                Read::Single(ro) => {
                    if other.conflict_keys.contains(ro) {
                        return true;
                    }
                }
                Read::All => {
                    if !other.conflict_keys.is_empty() {
                        return true;
                    }
                }
            }
        }

        false
    }

    fn rollback(&mut self) {
        self.reads.clear();
        self.conflict_keys.clear();
    }
}

impl<K, S> CmIter for HashCm<K, S>
where
    S: BuildHasher,
    K: Hash + Eq,
{
    fn mark_iter(&mut self) {
        self.reads.push(Read::All);
    }
}

impl<K, S> CmEquivalent for HashCm<K, S>
where
    S: BuildHasher,
    K: Hash + Eq,
{
    fn mark_read_equivalent<Q>(&mut self, key: &Q)
    where
        Self::Key: core::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let fp = self.conflict_keys.hasher().hash_one(key);
        self.reads.push(Read::Single(fp));
    }

    fn mark_conflict_equivalent<Q>(&mut self, key: &Q)
    where
        Self::Key: core::borrow::Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        let fp = self.conflict_keys.hasher().hash_one(key);
        self.conflict_keys.insert(fp);
    }
}

#[cfg(test)]
mod test {
    use super::{ConflictManager, HashCm, HashCmOptions};
    use crate::mvcc::conflict::CmEquivalent;

    #[test]
    fn test_hash_cm() {
        let mut cm =
            HashCm::<u64>::new(HashCmOptions::new(std::collections::hash_map::RandomState::new()));
        cm.mark_read(&1);
        cm.mark_read(&2);
        cm.mark_conflict(&3);
        let mut cm2 = cm.clone();
        cm2.mark_conflict_equivalent(&2);
        assert!(cm.has_conflict(&cm2));
    }
}
