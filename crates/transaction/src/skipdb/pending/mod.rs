// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::skipdb::txn::{Entry, EntryValue};
use std::borrow::Borrow;
use std::hash::Hash;
use std::ops::RangeBounds;

pub use btree::BTreePwm;
pub use hash::IndexMapPwm;

mod btree;
mod hash;

/// A pending writes manager that can be used to store pending writes in a transaction.
///
/// By default, there are two implementations of this trait:
/// - [`IndexMap`]: A hash map with consistent ordering and fast lookups.
/// - [`BTreeMap`]: A balanced binary tree with ordered keys and fast lookups.
///
/// But, users can create their own implementations by implementing this trait.
/// e.g. if you want to implement a recovery transaction manager, you can use a persistent
/// storage to store the pending writes.
pub trait Pwm: Sized {
    /// The error type returned by the conflict manager.
    type Error: crate::skipdb::error::Error;

    /// The key type.
    type Key;
    /// The value type.
    type Value;

    /// The iterator type.
    type Iter<'a>: Iterator<Item = (&'a Self::Key, &'a EntryValue<Self::Value>)>
    where
        Self: 'a;

    /// The IntoIterator type.
    type IntoIter: Iterator<Item = (Self::Key, EntryValue<Self::Value>)>;

    /// The options type used to create the pending manager.
    type Options;

    /// Create a new pending manager with the given options.
    fn new(options: Self::Options) -> Result<Self, Self::Error>;

    /// Returns true if the buffer is empty.
    fn is_empty(&self) -> bool;

    /// Returns the number of elements in the buffer.
    fn len(&self) -> usize;

    /// Validate if the entry is valid for this database.
    ///
    /// e.g.
    /// - If the entry is expired
    /// - If the key or the value is too large
    /// - If the key or the value is empty
    /// - If the key or the value contains invalid characters
    /// - and etc.
    fn validate_entry(&self, entry: &Entry<Self::Key, Self::Value>) -> Result<(), Self::Error>;

    /// Returns the maximum batch size in bytes
    fn max_batch_size(&self) -> u64;

    /// Returns the maximum entries in batch
    fn max_batch_entries(&self) -> u64;

    /// Returns the estimated size of the entry in bytes when persisted in the database.
    fn estimate_size(&self, entry: &Entry<Self::Key, Self::Value>) -> u64;

    /// Returns a reference to the value corresponding to the key.
    fn get(&self, key: &Self::Key) -> Result<Option<&EntryValue<Self::Value>>, Self::Error>;

    /// Returns a reference to the key-value pair corresponding to the key.
    fn get_entry(
        &self,
        key: &Self::Key,
    ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>;

    /// Returns true if the pending manager contains the key.
    fn contains_key(&self, key: &Self::Key) -> Result<bool, Self::Error>;

    /// Inserts a key-value pair into the er.
    fn insert(&mut self, key: Self::Key, value: EntryValue<Self::Value>)
    -> Result<(), Self::Error>;

    /// Removes a key from the pending writes, returning the key-value pair if the key was previously in the pending writes.
    fn remove_entry(
        &mut self,
        key: &Self::Key,
    ) -> Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>;

    /// Returns an iterator over the pending writes.
    fn iter(&self) -> Self::Iter<'_>;

    /// Returns an iterator that consumes the pending writes.
    fn into_iter(self) -> Self::IntoIter;

    /// Rollback the pending writes.
    fn rollback(&mut self) -> Result<(), Self::Error>;
}

/// An trait that can be used to get a range over the pending writes.
pub trait PwmRange: Pwm {
    /// The iterator type.
    type Range<'a>: IntoIterator<Item = (&'a Self::Key, &'a EntryValue<Self::Value>)>
    where
        Self: 'a;

    /// Returns an iterator over the pending writes.
    fn range<R: RangeBounds<Self::Key>>(&self, range: R) -> Self::Range<'_>;
}

/// An trait that can be used to get a range over the pending writes.
pub trait PwmComparableRange: PwmRange + PwmComparable {
    /// Returns an iterator over the pending writes.
    fn range_comparable<T, R>(&self, range: R) -> Self::Range<'_>
    where
        T: ?Sized + Ord,
        Self::Key: Borrow<T> + Ord,
        R: RangeBounds<T>;
}

/// An trait that can be used to get a range over the pending writes.
pub trait PwmEquivalentRange: PwmRange + PwmEquivalent {
    /// Returns an iterator over the pending writes.
    fn range_equivalent<T, R>(&self, range: R) -> Self::Range<'_>
    where
        T: ?Sized + Eq + Hash,
        Self::Key: Borrow<T> + Eq + Hash,
        R: RangeBounds<T>;
}

/// An optimized version of the [`Pwm`] trait that if your pending writes manager is depend on hash.
pub trait PwmEquivalent: Pwm {
    /// Optimized version of [`Pwm::get`] that accepts borrowed keys.
    fn get_equivalent<Q>(&self, key: &Q) -> Result<Option<&EntryValue<Self::Value>>, Self::Error>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Optimized version of [`Pwm::get_entry`] that accepts borrowed keys.
    fn get_entry_equivalent<Q>(
        &self,
        key: &Q,
    ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Optimized version of [`Pwm::contains_key`] that accepts borrowed keys.
    fn contains_key_equivalent<Q>(&self, key: &Q) -> Result<bool, Self::Error>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Optimized version of [`Pwm::remove_entry`] that accepts borrowed keys.
    fn remove_entry_equivalent<Q>(
        &mut self,
        key: &Q,
    ) -> Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
}

/// An optimized version of the [`Pwm`] trait that if your pending writes manager is depend on the order.
pub trait PwmComparable: Pwm {
    /// Optimized version of [`Pwm::get`] that accepts borrowed keys.
    fn get_comparable<Q>(&self, key: &Q) -> Result<Option<&EntryValue<Self::Value>>, Self::Error>
    where
        Self::Key: Borrow<Q>,
        Q: Ord + ?Sized;

    /// Optimized version of [`Pwm::get`] that accepts borrowed keys.
    fn get_entry_comparable<Q>(
        &self,
        key: &Q,
    ) -> Result<Option<(&Self::Key, &EntryValue<Self::Value>)>, Self::Error>
    where
        Self::Key: Borrow<Q>,
        Q: Ord + ?Sized;

    /// Optimized version of [`Pwm::contains_key`] that accepts borrowed keys.
    fn contains_key_comparable<Q>(&self, key: &Q) -> Result<bool, Self::Error>
    where
        Self::Key: Borrow<Q>,
        Q: Ord + ?Sized;

    /// Optimized version of [`Pwm::remove_entry`] that accepts borrowed keys.
    fn remove_entry_comparable<Q>(
        &mut self,
        key: &Q,
    ) -> Result<Option<(Self::Key, EntryValue<Self::Value>)>, Self::Error>
    where
        Self::Key: Borrow<Q>,
        Q: Ord + ?Sized;
}
