// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::types::Pending;
use std::ops::RangeBounds;

pub use btree::BTreePendingWrites;
use reifydb_core::EncodedKey;

mod btree;

/// A pending writes manager that can be used to store pending writes in a transaction.
pub trait PendingWrites: Default + Sized {
    /// The iterator type.
    type Iter<'a>: Iterator<Item = (&'a EncodedKey, &'a Pending)>
    where
        Self: 'a;

    /// The IntoIterator type.
    type IntoIter: Iterator<Item = (EncodedKey, Pending)>;

    /// Create a new pending writes manager.
    fn new() -> Self;

    /// Returns true if the buffer is empty.
    fn is_empty(&self) -> bool;

    /// Returns the number of elements in the buffer.
    fn len(&self) -> usize;

    /// Returns the maximum batch size in bytes
    fn max_batch_size(&self) -> u64;

    /// Returns the maximum entries in batch
    fn max_batch_entries(&self) -> u64;

    /// Returns the estimated size of the entry in bytes when persisted in the database.
    fn estimate_size(&self, entry: &Pending) -> u64;

    /// Returns a reference to the value corresponding to the key.
    fn get(&self, key: &EncodedKey) -> Option<&Pending>;

    /// Returns a reference to the key-value pair corresponding to the key.
    fn get_entry(&self, key: &EncodedKey) -> Option<(&EncodedKey, &Pending)>;

    /// Returns true if the pending manager contains the key.
    fn contains_key(&self, key: &EncodedKey) -> bool;

    /// Inserts a key-value pair into the er.
    fn insert(&mut self, key: EncodedKey, value: Pending);

    /// Removes a key from the pending writes, returning the key-value pair if the key was previously in the pending writes.
    fn remove_entry(&mut self, key: &EncodedKey) -> Option<(EncodedKey, Pending)>;

    /// Returns an iterator over the pending writes.
    fn iter(&self) -> Self::Iter<'_>;

    /// Returns an iterator that consumes the pending writes.
    fn into_iter(self) -> Self::IntoIter;

    /// Rollback the pending writes.
    fn rollback(&mut self);
}

/// An trait that can be used to get a range over the pending writes.
pub trait PendingWritesRange: PendingWrites {
    /// The iterator type.
    type Range<'a>: IntoIterator<Item = (&'a EncodedKey, &'a Pending)>
    where
        Self: 'a;

    /// Returns an iterator over the pending writes.
    fn range<R: RangeBounds<EncodedKey>>(&self, range: R) -> Self::Range<'_>;
}

/// An trait that can be used to get a range over the pending writes.
pub trait PendingWritesComparableRange: PendingWritesRange + PendingWritesComparable {
    /// Returns an iterator over the pending writes.
    fn range_comparable<R>(&self, range: R) -> Self::Range<'_>
    where
        R: RangeBounds<EncodedKey>;
}

/// An optimized version of the [`PendingWrites`] trait that if your pending writes manager is depend on the order.
pub trait PendingWritesComparable: PendingWrites {
    /// Optimized version of [`PendingWrites::get`] that accepts borrowed keys.
    fn get_comparable(&self, key: &EncodedKey) -> Option<&Pending>;

    /// Optimized version of [`PendingWrites::get`] that accepts borrowed keys.
    fn get_entry_comparable(&self, key: &EncodedKey) -> Option<(&EncodedKey, &Pending)>;

    /// Optimized version of [`PendingWrites::contains_key`] that accepts borrowed keys.
    fn contains_key_comparable(&self, key: &EncodedKey) -> bool;

    /// Optimized version of [`PendingWrites::remove_entry`] that accepts borrowed keys.
    fn remove_entry_comparable(&mut self, key: &EncodedKey) -> Option<(EncodedKey, Pending)>;
}
