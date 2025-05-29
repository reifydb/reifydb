// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::{borrow::Borrow, hash::Hash, ops::RangeBounds};

use crate::skipdb::conflict::{
    Cm, CmComparable, CmComparableRange, CmEquivalent, CmEquivalentRange, CmIter, CmRange,
};

/// A marker used to mark the keys that are read.
pub struct Marker<'a, C> {
    marker: &'a mut C,
}

impl<'a, C> Marker<'a, C> {
    /// Returns a new marker.

    pub fn new(marker: &'a mut C) -> Self {
        Self { marker }
    }
}

impl<C: Cm> Marker<'_, C> {
    /// Marks a key is operated.
    pub fn mark(&mut self, k: &C::Key) {
        self.marker.mark_read(k);
    }

    /// Marks a key is conflicted.
    pub fn mark_conflict(&mut self, k: &C::Key) {
        self.marker.mark_conflict(k);
    }
}

impl<C: CmRange> Marker<'_, C> {
    /// Marks a key is operated.
    pub fn mark_range(&mut self, range: impl RangeBounds<<C as Cm>::Key>) {
        self.marker.mark_range(range);
    }
}

impl<C: CmIter> Marker<'_, C> {
    /// Marks a key is operated.
    pub fn mark_iter(&mut self) {
        self.marker.mark_iter();
    }
}

impl<C: CmComparable> Marker<'_, C> {
    /// Marks a key is operated.
    pub fn mark_comparable<Q>(&mut self, k: &Q)
    where
        C::Key: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.marker.mark_read_comparable(k);
    }

    /// Marks a key is conflicted.
    pub fn mark_conflict_comparable<Q>(&mut self, k: &Q)
    where
        C::Key: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.marker.mark_conflict_comparable(k);
    }
}

impl<C: CmComparableRange> Marker<'_, C> {
    /// Marks a range is operated.
    pub fn mark_range_comparable<Q>(&mut self, range: impl RangeBounds<Q>)
    where
        C::Key: Borrow<Q>,
        Q: Ord + ?Sized,
    {
        self.marker.mark_range_comparable(range);
    }
}

impl<C: CmEquivalent> Marker<'_, C> {
    /// Marks a key is operated.
    pub fn mark_equivalent<Q>(&mut self, k: &Q)
    where
        C::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.marker.mark_read_equivalent(k);
    }

    /// Marks a key is conflicted.
    pub fn mark_conflict_equivalent<Q>(&mut self, k: &Q)
    where
        C::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.marker.mark_conflict_equivalent(k);
    }
}

impl<C: CmEquivalentRange> Marker<'_, C> {
    /// Marks a range is operated.
    pub fn mark_range_equivalent<Q>(&mut self, range: impl RangeBounds<Q>)
    where
        C::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.marker.mark_range_equivalent(range);
    }
}
