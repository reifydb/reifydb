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

use crate::mvcc::conflict::{Conflict, ConflictIter, ConflictRange};

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

impl<C: Conflict> Marker<'_, C> {
    /// Marks a key is operated.
    pub fn mark(&mut self, k: &C::Key) {
        self.marker.mark_read(k);
    }

    /// Marks a key is conflicted.
    pub fn mark_conflict(&mut self, k: &C::Key) {
        self.marker.mark_conflict(k);
    }
}

impl<C: ConflictRange> Marker<'_, C> {
    /// Marks a key is operated.
    pub fn mark_range(&mut self, range: impl RangeBounds<<C as Conflict>::Key>) {
        self.marker.mark_range(range);
    }
}

impl<C: ConflictIter> Marker<'_, C> {
    /// Marks a key is operated.
    pub fn mark_iter(&mut self) {
        self.marker.mark_iter();
    }
}
