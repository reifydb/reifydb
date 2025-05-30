// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::ops::RangeBounds;

use crate::Key;
pub use btree::BTreeConflict;

mod btree;

/// The conflict manager that can be used to manage the conflicts in a transaction.
pub trait Conflict: Default + Sized {
    /// Create a new conflict manager.
    fn new() -> Self;
    /// Mark the key is read.
    fn mark_read(&mut self, key: &Key);
    /// Mark the key is .
    fn mark_conflict(&mut self, key: &Key);
    /// Returns true if we have a conflict.
    fn has_conflict(&self, other: &Self) -> bool;
    /// Rollback the conflict manager.
    fn rollback(&mut self);
}

/// A extended trait of the [`Conflict`] trait that can be used to manage the range of keys.
pub trait ConflictRange: Conflict + Sized {
    /// Mark the range is read.
    fn mark_range(&mut self, range: impl RangeBounds<Key>);
}

/// A extended trait of the [`Conflict`] trait that can be used to manage the iterator of keys.
pub trait ConflictIter: Conflict + Sized {
    /// Mark the iterator is operated, this is useful to detect the indirect conflict.
    fn mark_iter(&mut self);
}

impl<T: ConflictRange> ConflictIter for T {
    fn mark_iter(&mut self) {
        self.mark_range(..);
    }
}
