// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use std::borrow::Borrow;
use std::hash::Hash;
use std::ops::RangeBounds;

pub use btree::BTreeCm;
pub use hash::{HashCm, HashCmOptions};

mod btree;
mod hash;

/// The conflict manager that can be used to manage the conflicts in a transaction.
///
/// The conflict normally needs to have:
///
/// 1. Contains fingerprints of keys read.
/// 2. Contains fingerprints of keys written. This is used for conflict detection.
pub trait Cm: Sized {
    /// The error type returned by the conflict manager.
    type Error: crate::skipdb::error::Error;

    /// The key type.
    type Key;

    /// The options type used to create the conflict manager.
    type Options;

    /// Create a new conflict manager with the given options.
    fn new(options: Self::Options) -> Result<Self, Self::Error>;

    /// Mark the key is read.
    fn mark_read(&mut self, key: &Self::Key);

    /// Mark the key is .
    fn mark_conflict(&mut self, key: &Self::Key);

    /// Returns true if we have a conflict.
    fn has_conflict(&self, other: &Self) -> bool;

    /// Rollback the conflict manager.
    fn rollback(&mut self) -> Result<(), Self::Error>;
}

/// A extended trait of the [`Cm`] trait that can be used to manage the range of keys.
pub trait CmRange: Cm + Sized {
    /// Mark the range is read.
    fn mark_range(&mut self, range: impl RangeBounds<<Self as Cm>::Key>);
}

/// A extended trait of the [`Cm`] trait that can be used to manage the iterator of keys.
pub trait CmIter: Cm + Sized {
    /// Mark the iterator is operated, this is useful to detect the indirect conflict.
    fn mark_iter(&mut self);
}

impl<T: CmRange> CmIter for T {
    fn mark_iter(&mut self) {
        self.mark_range(..);
    }
}

/// An optimized version of the [`Cm`] trait that if your conflict manager is depend on hash.
pub trait CmEquivalent: Cm {
    /// Optimized version of [`mark_read`] that accepts borrowed keys. Optional to implement.
    fn mark_read_equivalent<Q>(&mut self, key: &Q)
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;

    /// Optimized version of [`mark_conflict`] that accepts borrowed keys. Optional to implement.
    fn mark_conflict_equivalent<Q>(&mut self, key: &Q)
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
}

/// An optimized version of the [`CmRange`] trait that if your conflict manager is depend on hash.
pub trait CmEquivalentRange: CmRange + Sized {
    /// Mark the range is read.
    fn mark_range_equivalent<Q>(&mut self, range: impl RangeBounds<Q>)
    where
        Self::Key: Borrow<Q>,
        Q: Hash + Eq + ?Sized;
}

/// An optimized version of the [`Cm`] trait that if your conflict manager is depend on the order.
pub trait CmComparable: Cm {
    /// Optimized version of [`mark_read`] that accepts borrowed keys. Optional to implement.
    fn mark_read_comparable<Q>(&mut self, key: &Q)
    where
        Self::Key: Borrow<Q>,
        Q: Ord + ?Sized;

    /// Optimized version of [`mark_conflict`] that accepts borrowed keys. Optional to implement.
    fn mark_conflict_comparable<Q>(&mut self, key: &Q)
    where
        Self::Key: Borrow<Q>,
        Q: Ord + ?Sized;
}

/// An optimized version of the [`CmRange`] trait that if your conflict manager is depend on the order.
pub trait CmComparableRange: CmRange + CmComparable {
    /// Mark the range is read.
    fn mark_range_comparable<Q>(&mut self, range: impl RangeBounds<Q>)
    where
        Self::Key: Borrow<Q>,
        Q: Ord + ?Sized;
}
