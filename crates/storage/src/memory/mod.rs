// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

pub use iter::VersionedIter;
pub use iter_rev::IterRev;
pub use range::Range;
pub use range_rev::RangeRev;
use std::ops::Deref;
use std::sync::Arc;

mod apply;
mod contains;
mod get;
mod iter;
mod iter_rev;
mod range;
mod range_rev;
mod versioned;

use crate::memory::versioned::VersionedRow;
use crossbeam_skiplist::SkipMap;
use reifydb_core::EncodedKey;
use reifydb_core::interface::{
    UnversionedRemove, UnversionedSet, UnversionedStorage, VersionedStorage,
};
use reifydb_core::row::EncodedRow;

#[derive(Clone)]
pub struct Memory(Arc<MemoryInner>);

pub struct MemoryInner {
    versioned: SkipMap<EncodedKey, VersionedRow>,
    unversioned: SkipMap<EncodedKey, EncodedRow>,
}

impl Deref for Memory {
    type Target = MemoryInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for Memory {
    fn default() -> Self {
        Self::new()
    }
}

impl Memory {
    pub fn new() -> Self {
        Self(Arc::new(MemoryInner { versioned: SkipMap::new(), unversioned: SkipMap::new() }))
    }
}

impl VersionedStorage for Memory {}
impl UnversionedStorage for Memory {}
impl UnversionedSet for Memory {}
impl UnversionedRemove for Memory {}
