// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use iter::Iter;
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
use crate::unversioned::UnversionedStorage;
use crate::{GetHooks, Storage, VersionedStorage};
use crossbeam_skiplist::SkipMap;
use reifydb_core::EncodedKey;
use reifydb_core::hook::Hooks;
use reifydb_core::row::EncodedRow;

#[derive(Clone)]
pub struct Memory(Arc<MemoryInner>);

pub struct MemoryInner {
    versioned: SkipMap<EncodedKey, VersionedRow>,
    unversioned: SkipMap<EncodedKey, EncodedRow>,
    hooks: Hooks,
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
        Self(Arc::new(MemoryInner {
            versioned: SkipMap::new(),
            unversioned: SkipMap::new(),
            hooks: Default::default(),
        }))
    }
}

impl GetHooks for Memory {
    fn hooks(&self) -> Hooks {
        self.hooks.clone()
    }
}

impl VersionedStorage for Memory {}
impl UnversionedStorage for Memory {}
impl Storage for Memory {}
