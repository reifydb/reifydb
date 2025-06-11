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

use crate::Storage;
use crate::memory::versioned::Versioned;
use crate::storage::GetHooks;
use crossbeam_skiplist::SkipMap;
use reifydb_core::EncodedKey;
use reifydb_core::hook::Hooks;

#[derive(Clone)]
pub struct Memory(Arc<MemoryInner>);

pub struct MemoryInner {
    memory: SkipMap<EncodedKey, Versioned>,
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
        Self(Arc::new(MemoryInner { memory: SkipMap::new(), hooks: Default::default() }))
    }
}

impl GetHooks for Memory {
    fn hooks(&self) -> Hooks {
        self.hooks.clone()
    }
}

impl Storage for Memory {}
