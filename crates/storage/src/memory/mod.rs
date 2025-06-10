// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use iter::Iter;
pub use iter_rev::IterRev;
pub use range::Range;
pub use range_rev::RangeRev;

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
use crossbeam_skiplist::SkipMap;
use reifydb_core::Key;

pub struct Memory {
    memory: SkipMap<Key, Versioned>,
}

impl Default for Memory {
    fn default() -> Self {
        Self::new()
    }
}

impl Memory {
    pub fn new() -> Self {
        Self { memory: SkipMap::new() }
    }
}

impl Storage for Memory {}
