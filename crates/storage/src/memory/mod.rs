// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

pub use iter::Iter;
pub use iter_rev::IterRev;
pub use range::Range;
pub use range_rev::RangeRev;
use std::ops::RangeBounds;

mod apply;
mod contains;
mod get;
mod iter;
mod iter_rev;
mod range;
mod range_rev;
mod value;

use crate::Storage;
use crate::memory::value::VersionedValues;
use crossbeam_skiplist::SkipMap;
use reifydb_persistence::{Key, Value};

pub struct Memory {
    memory: SkipMap<Key, VersionedValues<Value>>,
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

impl<R> Storage<R> for Memory where R: RangeBounds<Key> {}
