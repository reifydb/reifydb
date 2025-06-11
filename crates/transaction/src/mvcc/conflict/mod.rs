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

pub use btree::BTreeConflict;
use reifydb_core::{EncodedKey, EncodedKeyRange};

mod btree;

pub trait Conflict: Default + Sized {
    fn new() -> Self;
    fn mark_read(&mut self, key: &EncodedKey);
    fn mark_conflict(&mut self, key: &EncodedKey);
    fn has_conflict(&self, other: &Self) -> bool;
    fn rollback(&mut self);
}

pub trait ConflictRange: Conflict + Sized {
    fn mark_range(&mut self, range: EncodedKeyRange);
}

pub trait ConflictIter: Conflict + Sized {
    fn mark_iter(&mut self);
}

impl<T: ConflictRange> ConflictIter for T {
    fn mark_iter(&mut self) {
        self.mark_range(EncodedKeyRange::all());
    }
}
