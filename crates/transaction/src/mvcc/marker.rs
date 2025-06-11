// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::conflict::{Conflict, ConflictRange};
use reifydb_core::{EncodedKey, EncodedKeyRange};

pub struct Marker<'a, C> {
    marker: &'a mut C,
}

impl<'a, C> Marker<'a, C> {
    pub fn new(marker: &'a mut C) -> Self {
        Self { marker }
    }
}

impl<C: Conflict> Marker<'_, C> {
    pub fn mark(&mut self, k: &EncodedKey) {
        self.marker.mark_read(k);
    }
}

impl<C: ConflictRange> Marker<'_, C> {
    pub fn mark_range(&mut self, range: EncodedKeyRange) {
        self.marker.mark_range(range);
    }
}
