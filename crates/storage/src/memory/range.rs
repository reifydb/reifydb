// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crossbeam_skiplist::map::Range as MapRange;

use crate::Stored;
use crate::memory::Memory;
use crate::memory::versioned::Versioned;
use crate::storage::ScanRange;
use reifydb_core::{EncodedKey, EncodedKeyRange, Version};
use std::ops::Bound;

impl ScanRange for Memory {
    type ScanRangeIter<'a>
        = Range<'a>
    where
        Self: 'a;

    fn scan_range(&self, range: EncodedKeyRange, version: Version) -> Self::ScanRangeIter<'_> {
        Range { range: self.memory.range(range), version }
    }
}

pub struct Range<'a> {
    pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, Versioned>,
    pub(crate) version: Version,
}

impl<'a> Iterator for Range<'a> {
    type Item = Stored;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.range.next()?;
            if let Some((version, value)) =
                item.value().upper_bound(Bound::Included(&self.version)).and_then(|item| {
                    if item.value().is_some() {
                        Some((*item.key(), item.value().clone().unwrap()))
                    } else {
                        None
                    }
                })
            {
                return Some(Stored { key: item.key().clone(), version, row: value }.into());
            }
        }
    }
}
