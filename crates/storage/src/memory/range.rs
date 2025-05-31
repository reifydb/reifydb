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

use crate::memory::Memory;
use crate::memory::value::VersionedValues;
use crate::storage::ScanRange;
use crate::{StoredValue, Version};
use reifydb_persistence::{Key, Value};
use std::ops::{Bound, RangeBounds};

impl<R> ScanRange<R> for Memory
where
    R: RangeBounds<Key>,
{
    type ScanRangeIter<'a>  = Range<'a, R>
    where
        Self: 'a;

    fn scan_range(&self, range: R, version: Version) -> Self::ScanRangeIter<'_> {
        Range { range: self.memory.range(range), version }
    }
}

pub struct Range<'a, R>
where
    R: RangeBounds<Key>,
{
    pub(crate) range: MapRange<'a, Key, R, Key, VersionedValues<Value>>,
    pub(crate) version: Version,
}

impl<'a, R> Iterator for Range<'a, R>
where
    R: RangeBounds<Key>,
{
    type Item = StoredValue;

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
                return Some(StoredValue { key: item.key().clone(), version, value }.into());
            }
        }
    }
}
