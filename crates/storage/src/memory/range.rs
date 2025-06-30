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
use crate::memory::versioned::VersionedRow;
use reifydb_core::interface::{Unversioned, UnversionedScanRange, Versioned, VersionedScanRange};
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange, Error, Version};
use std::ops::Bound;

impl VersionedScanRange for Memory {
    type ScanRangeIter<'a>
        = Range<'a>
    where
        Self: 'a;

    fn scan_range(&self, range: EncodedKeyRange, version: Version) -> Self::ScanRangeIter<'_> {
        Range { range: self.versioned.range(range), version }
    }
}

pub struct Range<'a> {
    pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, VersionedRow>,
    pub(crate) version: Version,
}

impl Iterator for Range<'_> {
    type Item = Versioned;

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
                return Some(Versioned { key: item.key().clone(), version, row: value });
            }
        }
    }
}

impl UnversionedScanRange for Memory {
    type ScanRange<'a>
        = UnversionedRange<'a>
    where
        Self: 'a;

    fn scan_range(&self, range: EncodedKeyRange) -> Result<Self::ScanRange<'_>, Error> {
        Ok(UnversionedRange { range: self.unversioned.range(range) })
    }
}

pub struct UnversionedRange<'a> {
    pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, EncodedRow>,
}

impl Iterator for UnversionedRange<'_> {
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.range.next()?;
            return Some(Unversioned { key: item.key().clone(), row: item.value().clone() });
        }
    }
}
