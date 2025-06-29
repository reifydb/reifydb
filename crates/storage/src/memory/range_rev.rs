// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use core::iter::Rev;
use crossbeam_skiplist::map::Range as MapRange;

use crate::memory::Memory;
use crate::memory::versioned::VersionedRow;
use reifydb_core::interface::{
    Unversioned, UnversionedScanRangeRev, Versioned, VersionedScanRangeRev,
};
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, EncodedKeyRange, Version};
use std::ops::Bound;

impl VersionedScanRangeRev for Memory {
    type ScanRangeIterRev<'a>
        = RangeRev<'a>
    where
        Self: 'a;

    fn scan_range_rev(
        &self,
        range: EncodedKeyRange,
        version: Version,
    ) -> Self::ScanRangeIterRev<'_> {
        RangeRev { range: self.versioned.range(range).rev(), version }
    }
}

pub struct RangeRev<'a> {
    pub(crate) range: Rev<MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, VersionedRow>>,
    pub(crate) version: Version,
}

impl Iterator for RangeRev<'_> {
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

impl UnversionedScanRangeRev for Memory {
    type ScanRangeRev<'a>
        = UnversionedRangeRev<'a>
    where
        Self: 'a;

    fn scan_range_rev_unversioned(&self, range: EncodedKeyRange) -> Self::ScanRangeRev<'_> {
        UnversionedRangeRev { range: self.unversioned.range(range) }
    }
}

pub struct UnversionedRangeRev<'a> {
    pub(crate) range: MapRange<'a, EncodedKey, EncodedKeyRange, EncodedKey, EncodedRow>,
}

impl Iterator for UnversionedRangeRev<'_> {
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.range.next_back()?;
            return Some(Unversioned { key: item.key().clone(), row: item.value().clone() });
        }
    }
}
