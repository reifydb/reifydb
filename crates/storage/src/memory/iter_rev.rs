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
use crossbeam_skiplist::map::Iter as MapIter;

use crate::memory::Memory;
use crate::memory::versioned::VersionedRow;
use crate::unversioned::UnversionedScanRev;
use crate::versioned::VersionedScanRev;
use crate::{Unversioned, Versioned};
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, Version};
use std::ops::Bound;

impl VersionedScanRev for Memory {
    type ScanIterRev<'a> = IterRev<'a>;

    fn scan_rev(&self, version: Version) -> Self::ScanIterRev<'_> {
        let iter = self.versioned.iter();
        IterRev { iter: iter.rev(), version }
    }
}

pub struct IterRev<'a> {
    pub(crate) iter: Rev<MapIter<'a, EncodedKey, VersionedRow>>,
    pub(crate) version: Version,
}

impl Iterator for IterRev<'_> {
    type Item = Versioned;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.iter.next()?;
            if let Some((version, value)) =
                item.value().upper_bound(Bound::Included(&self.version)).and_then(|item| {
                    if item.value().is_some() {
                        Some((*item.key(), item.value().clone().unwrap()))
                    } else {
                        None
                    }
                })
            {
                return Some(Versioned { key: item.key().clone(), row: value, version });
            }
        }
    }
}

impl UnversionedScanRev for Memory {
    type ScanIterRev<'a> = UnversionedIterRev<'a>;

    fn scan_rev_unversioned(&self) -> Self::ScanIterRev<'_> {
        let iter = self.unversioned.iter();
        UnversionedIterRev { iter }
    }
}

pub struct UnversionedIterRev<'a> {
    pub(crate) iter: MapIter<'a, EncodedKey, EncodedRow>,
}

impl Iterator for UnversionedIterRev<'_> {
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.iter.next_back()?;
            return Some(Unversioned { key: item.key().clone(), row: item.value().clone() });
        }
    }
}
