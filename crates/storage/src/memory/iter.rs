// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::memory::Memory;
use crate::memory::versioned::VersionedRow;
use crossbeam_skiplist::map::Iter as MapIter;
use reifydb_core::interface::{Unversioned, UnversionedScan, Versioned, VersionedScan};
use reifydb_core::row::EncodedRow;
use reifydb_core::{EncodedKey, Error, Version};
use std::ops::Bound;

impl VersionedScan for Memory {
    type ScanIter<'a> = VersionedIter<'a>;

    fn scan(&self, version: Version) -> Self::ScanIter<'_> {
        let iter = self.versioned.iter();
        VersionedIter { iter, version }
    }
}

pub struct VersionedIter<'a> {
    pub(crate) iter: MapIter<'a, EncodedKey, VersionedRow>,
    pub(crate) version: Version,
}

impl Iterator for VersionedIter<'_> {
    type Item = Versioned;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.iter.next()?;
            if let Some((version, row)) =
                item.value().upper_bound(Bound::Included(&self.version)).and_then(|item| {
                    if item.value().is_some() {
                        Some((*item.key(), item.value().clone().unwrap()))
                    } else {
                        None
                    }
                })
            {
                return Some(Versioned { key: item.key().clone(), row, version });
            }
        }
    }
}

impl UnversionedScan for Memory {
    type ScanIter<'a> = UnversionedIter<'a>;

    fn scan(&self) -> Result<Self::ScanIter<'_>, Error> {
        let iter = self.unversioned.iter();
        Ok(UnversionedIter { iter })
    }
}

pub struct UnversionedIter<'a> {
    pub(crate) iter: MapIter<'a, EncodedKey, EncodedRow>,
}

impl Iterator for UnversionedIter<'_> {
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let item = self.iter.next()?;
            return Some(Unversioned { key: item.key().clone(), row: item.value().clone() });
        }
    }
}
