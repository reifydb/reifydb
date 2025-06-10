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

use crate::Stored;
use crate::memory::Memory;
use crate::memory::versioned::Versioned;
use crate::storage::ScanRev;
use reifydb_core::{Key, Version};
use std::ops::Bound;

impl ScanRev for Memory {
    type ScanIterRev<'a> = IterRev<'a>;

    fn scan_rev(&self, version: Version) -> Self::ScanIterRev<'_> {
        let iter = self.memory.iter();
        IterRev { iter: iter.rev(), version }
    }
}

pub struct IterRev<'a> {
    pub(crate) iter: Rev<MapIter<'a, Key, Versioned>>,
    pub(crate) version: Version,
}

impl<'a> Iterator for IterRev<'a> {
    type Item = Stored;

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
                return Some(Stored { key: item.key().clone(), bytes: value, version }.into());
            }
        }
    }
}
