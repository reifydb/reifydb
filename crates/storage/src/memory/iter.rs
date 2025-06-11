// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::Stored;
use crate::memory::Memory;
use crate::memory::versioned::Versioned;
use crate::storage::Scan;
use crossbeam_skiplist::map::Iter as MapIter;
use reifydb_core::{EncodedKey, Version};
use std::ops::Bound;

impl Scan for Memory {
    type ScanIter<'a> = Iter<'a>;

    fn scan(&self, version: Version) -> Self::ScanIter<'_> {
        let iter = self.memory.iter();
        Iter { iter, version }
    }
}

pub struct Iter<'a> {
    pub(crate) iter: MapIter<'a, EncodedKey, Versioned>,
    pub(crate) version: Version,
}

impl<'a> Iterator for Iter<'a> {
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
                return Some(Stored { key: item.key().clone(), row: value, version }.into());
            }
        }
    }
}
