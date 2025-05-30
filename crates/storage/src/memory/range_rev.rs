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

use crate::memory::value::VersionedValues;
use crate::{StoredValue, Version};
use reifydb_persistence::{Key, Value};
use std::ops::{Bound, RangeBounds};

pub struct RevRange<'a, R>
where
    R: RangeBounds<Key>,
{
    pub(crate) range: Rev<MapRange<'a, Key, R, Key, VersionedValues<Value>>>,
    pub(crate) version: Version,
}

impl<'a, R> Iterator for RevRange<'a, R>
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
