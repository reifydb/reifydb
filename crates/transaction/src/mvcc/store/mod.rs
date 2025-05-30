// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

// #![deny(warnings)]
// #![forbid(unsafe_code)]
// #![allow(clippy::type_complexity)]

extern crate alloc;

use core::{
    borrow::Borrow,
    ops::{Bound, RangeBounds},
};

use crate::mvcc::types::Pending;
use crossbeam_skiplist::SkipMap;

use crate::mvcc::transaction::scan::iter::*;
use crate::mvcc::transaction::scan::range::*;
use crate::mvcc::transaction::scan::rev_iter::*;
use crate::mvcc::transaction::scan::rev_range::*;

pub mod types;
pub mod value;

use crate::Version;
use crate::mvcc::store::value::VersionedValues;
use reifydb_persistence::{Action, Key, Value};
use types::*;

pub struct Store {
    mem_table: SkipMap<Key, VersionedValues<Value>>,
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
}

impl Store {
    pub fn new() -> Self {
        Self { mem_table: SkipMap::new() }
    }
}

impl Store {
    pub fn apply(&self, actions: Vec<Pending>) {
        for item in actions {
            let version = item.version();
            match item.action {
                Action::Set { key, value } => {
                    let item = self.mem_table.get_or_insert_with(key, || VersionedValues::new());
                    let val = item.value();
                    val.lock();
                    val.insert(version, Some(value));
                    val.unlock();
                }
                Action::Remove { key } => {
                    if let Some(values) = self.mem_table.get(&key) {
                        let values = values.value();
                        if !values.is_empty() {
                            values.insert(version, None);
                        }
                    }
                }
            }
        }
    }
}

impl Store {
    pub fn get(&self, key: &Key, version: Version) -> Option<Committed> {
        let item = self.mem_table.get(key)?;
        let (version, value) =
            item.value().upper_bound(Bound::Included(&version)).and_then(|v| {
                if v.value().is_some() {
                    Some((*v.key(), v.value().clone().unwrap()))
                } else {
                    None
                }
            })?;

        Some(Committed { key: key.clone(), value, version })
    }

    pub fn contains_key(&self, key: &Key, version: Version) -> bool {
        match self.mem_table.get(key) {
            None => false,
            Some(values) => match values.value().upper_bound(Bound::Included(&version)) {
                None => false,
                Some(item) => item.value().is_some(),
            },
        }
    }

    pub fn iter(&self, version: Version) -> Iter<'_> {
        let iter = self.mem_table.iter();
        Iter { iter, version }
    }

    pub fn iter_rev(&self, version: Version) -> RevIter<'_> {
        let iter = self.mem_table.iter();
        RevIter { iter: iter.rev(), version }
    }

    pub fn range<R>(&self, range: R, version: Version) -> Range<'_, R>
    where
        R: RangeBounds<Key>,
    {
        Range { range: self.mem_table.range(range), version }
    }

    pub fn range_rev<R>(&self, range: R, version: Version) -> RevRange<'_, R>
    where
        R: RangeBounds<Key>,
    {
        RevRange { range: self.mem_table.range(range).rev(), version }
    }
}
