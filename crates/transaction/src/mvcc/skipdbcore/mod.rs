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
    sync::atomic::{AtomicU64, Ordering},
};

use crate::mvcc::version::types::{Entry, EntryData};
use crossbeam_skiplist::SkipMap;

use crate::mvcc::transaction::scan::iter::*;

use crate::mvcc::transaction::scan::rev_iter::*;

use crate::mvcc::transaction::scan::range::*;

use crate::mvcc::transaction::scan::rev_range::*;

pub mod types;
use crate::{Key, Value};
use types::*;

#[doc(hidden)]
pub trait Database: AsSkipCore {}

impl<T: AsSkipCore> Database for T {}

#[doc(hidden)]
pub trait AsSkipCore {
    // This trait is sealed and cannot be implemented for types outside of this crate.
    // So returning a reference to the inner database is ok.
    fn as_inner(&self) -> &SkipCore;
}

pub struct SkipCore {
    mem_table: SkipMap<Key, Values<Value>>,
    last_discard_version: AtomicU64,
}

impl Default for SkipCore {
    fn default() -> Self {
        Self::new()
    }
}

impl SkipCore {
    pub fn new() -> Self {
        Self { mem_table: SkipMap::new(), last_discard_version: AtomicU64::new(0) }
    }

    #[doc(hidden)]
    #[allow(private_interfaces)]
    pub fn __by_ref(&self) -> &SkipMap<Key, Values<Value>> {
        &self.mem_table
    }
}

impl SkipCore {
    pub fn apply(&self, entries: Vec<Entry>) {
        for ent in entries {
            let version = ent.version();
            match ent.data {
                EntryData::Set { key, value } => {
                    let ent = self.mem_table.get_or_insert_with(key, || Values::new());
                    let val = ent.value();
                    val.lock();
                    val.insert(version, Some(value));
                    val.unlock();
                }
                EntryData::Remove(key) => {
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

impl SkipCore {
    pub fn get(&self, key: &Key, version: u64) -> Option<CommittedRef<'_>> {
        let ent = self.mem_table.get(key)?;
        let version = ent
            .value()
            .upper_bound(Bound::Included(&version))
            .and_then(|v| if v.value().is_some() { Some(*v.key()) } else { None })?;

        Some(CommittedRef { ent, version })
    }

    pub fn contains_key(&self, key: &Key, version: u64) -> bool {
        match self.mem_table.get(key) {
            None => false,
            Some(values) => match values.value().upper_bound(Bound::Included(&version)) {
                None => false,
                Some(ent) => ent.value().is_some(),
            },
        }
    }

    pub fn iter(&self, version: u64) -> Iter<'_> {
        let iter = self.mem_table.iter();
        Iter { iter, version }
    }

    pub fn iter_rev(&self, version: u64) -> RevIter<'_> {
        let iter = self.mem_table.iter();
        RevIter { iter: iter.rev(), version }
    }

    pub fn range<R>(&self, range: R, version: u64) -> Range<'_, R>
    where
        R: RangeBounds<Key>,
    {
        Range { range: self.mem_table.range(range), version }
    }

    pub fn range_rev<R>(&self, range: R, version: u64) -> RevRange<'_, R>
    where
        R: RangeBounds<Key>,
    {
        RevRange { range: self.mem_table.range(range).rev(), version }
    }
}

impl SkipCore {
    pub fn compact(&self, new_discard_version: u64) {
        match self.last_discard_version.fetch_update(Ordering::SeqCst, Ordering::Acquire, |val| {
            if val >= new_discard_version { None } else { Some(new_discard_version) }
        }) {
            Ok(_) => {}
            // if we fail to insert the new discard version,
            // which means there is another thread that is compacting the database.
            // To avoid run multiple compacting at the same time, we just return.
            Err(_) => return,
        }

        for ent in self.mem_table.iter() {
            let values = ent.value();

            // if the oldest version is larger or equal to the new discard version,
            // then nothing to remove.
            if let Some(oldest) = values.front() {
                let oldest_version = *oldest.key();
                if oldest_version >= new_discard_version {
                    continue;
                }
            }

            if let Some(newest) = values.back() {
                let newest_version = *newest.key();

                // if the newest version is smaller than the new discard version,
                if newest_version < new_discard_version {
                    // if the newest value is none, then we can try to remove the whole key.
                    if newest.value().is_none() {
                        // try to lock the entry.
                        if values.try_lock() {
                            // we get the lock, then we can remove the whole key.
                            ent.remove();

                            // unlock the entry.
                            values.unlock();
                            continue;
                        }
                    }

                    // we leave the current newest value and try to remove previous values.
                    let mut prev = newest.prev();
                    while let Some(ent) = prev {
                        prev = ent.prev();
                        ent.remove();
                    }
                    continue;
                }

                // handle the complex case: we have some values that are larger than the new discard version,
                // and some values that are smaller than the new discard version.

                // find the first value that is smaller than the new discard version.
                let mut bound = values.upper_bound(Bound::Excluded(&new_discard_version));

                // means that no value is smaller than the new discard version.
                if bound.is_none() {
                    continue;
                }

                // remove all values that are smaller than the new discard version.
                while let Some(ent) = bound {
                    bound = ent.prev();
                    ent.remove();
                }
            } else {
                // we do not have any value in the entry, then we can try to remove the whole key.

                // try to lock the entry.
                if values.try_lock() {
                    // we get the lock, then we can remove the whole key.
                    ent.remove();

                    // unlock the entry.
                    values.unlock();
                }
            }
        }
    }
}
