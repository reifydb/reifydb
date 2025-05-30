// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

// This file includes and modifies code from the skipdb project (https://github.com/al8n/skipdb),
// originally licensed under the Apache License, Version 2.0.
// Original copyright:
//   Copyright (c) 2024 Al Liu
//
// The original Apache License can be found at:
//   http://www.apache.org/licenses/LICENSE-2.0

use crate::mvcc::conflict::Conflict;
use crate::mvcc::marker::Marker;
use core::cmp;
use crossbeam_skiplist::map::Range as MapRange;

use reifydb_storage::Version;
use crate::mvcc::types::{Committed, TransactionValue};
use crate::mvcc::store::value::VersionedValues;
use crate::mvcc::types::Pending;
use reifydb_core::either::Either;
use reifydb_persistence::{Key, Value};
use std::collections::btree_map::Range as BTreeMapRange;
use std::ops::{Bound, RangeBounds};

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
    type Item = TransactionValue;

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
                return Some(Committed { key: item.key().clone(), version, value }.into());
            }
        }
    }
}

pub struct TransactionRange<'a, R, C>
where
    R: RangeBounds<Key> + 'a,
{
    pub(crate) committed: Range<'a, R>,
    pub(crate) pending: BTreeMapRange<'a, Key, Pending>,
    next_pending: Option<(&'a Key, &'a Pending)>,
    next_committed: Option<TransactionValue>,
    last_yielded_key: Option<Either<&'a Key, TransactionValue>>,
    marker: Option<Marker<'a, C>>,
}

impl<'a, R, C> TransactionRange<'a, R, C>
where
    R: RangeBounds<Key> + 'a,
    C: Conflict,
{
    fn advance_pending(&mut self) {
        self.next_pending = self.pending.next();
    }

    fn advance_committed(&mut self) {
        self.next_committed = self.committed.next();
        if let (Some(item), Some(marker)) = (&self.next_committed, &mut self.marker) {
            marker.mark(item.key());
        }
    }

    pub fn new(
        pending: BTreeMapRange<'a, Key, Pending>,
        committed: Range<'a, R>,
        marker: Option<Marker<'a, C>>,
    ) -> Self {
        let mut iterator = TransactionRange {
            pending,
            committed,
            next_pending: None,
            next_committed: None,
            last_yielded_key: None,
            marker,
        };

        iterator.advance_pending();
        iterator.advance_committed();

        iterator
    }
}

impl<'a, R, C> Iterator for TransactionRange<'a, R, C>
where
    R: RangeBounds<Key> + 'a,
    C: Conflict,
{
    type Item = TransactionValue;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match (self.next_pending, &self.next_committed) {
                // Both pending and committed iterators have items to yield.
                (Some((pending_key, _)), Some(committed)) => {
                    match pending_key.cmp(committed.key()) {
                        // Pending item has a smaller key, so yield this one.
                        cmp::Ordering::Less => {
                            let (key, value) = self.next_pending.take().unwrap();
                            self.advance_pending();
                            self.last_yielded_key = Some(Either::Left(key));
                            let version = value.version;
                            match value.value() {
                                Some(value) => return Some((version, key, value).into()),
                                None => continue,
                            }
                        }
                        // Keys are equal, so we prefer the pending item and skip the committed one.
                        cmp::Ordering::Equal => {
                            // Skip committed if it has the same key as pending
                            self.advance_committed();
                            // Loop again to check the next item without yielding anything this time.
                            continue;
                        }
                        // Committed item has a smaller key, so we consider yielding this one.
                        cmp::Ordering::Greater => {
                            let committed = self.next_committed.take().unwrap();
                            self.advance_committed(); // Prepare the next committed item for future iterations.
                            // Yield the committed item if it has not been yielded before.
                            if self.last_yielded_key.as_ref().map_or(true, |k| match k {
                                Either::Left(k) => *k != committed.key(),
                                Either::Right(item) => item.key() != committed.key(),
                            }) {
                                self.last_yielded_key = Some(Either::Right(committed.clone()));
                                return Some(committed);
                            }
                        }
                    }
                }
                // Only pending items are left, so yield the next pending item.
                (Some((_, _)), None) => {
                    let (key, value) = self.next_pending.take().unwrap();
                    self.advance_pending(); // Advance the pending iterator for the next iteration.
                    self.last_yielded_key = Some(Either::Left(key)); // Update the last yielded key.
                    let version = value.version;
                    match value.value() {
                        Some(value) => return Some((version, key, value).into()),
                        None => continue,
                    }
                }
                // Only committed items are left, so yield the next committed item if it hasn't been yielded already.
                (None, Some(committed)) => {
                    if self.last_yielded_key.as_ref().map_or(true, |k| match k {
                        Either::Left(k) => *k != committed.key(),
                        Either::Right(item) => item.key() != committed.key(),
                    }) {
                        let committed = self.next_committed.take().unwrap();
                        self.advance_committed(); // Advance the committed iterator for the next iteration.
                        self.last_yielded_key = Some(Either::Right(committed.clone()));
                        return Some(committed);
                    } else {
                        // The key has already been yielded, so move to the next.
                        self.advance_committed();
                        // Loop again to check the next item without yielding anything this time.
                        continue;
                    }
                }
                // Both iterators have no items left to yield.
                (None, None) => return None,
            }
        }
    }
}
