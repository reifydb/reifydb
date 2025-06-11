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

use crate::mvcc::types::Pending;
use crate::mvcc::types::TransactionValue;
use reifydb_core::Key;
use reifydb_core::either::Either;
use reifydb_storage::Storage;
use std::collections::btree_map::Range as BTreeMapRange;

pub struct TransactionRange<'a, S, C>
where
    S: Storage + 'a,
{
    pub(crate) committed: S::ScanRangeIter<'a>,
    pub(crate) pending: BTreeMapRange<'a, Key, Pending>,
    next_pending: Option<(&'a Key, &'a Pending)>,
    next_committed: Option<TransactionValue>,
    last_yielded_key: Option<Either<&'a Key, TransactionValue>>,
    marker: Option<Marker<'a, C>>,
}

impl<'a, S, C> TransactionRange<'a, S, C>
where
    C: Conflict,
    S: Storage + 'a,
{
    fn advance_pending(&mut self) {
        self.next_pending = self.pending.next();
    }

    fn advance_committed(&mut self) {
        self.next_committed = self.committed.next().map(|sv| sv.into());
        if let (Some(item), Some(marker)) = (&self.next_committed, &mut self.marker) {
            marker.mark(item.key());
        }
    }

    pub fn new(
        pending: BTreeMapRange<'a, Key, Pending>,
        committed: S::ScanRangeIter<'a>,
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

impl<'a, S, C> Iterator for TransactionRange<'a, S, C>
where
    C: Conflict,
    S: Storage + 'a,
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
                            match value.row() {
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
                    match value.row() {
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
