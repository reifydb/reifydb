// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Key, Value};
use heed::types::Bytes;
use heed::{Database, Env};
use reifydb_core::AsyncCowVec;
use std::collections::{Bound, VecDeque};
use std::ops::RangeBounds;
use std::sync::Arc;

pub struct LmdbScanIter {
    env: Arc<Env>,
    db: Database<Bytes, Bytes>,
    start: Bound<Key>,
    end: Bound<Key>,
    buffer: VecDeque<crate::Result<(Key, Value)>>,
    last_key: Option<Key>,
    batch_size: usize,
}

impl LmdbScanIter {
    pub fn new(
        env: Arc<Env>,
        db: Database<Bytes, Bytes>,
        range: impl RangeBounds<Key>,
        batch_size: usize,
    ) -> Self {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        Self { env, db, buffer: VecDeque::new(), last_key: None, start, end, batch_size }
    }

    fn refill_buffer(&mut self) -> crate::Result<()> {
        let txn = self.env.read_txn().unwrap(); // FIXME

        let start_bound: Bound<&[u8]> = match &self.last_key {
            Some(k) => Bound::Excluded(&k[..]),
            None => match &self.start {
                Bound::Included(k) => Bound::Included(&k[..]),
                Bound::Excluded(k) => Bound::Excluded(&k[..]),
                Bound::Unbounded => Bound::Unbounded,
            },
        };

        let end_bound: Bound<&[u8]> = match &self.end {
            Bound::Included(k) => Bound::Included(&k[..]),
            Bound::Excluded(k) => Bound::Excluded(&k[..]),
            Bound::Unbounded => Bound::Unbounded,
        };

        let effective_range = (start_bound, end_bound);
        let iter = self.db.range(&txn, &effective_range).unwrap();
        self.buffer.clear();

        for result in iter.take(self.batch_size) {
            match result {
                Ok((k, v)) => {
                    self.last_key = Some(AsyncCowVec::new(k.to_vec()));
                    self.buffer.push_back(Ok((
                        AsyncCowVec::new(k.to_vec()),
                        AsyncCowVec::new(v.to_vec()),
                    )));
                }
                Err(e) => {
                    // FIXME
                    // return Err(crate::Error::Persistence(e.into()));
                    return panic!("");
                }
            }
        }

        Ok(())
    }
}

impl Iterator for LmdbScanIter {
    type Item = crate::Result<(Key, Value)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            if let Err(e) = self.refill_buffer() {
                return Some(Err(e));
            }
        }
        self.buffer.pop_front()
    }
}
