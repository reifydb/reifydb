// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::lmdb::Lmdb;
use heed::types::Bytes;
use heed::{Database, Env};
use reifydb_core::interface::{Unversioned, UnversionedScanRange, Versioned, VersionedScanRange};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, EncodedKey, EncodedKeyRange, Result, Version};
use std::collections::{Bound, VecDeque};
use std::ops::RangeBounds;
use std::sync::Arc;

impl VersionedScanRange for Lmdb {
    type ScanRangeIter<'a> = Range;

    fn range(
        &self,
        range: EncodedKeyRange,
        version: Version,
    ) -> Result<Self::ScanRangeIter<'_>> {
        Ok(Range::new(self.env.clone(), self.db, version, range, 100))
    }
}

pub struct Range {
    env: Arc<Env>,
    db: Database<Bytes, Bytes>,
    _version: Version,
    start: Bound<EncodedKey>,
    end: Bound<EncodedKey>,
    buffer: VecDeque<Versioned>,
    last_key: Option<EncodedKey>,
    batch_size: usize,
}

impl Range {
    pub fn new(
        env: Arc<Env>,
        db: Database<Bytes, Bytes>,
        version: Version,
        range: EncodedKeyRange,
        batch_size: usize,
    ) -> Self {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();

        Self {
            env,
            db,
            buffer: VecDeque::new(),
            _version: version,
            last_key: None,
            start,
            end,
            batch_size,
        }
    }

    fn refill_buffer(&mut self) {
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
                    self.last_key = Some(EncodedKey::new(k.to_vec()));
                    // self.buffer.push_back(Ok((
                    //     CowVec::new(k.to_vec()),
                    //     CowVec::new(v.to_vec()),
                    // )));
                    //
                    self.buffer.push_back(Versioned {
                        key: EncodedKey::new(k.to_vec()),
                        row: EncodedRow(CowVec::new(v.to_vec())),
                        version: 0, // FIXME
                    })
                }
                Err(_) => {
                    // FIXME
                    // return Err(crate::Error::Persistence(e.into()));
                    unimplemented!();
                }
            }
        }
    }
}

impl Iterator for Range {
    type Item = Versioned;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            self.refill_buffer()
        }
        self.buffer.pop_front()
    }
}

impl UnversionedScanRange for Lmdb {
    type ScanRange<'a>
        = UnversionedRange
    where
        Self: 'a;

    fn range(&self, _range: EncodedKeyRange) -> Result<Self::ScanRange<'_>> {
        todo!()
    }
}

pub struct UnversionedRange {}

impl Iterator for UnversionedRange {
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
