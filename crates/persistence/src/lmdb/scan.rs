// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Key, Value};
use heed::types::Bytes;
use heed::{Database, RoIter, RoTxn, WithTls};
use std::collections::Bound;
use std::ops::RangeBounds;

pub struct LmdbScanIter<'a> {
    iter: RoIter<'a, Bytes, Bytes>,
    txn: &'a RoTxn<'a, WithTls>, // borrow txn
    range: Box<dyn Fn(&[u8]) -> bool + 'a>,
}

impl<'a> LmdbScanIter<'a> {
    pub fn new(
        txn: &'a RoTxn<'a, WithTls>,
        db: Database<Bytes, Bytes>,
        range: impl RangeBounds<Key> + 'a,
    ) -> Self {
        let iter = db.iter(txn).unwrap();

        let start = match range.start_bound() {
            Bound::Included(start) => Some(start.clone()),
            Bound::Excluded(start) => Some(start.clone()),
            Bound::Unbounded => None,
        };

        let end = match range.end_bound() {
            Bound::Included(end) => Some(end.clone()),
            Bound::Excluded(end) => Some(end.clone()),
            Bound::Unbounded => None,
        };

        let range_fn = move |k: &[u8]| {
            (start.as_ref().map(|s| k >= &s[..]).unwrap_or(true))
                && (end.as_ref().map(|e| k <= &e[..]).unwrap_or(true))
        };

        Self { iter, txn, range: Box::new(range_fn) }
    }
}

impl<'a> Iterator for LmdbScanIter<'a> {
	type Item = crate::Result<(Key, Value)>;

	fn next(&mut self) -> Option<Self::Item> {
		while let Some(result) = self.iter.next() {
			match result {
				Ok((k, v)) if (self.range)(k) => return Some(Ok((k.to_vec(), v.to_vec()))),
				Ok(_) => continue,
				// Err(e) => return Some(Err(Box::new(e))),
				Err(e) => panic!("unexpected error: {}", e),
			}
		}
		None
	}
}
