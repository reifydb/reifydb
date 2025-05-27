// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later

use crate::{Key, Value};
use heed::types::Bytes;
use heed::{Database, Env};
use std::collections::{Bound, VecDeque};
use std::sync::Arc;

pub struct LmdbScanIter {
    env: Arc<Env>,
    db: Database<Bytes, Bytes>,
    buffer: VecDeque<crate::Result<(Key, Value)>>,
    last_key: Option<Key>,
    batch_size: usize,
}

impl LmdbScanIter {
    pub fn new(env: Arc<Env>, db: Database<Bytes, Bytes>, batch_size: usize) -> Self {
        Self { env, db, buffer: VecDeque::new(), last_key: None, batch_size }
    }

    fn refill_buffer(&mut self) -> crate::Result<()> {
        let txn = self.env.read_txn().unwrap();

        let start_bound = match &self.last_key {
            Some(key) => Bound::Excluded(&key[..]),
            None => Bound::Unbounded,
        };

        let range = (start_bound, Bound::Unbounded);
        let iter = self.db.range(&txn, &range).unwrap();

        self.buffer.clear();

        for result in iter.take(self.batch_size) {
            match result {
                Ok((k, v)) => {
                    self.last_key = Some(k.to_vec());
                    self.buffer.push_back(Ok((k.to_vec(), v.to_vec())));
                }
                Err(e) => {
                    // self.buffer.push_back(Err(e));
                    panic!("Failed to refill the batch: {}", e);
                    break;
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
