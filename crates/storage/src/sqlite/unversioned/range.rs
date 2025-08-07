// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::{build_unversioned_query, execute_range_query};
use crate::sqlite::Sqlite;
use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use reifydb_core::interface::{Unversioned, UnversionedScanRange};
use reifydb_core::{EncodedKey, EncodedKeyRange, Result};
use std::collections::VecDeque;
use std::ops::Bound;

impl UnversionedScanRange for Sqlite {
    type ScanRange<'a>
        = Range
    where
        Self: 'a;

    fn range(&self, range: EncodedKeyRange) -> Result<Self::ScanRange<'_>> {
        Ok(Range::new(self.get_conn(), range, 1024))
    }
}

pub struct Range {
    conn: PooledConnection<SqliteConnectionManager>,
    range: EncodedKeyRange,
    buffer: VecDeque<Unversioned>,
    last_key: Option<EncodedKey>,
    batch_size: usize,
    exhausted: bool,
}

impl Range {
    pub fn new(
        conn: PooledConnection<SqliteConnectionManager>,
        range: EncodedKeyRange,
        batch_size: usize,
    ) -> Self {
        Self { conn, range, buffer: VecDeque::new(), last_key: None, batch_size, exhausted: false }
    }

    fn refill_buffer(&mut self) {
        if self.exhausted {
            return;
        }

        self.buffer.clear();

        // Determine the effective start bound for this batch
        let start_bound = match &self.last_key {
            Some(k) => Bound::Excluded(k),
            None => self.range.start.as_ref(),
        };

        let end_bound = self.range.end.as_ref();

        // Build query and parameters based on bounds - note ASC order for forward iteration
        let (query_template, param_count) = build_unversioned_query(start_bound, end_bound, "ASC");

        let mut stmt = self.conn.prepare(query_template).unwrap();

        let count = execute_range_query(
            &mut stmt,
            start_bound,
            end_bound,
            self.batch_size,
            param_count,
            &mut self.buffer,
        );

        // Update last_key to the last item we retrieved
        if let Some(last_item) = self.buffer.back() {
            self.last_key = Some(last_item.key.clone());
        }

        // If we got fewer results than requested, we've reached the end
        if count < self.batch_size {
            self.exhausted = true;
        }
    }
}

impl Iterator for Range {
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            self.refill_buffer();
        }
        self.buffer.pop_front()
    }
}
