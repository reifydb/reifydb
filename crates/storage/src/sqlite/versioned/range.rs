// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use crate::sqlite::Sqlite;
use super::{build_range_query, execute_batched_range_query, table_name_for_range};
use reifydb_core::interface::{Versioned, VersionedScanRange};
use reifydb_core::{EncodedKey, EncodedKeyRange, Version};
use r2d2::{PooledConnection};
use r2d2_sqlite::SqliteConnectionManager;
use std::collections::VecDeque;
use std::ops::Bound;

impl VersionedScanRange for Sqlite {
    type ScanRangeIter<'a> = Range;

    fn scan_range(&self, range: EncodedKeyRange, version: Version) -> Self::ScanRangeIter<'_> {
        Range::new(self.get_conn(), range, version, 1024)
    }
}

pub struct Range {
    conn: PooledConnection<SqliteConnectionManager>,
    range: EncodedKeyRange,
    version: Version,
    table: String,
    buffer: VecDeque<Versioned>,
    last_key: Option<EncodedKey>,
    batch_size: usize,
    exhausted: bool,
}

impl Range {
    pub fn new(
        conn: PooledConnection<SqliteConnectionManager>,
        range: EncodedKeyRange,
        version: Version,
        batch_size: usize,
    ) -> Self {
        let table = table_name_for_range(&range).to_string();
        
        Self {
            conn,
            range,
            version,
            table,
            buffer: VecDeque::new(),
            last_key: None,
            batch_size,
            exhausted: false,
        }
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
        let (query_template, param_count) = build_range_query(start_bound, end_bound, "ASC");

        let query = query_template.replace("{}", &self.table);
        let mut stmt = self.conn.prepare(&query).unwrap();
        
        let count = execute_batched_range_query(
            &mut stmt, 
            start_bound, 
            end_bound, 
            self.version, 
            self.batch_size, 
            param_count, 
            &mut self.buffer
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
    type Item = Versioned;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            self.refill_buffer();
        }
        self.buffer.pop_front()
    }
}