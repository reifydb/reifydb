// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use super::{build_range_query, execute_batched_range_query, table_name_for_range};
use crate::sqlite::Sqlite;
use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use reifydb_core::interface::{Versioned, VersionedScanRangeRev};
use reifydb_core::{EncodedKey, EncodedKeyRange, Result, Version};
use std::collections::VecDeque;
use std::ops::Bound;

impl VersionedScanRangeRev for Sqlite {
    type ScanRangeIterRev<'a> = RangeRev;

    fn scan_range_rev(
        &self,
        range: EncodedKeyRange,
        version: Version,
    ) -> Result<Self::ScanRangeIterRev<'_>> {
        Ok(RangeRev::new(self.get_conn(), range, version, 1024))
    }
}

pub struct RangeRev {
    conn: PooledConnection<SqliteConnectionManager>,
    range: EncodedKeyRange,
    version: Version,
    table: String,
    buffer: VecDeque<Versioned>,
    last_key: Option<EncodedKey>,
    batch_size: usize,
    exhausted: bool,
}

impl RangeRev {
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

        // For reverse iteration, we need to adjust the bounds differently
        // If we have a last_key, we want everything before it (exclusive)
        let end_bound = match &self.last_key {
            Some(k) => Bound::Excluded(k),
            None => self.range.end.as_ref(),
        };

        let start_bound = self.range.start.as_ref();

        // Build query and parameters based on bounds - note DESC order for reverse
        let (query_template, param_count) = build_range_query(start_bound, end_bound, "DESC");

        let query = query_template.replace("{}", &self.table);
        let mut stmt = self.conn.prepare(&query).unwrap();

        let count = execute_batched_range_query(
            &mut stmt,
            start_bound,
            end_bound,
            self.version,
            self.batch_size,
            param_count,
            &mut self.buffer,
        );

        // Update last_key to the last item we retrieved (which is the smallest key due to DESC order)
        if let Some(last_item) = self.buffer.back() {
            self.last_key = Some(last_item.key.clone());
        }

        // If we got fewer results than requested, we've reached the end
        if count < self.batch_size {
            self.exhausted = true;
        }
    }
}

impl Iterator for RangeRev {
    type Item = Versioned;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            self.refill_buffer();
        }
        self.buffer.pop_front()
    }
}
