// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use super::{execute_iter_query, get_table_names};
use crate::sqlite::Sqlite;
use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use reifydb_core::interface::{Versioned, VersionedScanRev};
use reifydb_core::{EncodedKey, Result, Version};
use std::collections::VecDeque;

impl VersionedScanRev for Sqlite {
    type ScanIterRev<'a> = IterRev;

    fn scan_rev(&self, version: Version) -> Result<Self::ScanIterRev<'_>> {
        Ok(IterRev::new(self.get_conn(), version, 1024))
    }
}

pub struct IterRev {
    conn: PooledConnection<SqliteConnectionManager>,
    version: Version,
    table_names: Vec<String>,
    buffer: VecDeque<Versioned>,
    last_key: Option<EncodedKey>,
    batch_size: usize,
    exhausted: bool,
}

impl IterRev {
    pub fn new(
        conn: PooledConnection<SqliteConnectionManager>,
        version: Version,
        batch_size: usize,
    ) -> Self {
        let table_names = get_table_names(&conn);

        Self {
            conn,
            version,
            table_names,
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

        let count = execute_iter_query(
            &self.conn,
            &self.table_names,
            self.version,
            self.batch_size,
            self.last_key.as_ref(),
            "DESC",
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

impl Iterator for IterRev {
    type Item = Versioned;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            self.refill_buffer();
        }
        self.buffer.pop_front()
    }
}
