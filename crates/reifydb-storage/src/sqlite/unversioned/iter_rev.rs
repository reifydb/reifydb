// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use super::execute_iter_query;
use crate::sqlite::Sqlite;
use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use reifydb_core::EncodedKey;
use reifydb_core::Result;
use reifydb_core::interface::{Unversioned, UnversionedScanRev};
use std::collections::VecDeque;

impl UnversionedScanRev for Sqlite {
    type ScanIterRev<'a> = IterRev;

    fn scan_rev(&self) -> Result<Self::ScanIterRev<'_>> {
        Ok(IterRev::new(self.get_conn(), 1024))
    }
}

pub struct IterRev {
    conn: PooledConnection<SqliteConnectionManager>,
    buffer: VecDeque<Unversioned>,
    last_key: Option<EncodedKey>,
    batch_size: usize,
    exhausted: bool,
}

impl IterRev {
    pub fn new(conn: PooledConnection<SqliteConnectionManager>, batch_size: usize) -> Self {
        Self { conn, buffer: VecDeque::new(), last_key: None, batch_size, exhausted: false }
    }

    fn refill_buffer(&mut self) {
        if self.exhausted {
            return;
        }

        self.buffer.clear();

        let count = execute_iter_query(
            &self.conn,
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
    type Item = Unversioned;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            self.refill_buffer();
        }
        self.buffer.pop_front()
    }
}
