// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::cdc::codec::decode_cdc_event;
use crate::sqlite::Sqlite;
use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use reifydb_core::interface::{CdcEvent, CdcRange};
use reifydb_core::row::EncodedRow;
use reifydb_core::{CowVec, Result, Version};
use std::collections::VecDeque;
use std::ops::Bound;

impl CdcRange for Sqlite {
    type RangeIter<'a> = Range;

    fn range(&self, start: Bound<Version>, end: Bound<Version>) -> Result<Self::RangeIter<'_>> {
        Ok(Range::new(self.get_conn(), start, end, 1024))
    }
}

pub struct Range {
    conn: PooledConnection<SqliteConnectionManager>,
    start: Bound<Version>,
    end: Bound<Version>,
    buffer: VecDeque<CdcEvent>,
    last_version: Option<Version>,
    last_sequence: Option<u16>,
    batch_size: usize,
    exhausted: bool,
}

impl Range {
    pub fn new(
        conn: PooledConnection<SqliteConnectionManager>,
        start: Bound<Version>,
        end: Bound<Version>,
        batch_size: usize,
    ) -> Self {
        Self {
            conn,
            start,
            end,
            buffer: VecDeque::new(),
            last_version: None,
            last_sequence: None,
            batch_size,
            exhausted: false,
        }
    }

    fn refill_buffer(&mut self) {
        if self.exhausted {
            return;
        }

        self.buffer.clear();

        // Build the WHERE clause based on bounds
        let (where_clause, params) = self.build_query_and_params();

        let query = if where_clause.is_empty() {
            "SELECT value FROM cdc ORDER BY version ASC, key DESC LIMIT ?".to_string()
        } else {
            format!("SELECT value FROM cdc {} ORDER BY version ASC, key DESC LIMIT ?", where_clause)
        };

        let mut stmt = self.conn.prepare_cached(&query).unwrap();
        
        let mut query_params = params;
        query_params.push(self.batch_size as i64);
        
        let events: Vec<EncodedRow> = stmt
            .query_map(rusqlite::params_from_iter(query_params), |row| {
                let bytes: Vec<u8> = row.get(0)?;
                Ok(EncodedRow(CowVec::new(bytes)))
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();

        let count = events.len();
        
        for encoded in events {
            if let Ok(event) = decode_cdc_event(&encoded) {
                self.last_version = Some(event.version);
                self.last_sequence = Some(event.sequence);
                self.buffer.push_back(event);
            }
        }

        // If we got fewer results than requested, we've reached the end
        if count < self.batch_size {
            self.exhausted = true;
        }
    }
    
    fn build_query_and_params(&self) -> (String, Vec<i64>) {
        let mut conditions = Vec::new();
        let mut params = Vec::new();
        
        // Handle version bounds
        match &self.start {
            Bound::Included(v) => {
                conditions.push("version >= ?".to_string());
                params.push(*v as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("version > ?".to_string());
                params.push(*v as i64);
            }
            Bound::Unbounded => {}
        }
        
        match &self.end {
            Bound::Included(v) => {
                conditions.push("version <= ?".to_string());
                params.push(*v as i64);
            }
            Bound::Excluded(v) => {
                conditions.push("version < ?".to_string());
                params.push(*v as i64);
            }
            Bound::Unbounded => {}
        }
        
        // Handle pagination with last position
        if let (Some(last_version), Some(last_sequence)) = (self.last_version, self.last_sequence) {
            conditions.push("(version > ? OR (version = ? AND sequence > ?))".to_string());
            params.push(last_version as i64);
            params.push(last_version as i64);
            params.push(last_sequence as i64);
        }
        
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", conditions.join(" AND "))
        };
        
        (where_clause, params)
    }
}

impl Iterator for Range {
    type Item = CdcEvent;

    fn next(&mut self) -> Option<Self::Item> {
        if self.buffer.is_empty() {
            self.refill_buffer();
        }
        self.buffer.pop_front()
    }
}