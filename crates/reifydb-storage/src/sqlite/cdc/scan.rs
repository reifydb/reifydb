// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::VecDeque;

use r2d2::PooledConnection;
use r2d2_sqlite::SqliteConnectionManager;
use reifydb_core::{
	CowVec, Result, Version,
	interface::{CdcEvent, CdcScan},
	row::EncodedRow,
};

use crate::{cdc::codec::decode_cdc_event, sqlite::Sqlite};

impl CdcScan for Sqlite {
	type ScanIter<'a> = Scan;

	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		Ok(Scan::new(self.get_conn(), 1024))
	}
}

pub struct Scan {
	conn: PooledConnection<SqliteConnectionManager>,
	buffer: VecDeque<CdcEvent>,
	last_version: Option<Version>,
	last_sequence: Option<u16>,
	batch_size: usize,
	exhausted: bool,
}

impl Scan {
	pub fn new(
		conn: PooledConnection<SqliteConnectionManager>,
		batch_size: usize,
	) -> Self {
		Self {
			conn,
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

		let (where_clause, params) = if let (
			Some(last_version),
			Some(last_sequence),
		) =
			(self.last_version, self.last_sequence)
		{
			(
                "WHERE (version > ? OR (version = ? AND sequence > ?))".to_string(),
                vec![last_version as i64, last_version as i64, last_sequence as i64]
            )
		} else {
			(String::new(), vec![])
		};

		let query = if where_clause.is_empty() {
			"SELECT value FROM cdc ORDER BY version ASC, sequence ASC LIMIT ?".to_string()
		} else {
			format!(
				"SELECT value FROM cdc {} ORDER BY version ASC, sequence ASC LIMIT ?",
				where_clause
			)
		};

		let mut stmt = self.conn.prepare_cached(&query).unwrap();

		let mut query_params = params;
		query_params.push(self.batch_size as i64);

		let events: Vec<EncodedRow> = stmt
			.query_map(
				rusqlite::params_from_iter(query_params),
				|row| {
					let bytes: Vec<u8> = row.get(0)?;
					Ok(EncodedRow(CowVec::new(bytes)))
				},
			)
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
}

impl Iterator for Scan {
	type Item = CdcEvent;

	fn next(&mut self) -> Option<Self::Item> {
		if self.buffer.is_empty() {
			self.refill_buffer();
		}
		self.buffer.pop_front()
	}
}
