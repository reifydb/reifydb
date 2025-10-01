// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::VecDeque;

use reifydb_core::{
	CommitVersion, CowVec, Result,
	interface::{Cdc, CdcScan},
	value::row::EncodedRow,
};

use crate::{
	backend::sqlite::{Sqlite, read::Reader},
	cdc::codec::decode_cdc_transaction,
};

impl CdcScan for Sqlite {
	type ScanIter<'a> = Scan;

	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		Ok(Scan::new(self.get_reader(), 1024))
	}
}

pub struct Scan {
	conn: Reader,
	buffer: VecDeque<Cdc>,
	last_version: Option<CommitVersion>,
	batch_size: usize,
	exhausted: bool,
}

impl Scan {
	pub fn new(conn: Reader, batch_size: usize) -> Self {
		Self {
			conn,
			buffer: VecDeque::new(),
			last_version: None,
			batch_size,
			exhausted: false,
		}
	}

	fn refill_buffer(&mut self) {
		if self.exhausted {
			return;
		}

		self.buffer.clear();

		let (where_clause, params) = if let Some(last_version) = self.last_version {
			("WHERE version > ?".to_string(), vec![last_version as i64])
		} else {
			(String::new(), vec![])
		};

		let query = if where_clause.is_empty() {
			"SELECT version, value FROM cdc ORDER BY version ASC LIMIT ?".to_string()
		} else {
			format!("SELECT version, value FROM cdc {} ORDER BY version ASC LIMIT ?", where_clause)
		};

		let conn_guard = self.conn.lock().unwrap();
		let mut stmt = conn_guard.prepare_cached(&query).unwrap();

		let mut query_params = params;
		query_params.push(self.batch_size as i64);

		let transactions: Vec<(CommitVersion, EncodedRow)> = stmt
			.query_map(rusqlite::params_from_iter(query_params), |row| {
				let version: i64 = row.get(0)?;
				let bytes: Vec<u8> = row.get(1)?;
				Ok((version as CommitVersion, EncodedRow(CowVec::new(bytes))))
			})
			.unwrap()
			.collect::<rusqlite::Result<Vec<_>>>()
			.unwrap();

		let count = transactions.len();

		for (version, encoded_transaction) in transactions {
			if let Ok(txn) = decode_cdc_transaction(&encoded_transaction) {
				self.last_version = Some(version);
				// Add the transaction to the buffer
				self.buffer.push_back(txn);
			}
		}

		// If we got fewer results than requested, we've reached the end
		if count < self.batch_size {
			self.exhausted = true;
		}
	}
}

impl Iterator for Scan {
	type Item = Cdc;

	fn next(&mut self) -> Option<Self::Item> {
		if self.buffer.is_empty() {
			self.refill_buffer();
		}
		self.buffer.pop_front()
	}
}
