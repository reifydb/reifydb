// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::VecDeque, ops::Bound};

use reifydb_core::{
	CommitVersion, CowVec, Result,
	interface::{Cdc, CdcRange},
	value::row::EncodedRow,
};

use crate::{
	cdc::codec::decode_cdc_transaction,
	sqlite::{Sqlite, read::Reader},
};

impl CdcRange for Sqlite {
	type RangeIter<'a> = Range;

	fn range(&self, start: Bound<CommitVersion>, end: Bound<CommitVersion>) -> Result<Self::RangeIter<'_>> {
		Ok(Range::new(self.get_reader(), start, end, 1024))
	}
}

pub struct Range {
	conn: Reader,
	start: Bound<CommitVersion>,
	end: Bound<CommitVersion>,
	buffer: VecDeque<Cdc>,
	last_version: Option<CommitVersion>,
	batch_size: usize,
	exhausted: bool,
}

impl Range {
	pub fn new(conn: Reader, start: Bound<CommitVersion>, end: Bound<CommitVersion>, batch_size: usize) -> Self {
		Self {
			conn,
			start,
			end,
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

		// Build the WHERE clause based on bounds
		let (where_clause, params) = self.build_query_and_params();

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

		// Handle pagination with last version (simplified for
		// transactions)
		if let Some(last_version) = self.last_version {
			conditions.push("version > ?".to_string());
			params.push(last_version as i64);
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
	type Item = Cdc;

	fn next(&mut self) -> Option<Self::Item> {
		if self.buffer.is_empty() {
			self.refill_buffer();
		}
		self.buffer.pop_front()
	}
}
