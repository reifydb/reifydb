// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::VecDeque, sync::Arc};

use reifydb_core::{CommitVersion, CowVec, Result, interface::Cdc, value::encoded::EncodedValues};

use crate::{
	CdcScan,
	backend::sqlite::{SqliteBackend, read::Reader},
	cdc::{codec::decode_internal_cdc, converter::CdcConverter},
};

impl CdcScan for SqliteBackend {
	type ScanIter<'a> = CdcScanIter;

	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		Ok(CdcScanIter::new(Arc::new(self.clone()), self.get_reader(), 1024))
	}
}

pub struct CdcScanIter {
	backend: Arc<SqliteBackend>,
	reader: Reader,
	buffer: VecDeque<Cdc>,
	last_version: Option<CommitVersion>,
	batch_size: u64,
	exhausted: bool,
}

impl CdcScanIter {
	pub fn new(backend: Arc<SqliteBackend>, reader: Reader, batch_size: u64) -> Self {
		Self {
			backend,
			reader,
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
			("WHERE version > ?".to_string(), vec![last_version.0])
		} else {
			(String::new(), vec![])
		};

		let query = if where_clause.is_empty() {
			"SELECT version, value FROM cdc ORDER BY version ASC LIMIT ?".to_string()
		} else {
			format!("SELECT version, value FROM cdc {} ORDER BY version ASC LIMIT ?", where_clause)
		};

		let conn_guard = match self.reader.lock() {
			Ok(guard) => guard,
			Err(_) => {
				// Lock poisoned, mark as exhausted and return
				self.exhausted = true;
				return;
			}
		};

		let mut stmt = match conn_guard.prepare_cached(&query) {
			Ok(stmt) => stmt,
			Err(_) => {
				// Failed to prepare statement (possibly due to database shutdown)
				self.exhausted = true;
				return;
			}
		};

		let mut query_params = params;
		query_params.push(self.batch_size);

		let query_result = stmt.query_map(rusqlite::params_from_iter(query_params), |values| {
			let version = CommitVersion(values.get(0)?);
			let bytes: Vec<u8> = values.get(1)?;
			Ok((version, EncodedValues(CowVec::new(bytes))))
		});

		let transactions: Vec<(CommitVersion, EncodedValues)> = match query_result {
			Ok(rows) => match rows.collect::<rusqlite::Result<Vec<_>>>() {
				Ok(txns) => txns,
				Err(_) => {
					// Query execution failed (possibly database locked during shutdown)
					self.exhausted = true;
					return;
				}
			},
			Err(_) => {
				// Query failed to execute
				self.exhausted = true;
				return;
			}
		};

		let count = transactions.len();

		for (version, encoded_transaction) in transactions {
			if let Ok(internal_cdc) = decode_internal_cdc(&encoded_transaction) {
				self.last_version = Some(version);

				// Convert to public CDC using the converter
				if let Ok(cdc) = self.backend.convert(internal_cdc) {
					self.buffer.push_back(cdc);
				}
			}
		}

		// If we got fewer results than requested, we've reached the end
		if count < self.batch_size as usize {
			self.exhausted = true;
		}
	}
}

impl Iterator for CdcScanIter {
	type Item = Cdc;

	fn next(&mut self) -> Option<Self::Item> {
		if self.buffer.is_empty() {
			self.refill_buffer();
		}
		self.buffer.pop_front()
	}
}
