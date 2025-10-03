// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::VecDeque;

use reifydb_core::{CommitVersion, CowVec, Result, interface::Cdc, value::encoded::EncodedValues};

use crate::{
	CdcScan,
	backend::sqlite::{SqliteBackend, read::Reader},
	cdc::codec::decode_cdc_transaction,
};

impl CdcScan for SqliteBackend {
	type ScanIter<'a> = CdcScanIter;

	fn scan(&self) -> Result<Self::ScanIter<'_>> {
		Ok(CdcScanIter::new(self.get_reader(), 1024))
	}
}

pub struct CdcScanIter {
	reader: Reader,
	buffer: VecDeque<Cdc>,
	last_version: Option<CommitVersion>,
	batch_size: u64,
	exhausted: bool,
}

impl CdcScanIter {
	pub fn new(reader: Reader, batch_size: u64) -> Self {
		Self {
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

		let conn_guard = self.reader.lock().unwrap();
		let mut stmt = conn_guard.prepare_cached(&query).unwrap();

		let mut query_params = params;
		query_params.push(self.batch_size);

		let transactions: Vec<(CommitVersion, EncodedValues)> = stmt
			.query_map(rusqlite::params_from_iter(query_params), |values| {
				let version = CommitVersion(values.get(0)?);
				let bytes: Vec<u8> = values.get(1)?;
				Ok((version, EncodedValues(CowVec::new(bytes))))
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
