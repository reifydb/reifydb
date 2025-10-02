// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::collections::VecDeque;

use reifydb_core::{CommitVersion, EncodedKey, Result};

use super::{execute_scan_query, get_table_names};
use crate::backend::{
	multi::BackendMultiVersionScanRev,
	result::MultiVersionIterResult,
	sqlite::{SqliteBackend, read::Reader},
};

impl BackendMultiVersionScanRev for SqliteBackend {
	type ScanIterRev<'a> = MultiVersionScanRevIter;

	fn scan_rev(&self, version: CommitVersion) -> Result<Self::ScanIterRev<'_>> {
		Ok(MultiVersionScanRevIter::new(self.get_reader(), version, 1024))
	}
}

pub struct MultiVersionScanRevIter {
	reader: Reader,
	version: CommitVersion,
	table_names: Vec<String>,
	buffer: VecDeque<MultiVersionIterResult>,
	last_key: Option<EncodedKey>,
	batch_size: usize,
	exhausted: bool,
}

impl MultiVersionScanRevIter {
	pub fn new(reader: Reader, version: CommitVersion, batch_size: usize) -> Self {
		let table_names = get_table_names(&reader);

		Self {
			reader,
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

		let count = execute_scan_query(
			&self.reader,
			&self.table_names,
			self.version,
			self.batch_size,
			self.last_key.as_ref(),
			"DESC",
			&mut self.buffer,
		);

		// Update last_key to the last item we retrieved (which is the
		// smallest key due to DESC order)
		if let Some(last_item) = self.buffer.back() {
			self.last_key = Some(match last_item {
				MultiVersionIterResult::Value(v) => v.key.clone(),
				MultiVersionIterResult::Tombstone {
					key,
					..
				} => key.clone(),
			});
		}

		// If we got fewer results than requested, we've reached the end
		if count < self.batch_size {
			self.exhausted = true;
		}
	}
}

impl Iterator for MultiVersionScanRevIter {
	type Item = MultiVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		if self.buffer.is_empty() {
			self.refill_buffer();
		}
		self.buffer.pop_front()
	}
}
