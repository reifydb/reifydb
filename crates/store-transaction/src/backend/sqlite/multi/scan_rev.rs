// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::collections::VecDeque;

use reifydb_core::{CommitVersion, EncodedKey, Result};

use super::{execute_scan_query, get_source_names};
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
	source_names: Vec<String>,
	buffer: VecDeque<MultiVersionIterResult>,
	last_key: Option<EncodedKey>,
	batch_size: usize,
	exhausted: bool,
	items_returned: usize,
}

impl MultiVersionScanRevIter {
	pub fn new(reader: Reader, version: CommitVersion, batch_size: usize) -> Self {
		let source_names = get_source_names(&reader);

		Self {
			reader,
			version,
			source_names,
			buffer: VecDeque::new(),
			last_key: None,
			batch_size,
			exhausted: false,
			items_returned: 0,
		}
	}

	fn refill_buffer(&mut self) {
		if self.exhausted {
			return;
		}

		self.buffer.clear();

		let count = execute_scan_query(
			&self.reader,
			&self.source_names,
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
		// Check if we've already returned enough items
		if self.items_returned >= self.batch_size {
			return None;
		}

		if self.buffer.is_empty() {
			self.refill_buffer();
		}
		let item = self.buffer.pop_front();
		if item.is_some() {
			self.items_returned += 1;
		}
		item
	}
}
