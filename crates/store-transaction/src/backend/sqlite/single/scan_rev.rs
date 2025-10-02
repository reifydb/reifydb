// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::collections::VecDeque;

use reifydb_core::{EncodedKey, Result};

use super::execute_scan_query;
use crate::backend::{
	result::SingleVersionIterResult,
	single::BackendSingleVersionScanRev,
	sqlite::{SqliteBackend, read::Reader},
};

impl BackendSingleVersionScanRev for SqliteBackend {
	type ScanIterRev<'a> = SingleVersionScanRevIter;

	fn scan_rev(&self) -> Result<Self::ScanIterRev<'_>> {
		Ok(SingleVersionScanRevIter::new(self.get_reader(), 1024))
	}
}

pub struct SingleVersionScanRevIter {
	reader: Reader,
	buffer: VecDeque<SingleVersionIterResult>,
	last_key: Option<EncodedKey>,
	batch_size: usize,
	exhausted: bool,
}

impl SingleVersionScanRevIter {
	pub fn new(reader: Reader, batch_size: usize) -> Self {
		Self {
			reader,
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
			self.batch_size,
			self.last_key.as_ref(),
			"DESC",
			&mut self.buffer,
		);

		// Update last_key to the last item we retrieved (which is the
		// smallest key due to DESC order)
		if let Some(last_item) = self.buffer.back() {
			self.last_key = Some(last_item.key().clone());
		}

		// If we got fewer results than requested, we've reached the end
		if count < self.batch_size {
			self.exhausted = true;
		}
	}
}

impl Iterator for SingleVersionScanRevIter {
	type Item = SingleVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		if self.buffer.is_empty() {
			self.refill_buffer();
		}
		self.buffer.pop_front()
	}
}
