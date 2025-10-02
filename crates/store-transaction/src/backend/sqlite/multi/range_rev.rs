// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::{collections::VecDeque, ops::Bound};

use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange, Result, interface::MultiVersionValues};

use super::{build_range_query, execute_batched_range_query, table_name_for_range};
use crate::{
	MultiVersionRangeRev,
	backend::sqlite::{SqliteBackend, read::Reader},
};

impl MultiVersionRangeRev for SqliteBackend {
	type RangeIterRev<'a> = MultiVersionRangeRevIter;

	fn range_rev(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIterRev<'_>> {
		Ok(MultiVersionRangeRevIter::new(self.get_reader(), range, version, 1024))
	}
}

pub struct MultiVersionRangeRevIter {
	reader: Reader,
	range: EncodedKeyRange,
	version: CommitVersion,
	table: String,
	buffer: VecDeque<MultiVersionValues>,
	last_key: Option<EncodedKey>,
	batch_size: usize,
	exhausted: bool,
}

impl MultiVersionRangeRevIter {
	pub fn new(reader: Reader, range: EncodedKeyRange, version: CommitVersion, batch_size: usize) -> Self {
		let table = table_name_for_range(&range).to_string();

		Self {
			reader,
			range,
			version,
			table,
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

		// For reverse iteration, we need to adjust the bounds
		// differently If we have a last_key, we want everything
		// before it (exclusive)
		let end_bound = match &self.last_key {
			Some(k) => Bound::Excluded(k),
			None => self.range.end.as_ref(),
		};

		let start_bound = self.range.start.as_ref();

		// Build query and parameters based on bounds - note DESC order
		// for reverse
		let (query_template, param_count) = build_range_query(start_bound, end_bound, "DESC");

		let query = query_template.replace("{}", &self.table);
		let conn_guard = self.reader.lock().unwrap();
		let mut stmt = conn_guard.prepare(&query).unwrap();

		let count = execute_batched_range_query(
			&mut stmt,
			start_bound,
			end_bound,
			self.version,
			self.batch_size,
			param_count,
			&mut self.buffer,
		);

		// Update last_key to the last item we retrieved (which is the
		// smallest key due to DESC order)
		if let Some(last_item) = self.buffer.back() {
			self.last_key = Some(last_item.key.clone());
		}

		// If we got fewer results than requested, we've reached the end
		if count < self.batch_size {
			self.exhausted = true;
		}
	}
}

impl Iterator for MultiVersionRangeRevIter {
	type Item = MultiVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		if self.buffer.is_empty() {
			self.refill_buffer();
		}
		self.buffer.pop_front()
	}
}
