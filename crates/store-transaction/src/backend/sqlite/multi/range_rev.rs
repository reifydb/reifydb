// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::{collections::VecDeque, ops::Bound};

use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange, Result};

use super::{build_range_query, execute_batched_range_query, operator_name_for_range, source_name_for_range};
use crate::backend::{
	multi::BackendMultiVersionRangeRev,
	result::MultiVersionIterResult,
	sqlite::{SqliteBackend, read::Reader},
};

/// Helper function to get the appropriate table name for a given range
fn get_table_name_for_range(range: &EncodedKeyRange) -> String {
	// Try operator_name_for_range first (for FlowNodeStateKeyRange)
	let operator_table = operator_name_for_range(range);
	if operator_table != "multi" {
		return operator_table;
	}
	// Otherwise use source_name_for_range (for RowKeyRange or multi)
	source_name_for_range(range)
}

impl BackendMultiVersionRangeRev for SqliteBackend {
	type RangeIterRev<'a> = MultiVersionRangeRevIter;

	fn range_rev(&self, range: EncodedKeyRange, version: CommitVersion) -> Result<Self::RangeIterRev<'_>> {
		Ok(MultiVersionRangeRevIter::new(self.get_reader(), range, version, 1024))
	}
}

pub struct MultiVersionRangeRevIter {
	reader: Reader,
	range: EncodedKeyRange,
	version: CommitVersion,
	source: String,
	buffer: VecDeque<MultiVersionIterResult>,
	last_key: Option<EncodedKey>,
	batch_size: usize,
	exhausted: bool,
}

impl MultiVersionRangeRevIter {
	pub fn new(reader: Reader, range: EncodedKeyRange, version: CommitVersion, batch_size: usize) -> Self {
		let source = get_table_name_for_range(&range);

		Self {
			reader,
			range,
			version,
			source,
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

		let query = query_template.replace("{}", &self.source);
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

impl Iterator for MultiVersionRangeRevIter {
	type Item = MultiVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		if self.buffer.is_empty() {
			self.refill_buffer();
		}
		self.buffer.pop_front()
	}
}
