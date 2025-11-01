// Copyright (c) reifydb.com 2025.
// This file is licensed under the AGPL-3.0-or-later, see license.md file.

use std::{collections::VecDeque, ops::Bound};

use reifydb_core::{CommitVersion, EncodedKey, EncodedKeyRange, Result};

use super::{build_range_query, execute_batched_range_query, operator_name_for_range, source_name_for_range};
use crate::backend::{
	diagnostic::database_error,
	multi::BackendMultiVersionRange,
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

impl BackendMultiVersionRange for SqliteBackend {
	type RangeIter<'a> = MultiVersionRangeIter;

	fn range_batched(
		&self,
		range: EncodedKeyRange,
		version: CommitVersion,
		batch_size: u64,
	) -> Result<Self::RangeIter<'_>> {
		println!("batch_size = {}", batch_size);
		Ok(MultiVersionRangeIter::new(self.get_reader(), range, version, batch_size as usize))
	}
}

pub struct MultiVersionRangeIter {
	reader: Reader,
	range: EncodedKeyRange,
	version: CommitVersion,
	source: String,
	buffer: VecDeque<MultiVersionIterResult>,
	last_key: Option<EncodedKey>,
	batch_size: usize,
	exhausted: bool,
}

impl MultiVersionRangeIter {
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

	fn refill_buffer(&mut self) -> Result<()> {
		if self.exhausted {
			return Ok(());
		}

		self.buffer.clear();

		// Determine the effective start bound for this batch
		let start_bound = match &self.last_key {
			Some(k) => Bound::Excluded(k),
			None => self.range.start.as_ref(),
		};

		let end_bound = self.range.end.as_ref();

		// Build query and parameters based on bounds - note ASC order
		// for forward iteration
		let (query_template, param_count) = build_range_query(start_bound, end_bound, "ASC");

		let query = query_template.replace("{}", &self.source);
		let conn_guard = self.reader.lock().map_err(|e| {
			use crate::backend::diagnostic::database_error;
			reifydb_type::Error(database_error(format!("Failed to acquire reader lock: {}", e)))
		})?;
		let mut stmt = conn_guard
			.prepare(&query)
			.map_err(|e| reifydb_type::Error(database_error(format!("Failed to prepare query: {}", e))))?;

		let count = execute_batched_range_query(
			&mut stmt,
			start_bound,
			end_bound,
			self.version,
			self.batch_size,
			param_count,
			&mut self.buffer,
		);

		// Update last_key to the last item we retrieved
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
		Ok(())
	}
}

impl Iterator for MultiVersionRangeIter {
	type Item = MultiVersionIterResult;

	fn next(&mut self) -> Option<Self::Item> {
		if self.buffer.is_empty() {
			if let Err(_) = self.refill_buffer() {
				return None;
			}
		}
		self.buffer.pop_front()
	}
}
