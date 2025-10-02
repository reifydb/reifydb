// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use std::{collections::VecDeque, ops::Bound};

use reifydb_core::{
	EncodedKey, EncodedKeyRange, Result,
	interface::{SingleVersionRange, SingleVersionValues},
};

use super::{build_single_query, execute_range_query};
use crate::backend::sqlite::{Sqlite, read::Reader};

impl SingleVersionRange for Sqlite {
	type Range<'a>
		= Range
	where
		Self: 'a;

	fn range(&self, range: EncodedKeyRange) -> Result<Self::Range<'_>> {
		Ok(Range::new(self.get_reader(), range, 1024))
	}
}

pub struct Range {
	conn: Reader,
	range: EncodedKeyRange,
	buffer: VecDeque<SingleVersionValues>,
	last_key: Option<EncodedKey>,
	batch_size: usize,
	exhausted: bool,
}

impl Range {
	pub fn new(conn: Reader, range: EncodedKeyRange, batch_size: usize) -> Self {
		Self {
			conn,
			range,
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

		// Determine the effective start bound for this batch
		let start_bound = match &self.last_key {
			Some(k) => Bound::Excluded(k),
			None => self.range.start.as_ref(),
		};

		let end_bound = self.range.end.as_ref();

		// Build query and parameters based on bounds - note ASC order
		// for forward iteration
		let (query_template, param_count) = build_single_query(start_bound, end_bound, "ASC");

		let conn_guard = self.conn.lock().unwrap();
		let mut stmt = conn_guard.prepare(query_template).unwrap();

		let count = execute_range_query(
			&mut stmt,
			start_bound,
			end_bound,
			self.batch_size,
			param_count,
			&mut self.buffer,
		);

		// Update last_key to the last item we retrieved
		if let Some(last_item) = self.buffer.back() {
			self.last_key = Some(last_item.key.clone());
		}

		// If we got fewer results than requested, we've reached the end
		if count < self.batch_size {
			self.exhausted = true;
		}
	}
}

impl Iterator for Range {
	type Item = SingleVersionValues;

	fn next(&mut self) -> Option<Self::Item> {
		if self.buffer.is_empty() {
			self.refill_buffer();
		}
		self.buffer.pop_front()
	}
}
