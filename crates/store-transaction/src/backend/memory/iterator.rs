// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Iterators for memory backend range queries.

use std::{ops::Bound, sync::Arc};

use parking_lot::RwLock;
use reifydb_type::Result;

use super::tables::Tables;
use crate::backend::primitive::{RawEntry, TableId};

/// Forward range iterator for memory storage
pub struct MemoryRangeIter {
	pub(super) tables: Arc<RwLock<Tables>>,
	pub(super) table: TableId,
	pub(super) end: Bound<Vec<u8>>,
	pub(super) batch_size: usize,
	pub(super) buffer: Vec<RawEntry>,
	pub(super) pos: usize,
	pub(super) exhausted: bool,
}

impl MemoryRangeIter {
	/// Load initial batch with the given start bound.
	pub(super) fn load_initial(&mut self, start: Bound<&[u8]>) {
		self.load_batch(start);
	}

	fn load_batch(&mut self, start: Bound<&[u8]>) {
		let end_ref = match &self.end {
			Bound::Included(v) => Bound::Included(v.as_slice()),
			Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
			Bound::Unbounded => Bound::Unbounded,
		};

		let range_bounds = make_range_bounds(start, end_ref);

		let tables = self.tables.read();
		if let Some(table_data) = tables.get_table(self.table) {
			self.buffer = table_data
				.range::<Vec<u8>, _>(range_bounds)
				.take(self.batch_size)
				.map(|(k, v)| RawEntry {
					key: k.clone(),
					value: v.clone(),
				})
				.collect();
		} else {
			self.buffer = Vec::new();
		}

		self.pos = 0;
		// Only set exhausted when we get zero entries - a partial batch might
		// not mean we're done (e.g., with tombstones or edge cases)
		self.exhausted = self.buffer.is_empty();
	}

	fn refill(&mut self) {
		// Clone the last key to avoid borrow conflicts
		let last_key = self.buffer.last().map(|e| e.key.clone());

		let start = match &last_key {
			Some(key) => Bound::Excluded(key.as_slice()),
			None => {
				// No previous entries means we're done
				self.exhausted = true;
				return;
			}
		};

		self.load_batch(start);
	}
}

impl Iterator for MemoryRangeIter {
	type Item = Result<RawEntry>;

	fn next(&mut self) -> Option<Self::Item> {
		// If buffer exhausted and not done, refill
		if self.pos >= self.buffer.len() && !self.exhausted {
			self.refill();
		}

		if self.pos < self.buffer.len() {
			let entry = self.buffer[self.pos].clone();
			self.pos += 1;
			Some(Ok(entry))
		} else {
			None
		}
	}
}

/// Reverse range iterator for memory storage
pub struct MemoryRangeRevIter {
	pub(super) tables: Arc<RwLock<Tables>>,
	pub(super) table: TableId,
	pub(super) start: Bound<Vec<u8>>,
	pub(super) batch_size: usize,
	pub(super) buffer: Vec<RawEntry>,
	pub(super) pos: usize,
	pub(super) exhausted: bool,
}

impl MemoryRangeRevIter {
	/// Load initial batch with the given end bound.
	pub(super) fn load_initial(&mut self, end: Bound<&[u8]>) {
		self.load_batch(end);
	}

	fn load_batch(&mut self, end: Bound<&[u8]>) {
		let start_ref = match &self.start {
			Bound::Included(v) => Bound::Included(v.as_slice()),
			Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
			Bound::Unbounded => Bound::Unbounded,
		};

		let range_bounds = make_range_bounds(start_ref, end);

		let tables = self.tables.read();
		if let Some(table_data) = tables.get_table(self.table) {
			self.buffer = table_data
				.range::<Vec<u8>, _>(range_bounds)
				.rev()
				.take(self.batch_size)
				.map(|(k, v)| RawEntry {
					key: k.clone(),
					value: v.clone(),
				})
				.collect();
		} else {
			self.buffer = Vec::new();
		}

		self.pos = 0;
		// Only set exhausted when we get zero entries - a partial batch might
		// not mean we're done (e.g., with tombstones or edge cases)
		self.exhausted = self.buffer.is_empty();
	}

	fn refill(&mut self) {
		// Clone the last key to avoid borrow conflicts
		let last_key = self.buffer.last().map(|e| e.key.clone());

		// For reverse iteration, continue from before the last key
		let end = match &last_key {
			Some(key) => Bound::Excluded(key.as_slice()),
			None => {
				// No previous entries means we're done
				self.exhausted = true;
				return;
			}
		};

		self.load_batch(end);
	}
}

impl Iterator for MemoryRangeRevIter {
	type Item = Result<RawEntry>;

	fn next(&mut self) -> Option<Self::Item> {
		// If buffer exhausted and not done, refill
		if self.pos >= self.buffer.len() && !self.exhausted {
			self.refill();
		}

		if self.pos < self.buffer.len() {
			let entry = self.buffer[self.pos].clone();
			self.pos += 1;
			Some(Ok(entry))
		} else {
			None
		}
	}
}

/// Convert borrowed Bound references to owned Bound values.
pub(super) fn make_range_bounds(start: Bound<&[u8]>, end: Bound<&[u8]>) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
	let start_bound = match start {
		Bound::Included(v) => Bound::Included(v.to_vec()),
		Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
		Bound::Unbounded => Bound::Unbounded,
	};
	let end_bound = match end {
		Bound::Included(v) => Bound::Included(v.to_vec()),
		Bound::Excluded(v) => Bound::Excluded(v.to_vec()),
		Bound::Unbounded => Bound::Unbounded,
	};
	(start_bound, end_bound)
}
