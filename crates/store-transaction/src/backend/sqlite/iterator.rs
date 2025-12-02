// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Iterators for SQLite backend range queries.

use std::{
	ops::Bound,
	sync::{Arc, Mutex},
};

use reifydb_type::{Result, diagnostic::internal::internal, error};
use rusqlite::Connection;

use super::query::build_range_query;
use crate::backend::primitive::RawEntry;

/// Forward range iterator for SQLite storage.
pub struct SqliteRangeIter {
	pub(super) reader: Arc<Mutex<Connection>>,
	pub(super) table_name: String,
	pub(super) end: Bound<Vec<u8>>,
	pub(super) batch_size: usize,
	pub(super) buffer: Vec<RawEntry>,
	pub(super) pos: usize,
	pub(super) exhausted: bool,
}

impl SqliteRangeIter {
	/// Load initial batch with the given start bound.
	pub(super) fn load_initial(&mut self, start: Bound<&[u8]>) -> Result<()> {
		self.load_batch(start)
	}

	fn load_batch(&mut self, start: Bound<&[u8]>) -> Result<()> {
		let end_ref = match &self.end {
			Bound::Included(v) => Bound::Included(v.as_slice()),
			Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
			Bound::Unbounded => Bound::Unbounded,
		};

		let (query, params) = build_range_query(&self.table_name, start, end_ref, false, self.batch_size);

		let conn = self.reader.lock().unwrap();
		let mut stmt =
			conn.prepare(&query).map_err(|e| error!(internal(format!("Failed to prepare: {}", e))))?;

		self.buffer = stmt
			.query_map(
				params.iter().map(|v| v as &dyn rusqlite::ToSql).collect::<Vec<_>>().as_slice(),
				|row| {
					Ok(RawEntry {
						key: row.get(0)?,
						value: row.get(1)?,
					})
				},
			)
			.map_err(|e| error!(internal(format!("Failed to query: {}", e))))?
			.filter_map(|r| r.ok())
			.collect();

		self.pos = 0;
		// Only set exhausted when we get zero entries - a partial batch might
		// not mean we're done (e.g., with tombstones or edge cases)
		self.exhausted = self.buffer.is_empty();
		Ok(())
	}

	fn refill(&mut self) -> Result<()> {
		// Clone the last key to avoid borrow conflicts
		let last_key = self.buffer.last().map(|e| e.key.clone());

		let start = match &last_key {
			Some(key) => Bound::Excluded(key.as_slice()),
			None => {
				// No previous entries means we're done
				self.exhausted = true;
				return Ok(());
			}
		};

		self.load_batch(start)
	}
}

impl Iterator for SqliteRangeIter {
	type Item = Result<RawEntry>;

	fn next(&mut self) -> Option<Self::Item> {
		// If buffer exhausted and not done, refill
		if self.pos >= self.buffer.len() && !self.exhausted {
			if let Err(e) = self.refill() {
				return Some(Err(e));
			}
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

/// Reverse range iterator for SQLite storage.
pub struct SqliteRangeRevIter {
	pub(super) reader: Arc<Mutex<Connection>>,
	pub(super) table_name: String,
	pub(super) start: Bound<Vec<u8>>,
	pub(super) batch_size: usize,
	pub(super) buffer: Vec<RawEntry>,
	pub(super) pos: usize,
	pub(super) exhausted: bool,
}

impl SqliteRangeRevIter {
	/// Load initial batch with the given end bound.
	pub(super) fn load_initial(&mut self, end: Bound<&[u8]>) -> Result<()> {
		self.load_batch(end)
	}

	fn load_batch(&mut self, end: Bound<&[u8]>) -> Result<()> {
		let start_ref = match &self.start {
			Bound::Included(v) => Bound::Included(v.as_slice()),
			Bound::Excluded(v) => Bound::Excluded(v.as_slice()),
			Bound::Unbounded => Bound::Unbounded,
		};

		let (query, params) = build_range_query(
			&self.table_name,
			start_ref,
			end,
			true, // reverse
			self.batch_size,
		);

		let conn = self.reader.lock().unwrap();
		let mut stmt =
			conn.prepare(&query).map_err(|e| error!(internal(format!("Failed to prepare: {}", e))))?;

		self.buffer = stmt
			.query_map(
				params.iter().map(|v| v as &dyn rusqlite::ToSql).collect::<Vec<_>>().as_slice(),
				|row| {
					Ok(RawEntry {
						key: row.get(0)?,
						value: row.get(1)?,
					})
				},
			)
			.map_err(|e| error!(internal(format!("Failed to query: {}", e))))?
			.filter_map(|r| r.ok())
			.collect();

		self.pos = 0;
		// Only set exhausted when we get zero entries - a partial batch might
		// not mean we're done (e.g., with tombstones or edge cases)
		self.exhausted = self.buffer.is_empty();
		Ok(())
	}

	fn refill(&mut self) -> Result<()> {
		// Clone the last key to avoid borrow conflicts
		let last_key = self.buffer.last().map(|e| e.key.clone());

		// For reverse iteration, continue from before the last key
		let end = match &last_key {
			Some(key) => Bound::Excluded(key.as_slice()),
			None => {
				// No previous entries means we're done
				self.exhausted = true;
				return Ok(());
			}
		};

		self.load_batch(end)
	}
}

impl Iterator for SqliteRangeRevIter {
	type Item = Result<RawEntry>;

	fn next(&mut self) -> Option<Self::Item> {
		// If buffer exhausted and not done, refill
		if self.pos >= self.buffer.len() && !self.exhausted {
			if let Err(e) = self.refill() {
				return Some(Err(e));
			}
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
