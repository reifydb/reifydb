// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_type::params::Params;

use crate::bulk_insert::builder::{BulkInsertBuilder, ValidationMode};

/// Buffered ring buffer insert operation
#[derive(Debug, Clone)]
pub struct PendingRingBufferInsert {
	pub namespace: String,
	pub ringbuffer: String,
	pub rows: Vec<Params>,
}

impl PendingRingBufferInsert {
	pub fn new(namespace: String, ringbuffer: String) -> Self {
		Self {
			namespace,
			ringbuffer,
			rows: Vec::new(),
		}
	}

	pub fn add_row(&mut self, params: Params) {
		self.rows.push(params);
	}

	pub fn add_rows<I: IntoIterator<Item = Params>>(&mut self, iter: I) {
		self.rows.extend(iter);
	}
}

/// Builder for inserting rows into a specific ring buffer.
///
/// Created by calling `ringbuffer()` on a `BulkInsertBuilder`.
/// Call `done()` to finish and return to the main builder.
pub struct RingBufferInsertBuilder<'a, 'e, V: ValidationMode> {
	parent: &'a mut BulkInsertBuilder<'e, V>,
	pending: PendingRingBufferInsert,
}

impl<'a, 'e, V: ValidationMode> RingBufferInsertBuilder<'a, 'e, V> {
	/// Create a new ring buffer insert builder.
	pub(crate) fn new(parent: &'a mut BulkInsertBuilder<'e, V>, namespace: String, ringbuffer: String) -> Self {
		Self {
			parent,
			pending: PendingRingBufferInsert::new(namespace, ringbuffer),
		}
	}

	/// Add a single row from named params.
	///
	/// # Example
	///
	/// ```ignore
	/// builder.row(params!{ timestamp: 12345, event_type: "login" })
	/// ```
	pub fn row(mut self, params: Params) -> Self {
		self.pending.add_row(params);
		self
	}

	/// Add multiple rows from an iterator.
	///
	/// # Example
	///
	/// ```ignore
	/// let rows = vec![
	///     params!{ timestamp: 12345, event_type: "login" },
	///     params!{ timestamp: 12346, event_type: "logout" },
	/// ];
	/// builder.rows(rows)
	/// ```
	pub fn rows<I>(mut self, iter: I) -> Self
	where
		I: IntoIterator<Item = Params>,
	{
		self.pending.add_rows(iter);
		self
	}

	/// Finish this ring buffer insert and return to the main builder.
	///
	/// This allows chaining to insert into additional targets.
	pub fn done(self) -> &'a mut BulkInsertBuilder<'e, V> {
		self.parent.add_ringbuffer_insert(self.pending);
		self.parent
	}
}
