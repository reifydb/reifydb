// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Builder for inserting into a specific table.

use reifydb_type::Params;

use crate::bulk_insert::{BulkInsertBuilder, ValidationMode};

/// Buffered table insert operation
#[derive(Debug, Clone)]
pub struct PendingTableInsert {
	pub namespace: String,
	pub table: String,
	pub rows: Vec<Params>,
}

impl PendingTableInsert {
	pub fn new(namespace: String, table: String) -> Self {
		Self {
			namespace,
			table,
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

/// Builder for inserting rows into a specific table.
///
/// Created by calling `table()` on a `BulkInsertBuilder`.
/// Call `done()` to finish and return to the main builder.
pub struct TableInsertBuilder<'a, 'e, V: ValidationMode> {
	parent: &'a mut BulkInsertBuilder<'e, V>,
	pending: PendingTableInsert,
}

impl<'a, 'e, V: ValidationMode> TableInsertBuilder<'a, 'e, V> {
	/// Create a new table insert builder.
	pub(crate) fn new(parent: &'a mut BulkInsertBuilder<'e, V>, namespace: String, table: String) -> Self {
		Self {
			parent,
			pending: PendingTableInsert::new(namespace, table),
		}
	}

	/// Add a single row from named params.
	///
	/// # Example
	///
	/// ```ignore
	/// builder.row(params!{ id: 1, name: "Alice" })
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
	///     params!{ id: 1, name: "Alice" },
	///     params!{ id: 2, name: "Bob" },
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

	/// Finish this table insert and return to the main builder.
	///
	/// This allows chaining to insert into additional targets.
	pub fn done(self) -> &'a mut BulkInsertBuilder<'e, V> {
		self.parent.add_table_insert(self.pending);
		self.parent
	}
}
