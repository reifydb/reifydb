// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Adapters that wrap user virtual table implementations into the internal `TableVirtual` trait.

use std::sync::Arc;

use reifydb_core::{
	interface::TableVirtualDef,
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::{Fragment, Value};

use super::{TableVirtual, TableVirtualContext};
use crate::{
	execute::Batch,
	table_virtual::user::{
		TableVirtualUser, TableVirtualUserColumnDef, TableVirtualUserIterator, TableVirtualUserPushdownContext,
	},
	transaction::StandardTransaction,
};

/// Adapter that wraps a `TableVirtualUser` into the internal `TableVirtual` trait.
pub struct TableVirtualUserAdapter<T: TableVirtualUser> {
	user_table: T,
	definition: Arc<TableVirtualDef>,
	exhausted: bool,
}

impl<T: TableVirtualUser> TableVirtualUserAdapter<T> {
	/// Create a new adapter wrapping the user table.
	pub fn new(user_table: T, definition: Arc<TableVirtualDef>) -> Self {
		Self {
			user_table,
			definition,
			exhausted: false,
		}
	}
}

impl<'a, T: TableVirtualUser> TableVirtual<'a> for TableVirtualUserAdapter<T> {
	fn initialize(
		&mut self,
		_txn: &mut StandardTransaction<'a>,
		_ctx: TableVirtualContext<'a>,
	) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut StandardTransaction<'a>) -> crate::Result<Option<Batch<'a>>> {
		if self.exhausted {
			return Ok(None);
		}

		let user_rows = self.user_table.rows();
		let user_columns = self.user_table.columns();

		if user_rows.is_empty() {
			self.exhausted = true;
			return Ok(None);
		}

		let columns = convert_rows_to_columns(&user_columns, user_rows);

		self.exhausted = true;
		Ok(Some(Batch {
			columns: Columns::new(columns),
		}))
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}

/// Adapter that wraps a `TableVirtualUserIterator` into the internal `TableVirtual` trait.
pub struct TableVirtualUserIteratorAdapter<T: TableVirtualUserIterator> {
	user_iter: T,
	definition: Arc<TableVirtualDef>,
	batch_size: usize,
	initialized: bool,
}

impl<T: TableVirtualUserIterator> TableVirtualUserIteratorAdapter<T> {
	/// Create a new adapter wrapping the user iterator.
	#[allow(dead_code)]
	pub fn new(user_iter: T, definition: Arc<TableVirtualDef>) -> Self {
		Self {
			user_iter,
			definition,
			batch_size: 1000, // Default batch size
			initialized: false,
		}
	}
}

impl<'a, T: TableVirtualUserIterator + Sync> TableVirtual<'a> for TableVirtualUserIteratorAdapter<T> {
	fn initialize(
		&mut self,
		_txn: &mut StandardTransaction<'a>,
		ctx: TableVirtualContext<'a>,
	) -> crate::Result<()> {
		// Convert internal context to user pushdown context
		let user_ctx = match ctx {
			TableVirtualContext::Basic {
				..
			} => None,
			TableVirtualContext::PushDown {
				limit,
				..
			} => Some(TableVirtualUserPushdownContext {
				limit,
			}),
		};

		self.user_iter.initialize(user_ctx.as_ref())?;
		self.initialized = true;
		Ok(())
	}

	fn next(&mut self, _txn: &mut StandardTransaction<'a>) -> crate::Result<Option<Batch<'a>>> {
		if !self.initialized {
			return Ok(None);
		}

		let user_columns = self.user_iter.columns();
		let user_rows = self.user_iter.next_batch(self.batch_size)?;

		match user_rows {
			None => Ok(None),
			Some(rows) if rows.is_empty() => Ok(None),
			Some(rows) => {
				let columns = convert_rows_to_columns(&user_columns, rows);
				Ok(Some(Batch {
					columns: Columns::new(columns),
				}))
			}
		}
	}

	fn definition(&self) -> &TableVirtualDef {
		&self.definition
	}
}

/// Convert user row-oriented data to column-oriented data.
pub(super) fn convert_rows_to_columns(
	user_columns: &[TableVirtualUserColumnDef],
	rows: Vec<Vec<Value>>,
) -> Vec<Column<'static>> {
	let num_rows = rows.len();
	let num_cols = user_columns.len();

	// Initialize column data vectors
	let mut column_data: Vec<ColumnData> =
		user_columns.iter().map(|col| ColumnData::with_capacity(col.data_type.clone(), num_rows)).collect();

	// Transpose row data into columns
	for row in rows {
		for (col_idx, value) in row.into_iter().enumerate() {
			if col_idx < num_cols {
				column_data[col_idx].push_value(value);
			}
		}
	}

	// Create Column structs
	user_columns
		.iter()
		.zip(column_data)
		.map(|(def, data)| Column {
			name: Fragment::owned_internal(def.name.clone()),
			data,
		})
		.collect()
}
