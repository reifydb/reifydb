// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Adapters that wrap user virtual table implementations into the internal `VTable` trait.

use std::sync::Arc;

use async_trait::async_trait;
use reifydb_core::{
	interface::{Batch, QueryTransaction, VTableDef},
	value::column::{Column, ColumnData, Columns},
};
use reifydb_type::{Fragment, Value};

use super::{VTable, VTableContext};
use crate::vtable::user::{
	UserVTable, UserVTableColumnDef, UserVTableIterator, UserVTablePushdownContext,
};

/// Adapter that wraps a `UserVTable` into the internal `VTable` trait.
pub struct UserVTableAdapter<U: UserVTable> {
	user_table: U,
	definition: Arc<VTableDef>,
	exhausted: bool,
}

impl<U: UserVTable> UserVTableAdapter<U> {
	/// Create a new adapter wrapping the user table.
	pub fn new(user_table: U, definition: Arc<VTableDef>) -> Self {
		Self {
			user_table,
			definition,
			exhausted: false,
		}
	}
}

#[async_trait]
impl<U: UserVTable, T: QueryTransaction> VTable<T> for UserVTableAdapter<U> {
	async fn initialize(&mut self, _txn: &mut T, _ctx: VTableContext) -> crate::Result<()> {
		self.exhausted = false;
		Ok(())
	}

	async fn next(&mut self, _txn: &mut T) -> crate::Result<Option<Batch>> {
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

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}

/// Adapter that wraps a `UserVTableIterator` into the internal `VTable` trait.
pub struct UserVTableIteratorAdapter<U: UserVTableIterator> {
	user_iter: U,
	definition: Arc<VTableDef>,
	batch_size: usize,
	initialized: bool,
}

impl<U: UserVTableIterator> UserVTableIteratorAdapter<U> {
	/// Create a new adapter wrapping the user iterator.
	#[allow(dead_code)]
	pub fn new(user_iter: U, definition: Arc<VTableDef>) -> Self {
		Self {
			user_iter,
			definition,
			batch_size: 1000, // Default batch size
			initialized: false,
		}
	}
}

#[async_trait]
impl<U: UserVTableIterator, T: QueryTransaction> VTable<T> for UserVTableIteratorAdapter<U> {
	async fn initialize(&mut self, _txn: &mut T, ctx: VTableContext) -> crate::Result<()> {
		// Convert internal context to user pushdown context
		let user_ctx = match ctx {
			VTableContext::Basic {
				..
			} => None,
			VTableContext::PushDown {
				limit,
				..
			} => Some(UserVTablePushdownContext {
				limit,
			}),
		};

		self.user_iter.initialize(user_ctx.as_ref()).await?;
		self.initialized = true;
		Ok(())
	}

	async fn next(&mut self, _txn: &mut T) -> crate::Result<Option<Batch>> {
		if !self.initialized {
			return Ok(None);
		}

		let user_columns = self.user_iter.columns();
		let user_rows = self.user_iter.next_batch(self.batch_size).await?;

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

	fn definition(&self) -> &VTableDef {
		&self.definition
	}
}

/// Convert user row-oriented data to column-oriented data.
pub(super) fn convert_rows_to_columns(
	user_columns: &[UserVTableColumnDef],
	rows: Vec<Vec<Value>>,
) -> Vec<Column> {
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
			name: Fragment::internal(def.name.clone()),
			data,
		})
		.collect()
}
