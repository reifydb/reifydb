// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB


use std::sync::Arc;

use reifydb_core::{
	interface::{Batch, VTable},
	value::column::{ColumnWithName, columns::Columns, buffer::ColumnBuffer},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::{fragment::Fragment, value::Value};

use super::{BaseVTable, VTableContext};
use crate::vtable::user::{
	UserVTable, UserVTableColumn, UserVTableIterator, UserVTablePushdownContext,
};
use crate::Result;


pub struct UserVTableAdapter<U: UserVTable> {
	user_table: U,
	vtable: Arc<VTable>,
	exhausted: bool,
}

impl<U: UserVTable> UserVTableAdapter<U> {

	pub fn new(user_table: U, definition: Arc<VTable>) -> Self {
		Self {
			user_table,
			vtable: definition,
			exhausted: false,
		}
	}
}

impl<U: UserVTable> VTable for UserVTableAdapter<U> {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, _ctx: VTableContext) -> Result<()> {
		self.exhausted = false;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
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

	fn vtable(&self) -> &VTable {
		&self.vtable
	}
}


pub struct UserVTableIteratorAdapter<U: UserVTableIterator> {
	user_iter: U,
	vtable: Arc<VTable>,
	batch_size: usize,
	initialized: bool,
}

impl<U: UserVTableIterator> UserVTableIteratorAdapter<U> {

	#[allow(dead_code)]
	pub fn new(user_iter: U, vtable: Arc<VTable>) -> Self {
		Self {
			user_iter,
			vtable,
			batch_size: 1000,
			initialized: false,
		}
	}
}

impl<U: UserVTableIterator> VTable for UserVTableIteratorAdapter<U> {
	fn initialize(&mut self, _txn: &mut Transaction<'_>, ctx: VTableContext) -> Result<()> {

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

		self.user_iter.initialize(user_ctx.as_ref())?;
		self.initialized = true;
		Ok(())
	}

	fn next(&mut self, _txn: &mut Transaction<'_>) -> Result<Option<Batch>> {
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

	fn definition(&self) -> &VTable {
		&self.vtable
	}
}


pub(super) fn convert_rows_to_columns(
	user_columns: &[UserVTableColumn],
	rows: Vec<Vec<Value>>,
) -> Vec<ColumnWithName> {
	let num_rows = rows.len();
	let num_cols = user_columns.len();


	let mut column_data: Vec<ColumnBuffer> =
		user_columns.iter().map(|col| ColumnBuffer::with_capacity(col.data_type.clone(), num_rows)).collect();


	for row in rows {
		for (col_idx, value) in row.into_iter().enumerate() {
			if col_idx < num_cols {
				column_data[col_idx].push_value(value);
			}
		}
	}


	user_columns
		.iter()
		.zip(column_data)
		.map(|(def, data)| ColumnWithName {
			name: Fragment::internal(def.name.clone()),
			data,
		})
		.collect()
}
