// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	RowNumber, impl_interceptor_method,
	interceptor::{
		InterceptorChain, TablePostDeleteContext,
		TablePostDeleteInterceptor, TablePostInsertContext,
		TablePostInsertInterceptor, TablePostUpdateContext,
		TablePostUpdateInterceptor, TablePreDeleteContext,
		TablePreDeleteInterceptor, TablePreInsertContext,
		TablePreInsertInterceptor, TablePreUpdateContext,
		TablePreUpdateInterceptor,
	},
	interface::{CommandTransaction, TableDef, Transaction},
	row::EncodedRow,
};

/// Extension trait for interceptor execution on CommandTransaction
pub trait TableInterceptor<T: Transaction> {
	/// Intercept table pre-insert operations
	fn pre_insert(
		&mut self,
		table: &TableDef,
		row: &EncodedRow,
	) -> crate::Result<()>;

	/// Intercept table post-insert operations
	fn post_insert(
		&mut self,
		table: &TableDef,
		id: RowNumber,
		row: &EncodedRow,
	) -> crate::Result<()>;

	/// Intercept table pre-update operations
	fn pre_update(
		&mut self,
		table: &TableDef,
		id: RowNumber,
		row: &EncodedRow,
	) -> crate::Result<()>;

	/// Intercept table post-update operations
	fn post_update(
		&mut self,
		table: &TableDef,
		id: RowNumber,
		row: &EncodedRow,
		old_row: &EncodedRow,
	) -> crate::Result<()>;

	/// Intercept table pre-delete operations
	fn pre_delete(
		&mut self,
		table: &TableDef,
		id: RowNumber,
	) -> crate::Result<()>;

	/// Intercept table post-delete operations
	fn post_delete(
		&mut self,
		table: &TableDef,
		id: RowNumber,
		deleted_row: &EncodedRow,
	) -> crate::Result<()>;
}

impl<T: Transaction> TableInterceptor<T> for CommandTransaction<T> {
	impl_interceptor_method!(
		pre_insert,
		table_pre_insert,
		TablePreInsertInterceptor,
		TablePreInsertContext,
		(table: &TableDef, row: &EncodedRow)
	);

	impl_interceptor_method!(
		post_insert,
		table_post_insert,
		TablePostInsertInterceptor,
		TablePostInsertContext,
		(table: &TableDef, id: RowNumber, row: &EncodedRow)
	);

	impl_interceptor_method!(
		pre_update,
		table_pre_update,
		TablePreUpdateInterceptor,
		TablePreUpdateContext,
		(table: &TableDef, id: RowNumber, row: &EncodedRow)
	);

	impl_interceptor_method!(
		post_update,
		table_post_update,
		TablePostUpdateInterceptor,
		TablePostUpdateContext,
		(table: &TableDef, id: RowNumber, row: &EncodedRow, old_row: &EncodedRow)
	);

	impl_interceptor_method!(
		pre_delete,
		table_pre_delete,
		TablePreDeleteInterceptor,
		TablePreDeleteContext,
		(table: &TableDef, id: RowNumber)
	);

	impl_interceptor_method!(
		post_delete,
		table_post_delete,
		TablePostDeleteInterceptor,
		TablePostDeleteContext,
		(table: &TableDef, id: RowNumber, deleted_row: &EncodedRow)
	);
}
