// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interface::{CommandTransaction, TableDef};
use crate::interceptor::{
	Chain, PostCommitInterceptor, PreCommitInterceptor,
	TablePostDeleteInterceptor, TablePostInsertInterceptor,
	TablePostUpdateInterceptor, TablePreDeleteInterceptor,
	TablePreInsertInterceptor, TablePreUpdateInterceptor,
};
use crate::row::EncodedRow;
use crate::RowNumber;

pub trait TableInterceptor<CT: CommandTransaction> {
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

pub trait TransactionInterceptor<CT: CommandTransaction> {
	/// Intercept pre-commit operations
	fn pre_commit(&mut self) -> crate::Result<()>;

	/// Intercept post-commit operations
	fn post_commit(&mut self, version: crate::Version)
	-> crate::Result<()>;
}

/// Trait for accessing interceptor chains from transaction types
pub trait WithInterceptors<CT: CommandTransaction> {
	/// Access table pre-insert interceptor chain
	fn table_pre_insert_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn TablePreInsertInterceptor<CT>>;

	/// Access table post-insert interceptor chain
	fn table_post_insert_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn TablePostInsertInterceptor<CT>>;

	/// Access table pre-update interceptor chain
	fn table_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn TablePreUpdateInterceptor<CT>>;

	/// Access table post-update interceptor chain
	fn table_post_update_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn TablePostUpdateInterceptor<CT>>;

	/// Access table pre-delete interceptor chain
	fn table_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn TablePreDeleteInterceptor<CT>>;

	/// Access table post-delete interceptor chain
	fn table_post_delete_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn TablePostDeleteInterceptor<CT>>;

	/// Access pre-commit interceptor chain
	fn pre_commit_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn PreCommitInterceptor<CT>>;

	/// Access post-commit interceptor chain
	fn post_commit_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn PostCommitInterceptor<CT>>;
}
