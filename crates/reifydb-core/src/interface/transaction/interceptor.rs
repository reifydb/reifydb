// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interceptor::{
	Chain, PostCommitInterceptor, PreCommitInterceptor,
	SchemaDefPostCreateInterceptor, SchemaDefPostUpdateInterceptor,
	SchemaDefPreDeleteInterceptor, SchemaDefPreUpdateInterceptor,
	TableDefPostCreateInterceptor, TableDefPostUpdateInterceptor,
	TableDefPreDeleteInterceptor, TableDefPreUpdateInterceptor,
	TablePostDeleteInterceptor, TablePostInsertInterceptor,
	TablePostUpdateInterceptor, TablePreDeleteInterceptor,
	TablePreInsertInterceptor, TablePreUpdateInterceptor,
	ViewDefPostCreateInterceptor, ViewDefPostUpdateInterceptor,
	ViewDefPreDeleteInterceptor, ViewDefPreUpdateInterceptor,
};
use crate::interface::transaction::change::TransactionalChanges;
use crate::interface::{
	CommandTransaction, SchemaDef, TableDef, TransactionId, ViewDef,
};
use crate::row::EncodedRow;
use crate::{RowNumber, Version};

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

pub trait SchemaDefInterceptor<CT: CommandTransaction> {
	/// Intercept schema post-create operations
	fn post_create(&mut self, post: &SchemaDef) -> crate::Result<()>;

	/// Intercept schema pre-update operations
	fn pre_update(&mut self, pre: &SchemaDef) -> crate::Result<()>;

	/// Intercept schema post-update operations
	fn post_update(
		&mut self,
		pre: &SchemaDef,
		post: &SchemaDef,
	) -> crate::Result<()>;

	/// Intercept schema pre-delete operations
	fn pre_delete(&mut self, pre: &SchemaDef) -> crate::Result<()>;
}

pub trait TableDefInterceptor<CT: CommandTransaction> {
	/// Intercept table definition post-create operations
	fn post_create(&mut self, post: &TableDef) -> crate::Result<()>;

	/// Intercept table definition pre-update operations
	fn pre_update(&mut self, pre: &TableDef) -> crate::Result<()>;

	/// Intercept table definition post-update operations
	fn post_update(
		&mut self,
		pre: &TableDef,
		post: &TableDef,
	) -> crate::Result<()>;

	/// Intercept table definition pre-delete operations
	fn pre_delete(&mut self, pre: &TableDef) -> crate::Result<()>;
}

pub trait ViewDefInterceptor<CT: CommandTransaction> {
	/// Intercept view post-create operations
	fn post_create(&mut self, post: &ViewDef) -> crate::Result<()>;

	/// Intercept view pre-update operations
	fn pre_update(&mut self, pre: &ViewDef) -> crate::Result<()>;

	/// Intercept view post-update operations
	fn post_update(
		&mut self,
		pre: &ViewDef,
		post: &ViewDef,
	) -> crate::Result<()>;

	/// Intercept view pre-delete operations
	fn pre_delete(&mut self, pre: &ViewDef) -> crate::Result<()>;
}

pub trait TransactionInterceptor<CT: CommandTransaction> {
	/// Intercept pre-commit operations
	fn pre_commit(&mut self) -> crate::Result<()>;

	/// Intercept post-commit operations
	fn post_commit(
		&mut self,
		id: TransactionId,
		version: Version,
		changes: TransactionalChanges,
	) -> crate::Result<()>;
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

	// Schema definition interceptor chains
	/// Access schema post-create interceptor chain
	fn schema_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn SchemaDefPostCreateInterceptor<CT>>;

	/// Access schema pre-update interceptor chain
	fn schema_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn SchemaDefPreUpdateInterceptor<CT>>;

	/// Access schema post-update interceptor chain
	fn schema_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn SchemaDefPostUpdateInterceptor<CT>>;

	/// Access schema pre-delete interceptor chain
	fn schema_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn SchemaDefPreDeleteInterceptor<CT>>;

	// Table definition interceptor chains
	/// Access table definition post-create interceptor chain
	fn table_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn TableDefPostCreateInterceptor<CT>>;

	/// Access table definition pre-update interceptor chain
	fn table_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn TableDefPreUpdateInterceptor<CT>>;

	/// Access table definition post-update interceptor chain
	fn table_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn TableDefPostUpdateInterceptor<CT>>;

	/// Access table definition pre-delete interceptor chain
	fn table_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn TableDefPreDeleteInterceptor<CT>>;

	// View definition interceptor chains
	/// Access view post-create interceptor chain
	fn view_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn ViewDefPostCreateInterceptor<CT>>;

	/// Access view pre-update interceptor chain
	fn view_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn ViewDefPreUpdateInterceptor<CT>>;

	/// Access view post-update interceptor chain
	fn view_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn ViewDefPostUpdateInterceptor<CT>>;

	/// Access view pre-delete interceptor chain
	fn view_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn ViewDefPreDeleteInterceptor<CT>>;
}
