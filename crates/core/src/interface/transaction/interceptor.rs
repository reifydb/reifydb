// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::RowNumber;

use crate::{
	CommitVersion, TransactionId,
	interceptor::{
		Chain, NamespaceDefPostCreateInterceptor, NamespaceDefPostUpdateInterceptor,
		NamespaceDefPreDeleteInterceptor, NamespaceDefPreUpdateInterceptor, PostCommitInterceptor,
		PreCommitInterceptor, RingBufferDefPostCreateInterceptor, RingBufferDefPostUpdateInterceptor,
		RingBufferDefPreDeleteInterceptor, RingBufferDefPreUpdateInterceptor, RingBufferPostDeleteInterceptor,
		RingBufferPostInsertInterceptor, RingBufferPostUpdateInterceptor, RingBufferPreDeleteInterceptor,
		RingBufferPreInsertInterceptor, RingBufferPreUpdateInterceptor, TableDefPostCreateInterceptor,
		TableDefPostUpdateInterceptor, TableDefPreDeleteInterceptor, TableDefPreUpdateInterceptor,
		TablePostDeleteInterceptor, TablePostInsertInterceptor, TablePostUpdateInterceptor,
		TablePreDeleteInterceptor, TablePreInsertInterceptor, TablePreUpdateInterceptor,
		ViewDefPostCreateInterceptor, ViewDefPostUpdateInterceptor, ViewDefPreDeleteInterceptor,
		ViewDefPreUpdateInterceptor, ViewPostDeleteInterceptor, ViewPostInsertInterceptor,
		ViewPostUpdateInterceptor, ViewPreDeleteInterceptor, ViewPreInsertInterceptor,
		ViewPreUpdateInterceptor,
	},
	interface::{
		CommandTransaction, NamespaceDef, RingBufferDef, RowChange, TableDef, ViewDef,
		transaction::change::TransactionalDefChanges,
	},
	value::encoded::EncodedValues,
};

pub trait TableInterceptor<CT: CommandTransaction> {
	/// Intercept table pre-insert operations
	fn pre_insert(&mut self, table: &TableDef, rn: RowNumber, row: &EncodedValues) -> crate::Result<()>;

	/// Intercept table post-insert operations
	fn post_insert(&mut self, table: &TableDef, id: RowNumber, row: &EncodedValues) -> crate::Result<()>;

	/// Intercept table pre-update operations
	fn pre_update(&mut self, table: &TableDef, id: RowNumber, row: &EncodedValues) -> crate::Result<()>;

	/// Intercept table post-update operations
	fn post_update(
		&mut self,
		table: &TableDef,
		id: RowNumber,
		row: &EncodedValues,
		old_row: &EncodedValues,
	) -> crate::Result<()>;

	/// Intercept table pre-delete operations
	fn pre_delete(&mut self, table: &TableDef, id: RowNumber) -> crate::Result<()>;

	/// Intercept table post-delete operations
	fn post_delete(&mut self, table: &TableDef, id: RowNumber, deleted_row: &EncodedValues) -> crate::Result<()>;
}

pub trait RingBufferInterceptor<CT: CommandTransaction> {
	/// Intercept ring buffer pre-insert operations
	fn pre_insert(&mut self, ringbuffer: &RingBufferDef, row: &EncodedValues) -> crate::Result<()>;

	/// Intercept ring buffer post-insert operations
	fn post_insert(&mut self, ringbuffer: &RingBufferDef, id: RowNumber, row: &EncodedValues) -> crate::Result<()>;

	/// Intercept ring buffer pre-update operations
	fn pre_update(&mut self, ringbuffer: &RingBufferDef, id: RowNumber, row: &EncodedValues) -> crate::Result<()>;

	/// Intercept ring buffer post-update operations
	fn post_update(
		&mut self,
		ringbuffer: &RingBufferDef,
		id: RowNumber,
		row: &EncodedValues,
		old_row: &EncodedValues,
	) -> crate::Result<()>;

	/// Intercept ring buffer pre-delete operations
	fn pre_delete(&mut self, ringbuffer: &RingBufferDef, id: RowNumber) -> crate::Result<()>;

	/// Intercept ring buffer post-delete operations
	fn post_delete(
		&mut self,
		ringbuffer: &RingBufferDef,
		id: RowNumber,
		deleted_row: &EncodedValues,
	) -> crate::Result<()>;
}

pub trait NamespaceDefInterceptor<CT: CommandTransaction> {
	/// Intercept namespace post-create operations
	fn post_create(&mut self, post: &NamespaceDef) -> crate::Result<()>;

	/// Intercept namespace pre-update operations
	fn pre_update(&mut self, pre: &NamespaceDef) -> crate::Result<()>;

	/// Intercept namespace post-update operations
	fn post_update(&mut self, pre: &NamespaceDef, post: &NamespaceDef) -> crate::Result<()>;

	/// Intercept namespace pre-delete operations
	fn pre_delete(&mut self, pre: &NamespaceDef) -> crate::Result<()>;
}

pub trait TableDefInterceptor<CT: CommandTransaction> {
	/// Intercept table definition post-create operations
	fn post_create(&mut self, post: &TableDef) -> crate::Result<()>;

	/// Intercept table definition pre-update operations
	fn pre_update(&mut self, pre: &TableDef) -> crate::Result<()>;

	/// Intercept table definition post-update operations
	fn post_update(&mut self, pre: &TableDef, post: &TableDef) -> crate::Result<()>;

	/// Intercept table definition pre-delete operations
	fn pre_delete(&mut self, pre: &TableDef) -> crate::Result<()>;
}

pub trait ViewDefInterceptor<CT: CommandTransaction> {
	/// Intercept view post-create operations
	fn post_create(&mut self, post: &ViewDef) -> crate::Result<()>;

	/// Intercept view pre-update operations
	fn pre_update(&mut self, pre: &ViewDef) -> crate::Result<()>;

	/// Intercept view post-update operations
	fn post_update(&mut self, pre: &ViewDef, post: &ViewDef) -> crate::Result<()>;

	/// Intercept view pre-delete operations
	fn pre_delete(&mut self, pre: &ViewDef) -> crate::Result<()>;
}

pub trait RingBufferDefInterceptor<CT: CommandTransaction> {
	/// Intercept ring buffer definition post-create operations
	fn post_create(&mut self, post: &RingBufferDef) -> crate::Result<()>;

	/// Intercept ring buffer definition pre-update operations
	fn pre_update(&mut self, pre: &RingBufferDef) -> crate::Result<()>;

	/// Intercept ring buffer definition post-update operations
	fn post_update(&mut self, pre: &RingBufferDef, post: &RingBufferDef) -> crate::Result<()>;

	/// Intercept ring buffer definition pre-delete operations
	fn pre_delete(&mut self, pre: &RingBufferDef) -> crate::Result<()>;
}

pub trait ViewInterceptor<CT: CommandTransaction> {
	/// Intercept view pre-insert operations
	fn pre_insert(&mut self, view: &ViewDef, rn: RowNumber, row: &EncodedValues) -> crate::Result<()>;

	/// Intercept view post-insert operations
	fn post_insert(&mut self, view: &ViewDef, id: RowNumber, row: &EncodedValues) -> crate::Result<()>;

	/// Intercept view pre-update operations
	fn pre_update(&mut self, view: &ViewDef, id: RowNumber, row: &EncodedValues) -> crate::Result<()>;

	/// Intercept view post-update operations
	fn post_update(
		&mut self,
		view: &ViewDef,
		id: RowNumber,
		row: &EncodedValues,
		old_row: &EncodedValues,
	) -> crate::Result<()>;

	/// Intercept view pre-delete operations
	fn pre_delete(&mut self, view: &ViewDef, id: RowNumber) -> crate::Result<()>;

	/// Intercept view post-delete operations
	fn post_delete(&mut self, view: &ViewDef, id: RowNumber, deleted_row: &EncodedValues) -> crate::Result<()>;
}

pub trait TransactionInterceptor<CT: CommandTransaction> {
	/// Intercept pre-commit operations
	fn pre_commit(&mut self) -> crate::Result<()>;

	/// Intercept post-commit operations
	fn post_commit(
		&mut self,
		id: TransactionId,
		version: CommitVersion,
		changes: TransactionalDefChanges,
		row_changes: Vec<RowChange>,
	) -> crate::Result<()>;
}

/// Trait for accessing interceptor chains from transaction types
pub trait WithInterceptors<CT: CommandTransaction> {
	/// Access table pre-insert interceptor chain
	fn table_pre_insert_interceptors(&mut self) -> &mut Chain<CT, dyn TablePreInsertInterceptor<CT>>;

	/// Access table post-insert interceptor chain
	fn table_post_insert_interceptors(&mut self) -> &mut Chain<CT, dyn TablePostInsertInterceptor<CT>>;

	/// Access table pre-update interceptor chain
	fn table_pre_update_interceptors(&mut self) -> &mut Chain<CT, dyn TablePreUpdateInterceptor<CT>>;

	/// Access table post-update interceptor chain
	fn table_post_update_interceptors(&mut self) -> &mut Chain<CT, dyn TablePostUpdateInterceptor<CT>>;

	/// Access table pre-delete interceptor chain
	fn table_pre_delete_interceptors(&mut self) -> &mut Chain<CT, dyn TablePreDeleteInterceptor<CT>>;

	/// Access table post-delete interceptor chain
	fn table_post_delete_interceptors(&mut self) -> &mut Chain<CT, dyn TablePostDeleteInterceptor<CT>>;

	/// Access ring buffer pre-insert interceptor chain
	fn ringbuffer_pre_insert_interceptors(&mut self) -> &mut Chain<CT, dyn RingBufferPreInsertInterceptor<CT>>;

	/// Access ring buffer post-insert interceptor chain
	fn ringbuffer_post_insert_interceptors(&mut self) -> &mut Chain<CT, dyn RingBufferPostInsertInterceptor<CT>>;

	/// Access ring buffer pre-update interceptor chain
	fn ringbuffer_pre_update_interceptors(&mut self) -> &mut Chain<CT, dyn RingBufferPreUpdateInterceptor<CT>>;

	/// Access ring buffer post-update interceptor chain
	fn ringbuffer_post_update_interceptors(&mut self) -> &mut Chain<CT, dyn RingBufferPostUpdateInterceptor<CT>>;

	/// Access ring buffer pre-delete interceptor chain
	fn ringbuffer_pre_delete_interceptors(&mut self) -> &mut Chain<CT, dyn RingBufferPreDeleteInterceptor<CT>>;

	/// Access ring buffer post-delete interceptor chain
	fn ringbuffer_post_delete_interceptors(&mut self) -> &mut Chain<CT, dyn RingBufferPostDeleteInterceptor<CT>>;

	/// Access pre-commit interceptor chain
	fn pre_commit_interceptors(&mut self) -> &mut Chain<CT, dyn PreCommitInterceptor<CT>>;

	/// Access post-commit interceptor chain
	fn post_commit_interceptors(&mut self) -> &mut Chain<CT, dyn PostCommitInterceptor<CT>>;

	// Namespace definition interceptor chains
	/// Access namespace post-create interceptor chain
	fn namespace_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn NamespaceDefPostCreateInterceptor<CT>>;

	/// Access namespace pre-update interceptor chain
	fn namespace_def_pre_update_interceptors(&mut self)
	-> &mut Chain<CT, dyn NamespaceDefPreUpdateInterceptor<CT>>;

	/// Access namespace post-update interceptor chain
	fn namespace_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn NamespaceDefPostUpdateInterceptor<CT>>;

	/// Access namespace pre-delete interceptor chain
	fn namespace_def_pre_delete_interceptors(&mut self)
	-> &mut Chain<CT, dyn NamespaceDefPreDeleteInterceptor<CT>>;

	// Table definition interceptor chains
	/// Access table definition post-create interceptor chain
	fn table_def_post_create_interceptors(&mut self) -> &mut Chain<CT, dyn TableDefPostCreateInterceptor<CT>>;

	/// Access table definition pre-update interceptor chain
	fn table_def_pre_update_interceptors(&mut self) -> &mut Chain<CT, dyn TableDefPreUpdateInterceptor<CT>>;

	/// Access table definition post-update interceptor chain
	fn table_def_post_update_interceptors(&mut self) -> &mut Chain<CT, dyn TableDefPostUpdateInterceptor<CT>>;

	/// Access table definition pre-delete interceptor chain
	fn table_def_pre_delete_interceptors(&mut self) -> &mut Chain<CT, dyn TableDefPreDeleteInterceptor<CT>>;

	// View definition interceptor chains
	/// Access view post-create interceptor chain
	fn view_def_post_create_interceptors(&mut self) -> &mut Chain<CT, dyn ViewDefPostCreateInterceptor<CT>>;

	/// Access view pre-update interceptor chain
	fn view_def_pre_update_interceptors(&mut self) -> &mut Chain<CT, dyn ViewDefPreUpdateInterceptor<CT>>;

	/// Access view post-update interceptor chain
	fn view_def_post_update_interceptors(&mut self) -> &mut Chain<CT, dyn ViewDefPostUpdateInterceptor<CT>>;

	/// Access view pre-delete interceptor chain
	fn view_def_pre_delete_interceptors(&mut self) -> &mut Chain<CT, dyn ViewDefPreDeleteInterceptor<CT>>;

	// Ring buffer definition interceptor chains
	/// Access ring buffer definition post-create interceptor chain
	fn ringbuffer_def_post_create_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn RingBufferDefPostCreateInterceptor<CT>>;

	/// Access ring buffer definition pre-update interceptor chain
	fn ringbuffer_def_pre_update_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn RingBufferDefPreUpdateInterceptor<CT>>;

	/// Access ring buffer definition post-update interceptor chain
	fn ringbuffer_def_post_update_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn RingBufferDefPostUpdateInterceptor<CT>>;

	/// Access ring buffer definition pre-delete interceptor chain
	fn ringbuffer_def_pre_delete_interceptors(
		&mut self,
	) -> &mut Chain<CT, dyn RingBufferDefPreDeleteInterceptor<CT>>;

	// View data interceptor chains
	/// Access view pre-insert interceptor chain
	fn view_pre_insert_interceptors(&mut self) -> &mut Chain<CT, dyn ViewPreInsertInterceptor<CT>>;

	/// Access view post-insert interceptor chain
	fn view_post_insert_interceptors(&mut self) -> &mut Chain<CT, dyn ViewPostInsertInterceptor<CT>>;

	/// Access view pre-update interceptor chain
	fn view_pre_update_interceptors(&mut self) -> &mut Chain<CT, dyn ViewPreUpdateInterceptor<CT>>;

	/// Access view post-update interceptor chain
	fn view_post_update_interceptors(&mut self) -> &mut Chain<CT, dyn ViewPostUpdateInterceptor<CT>>;

	/// Access view pre-delete interceptor chain
	fn view_pre_delete_interceptors(&mut self) -> &mut Chain<CT, dyn ViewPreDeleteInterceptor<CT>>;

	/// Access view post-delete interceptor chain
	fn view_post_delete_interceptors(&mut self) -> &mut Chain<CT, dyn ViewPostDeleteInterceptor<CT>>;
}
