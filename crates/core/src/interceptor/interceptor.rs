// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_type::RowNumber;

use crate::{
	CommitVersion,
	interceptor::{
		InterceptorChain, NamespaceDefPostCreateContext,
		NamespaceDefPostCreateInterceptor,
		NamespaceDefPostUpdateContext,
		NamespaceDefPostUpdateInterceptor,
		NamespaceDefPreDeleteContext, NamespaceDefPreDeleteInterceptor,
		NamespaceDefPreUpdateContext, NamespaceDefPreUpdateInterceptor,
		PostCommitContext, PostCommitInterceptor, PreCommitContext,
		PreCommitInterceptor, TableDefPostCreateContext,
		TableDefPostCreateInterceptor, TableDefPostUpdateContext,
		TableDefPostUpdateInterceptor, TableDefPreDeleteContext,
		TableDefPreDeleteInterceptor, TableDefPreUpdateContext,
		TableDefPreUpdateInterceptor, TablePostDeleteContext,
		TablePostDeleteInterceptor, TablePostInsertContext,
		TablePostInsertInterceptor, TablePostUpdateContext,
		TablePostUpdateInterceptor, TablePreDeleteContext,
		TablePreDeleteInterceptor, TablePreInsertContext,
		TablePreInsertInterceptor, TablePreUpdateContext,
		TablePreUpdateInterceptor, ViewDefPostCreateContext,
		ViewDefPostCreateInterceptor, ViewDefPostUpdateContext,
		ViewDefPostUpdateInterceptor, ViewDefPreDeleteContext,
		ViewDefPreDeleteInterceptor, ViewDefPreUpdateContext,
		ViewDefPreUpdateInterceptor,
	},
	interface::{
		CommandTransaction, NamespaceDef, TableDef, TransactionId,
		TransactionalChanges, ViewDef,
		interceptor::{
			NamespaceDefInterceptor, TableDefInterceptor,
			TableInterceptor, TransactionInterceptor,
			ViewDefInterceptor, WithInterceptors,
		},
	},
	row::EncodedRow,
};

/// Macro to generate interceptor execution methods
macro_rules! impl_interceptor_method {
	(
		$method_name:ident,
		$accessor_method:ident,
		$interceptor_trait:ident,
		$context_type:ident,
		($($param:ident: $type:ty),*)
	) => {
		fn $method_name(
			&mut self,
			$($param: $type),*
		) -> crate::Result<()> {
			if self.$accessor_method().is_empty() {
				return Ok(());
			}
			// We need to use unsafe here to work around the borrow checker
			// This is safe because:
			// 1. We know the interceptor chain won't outlive this function
			//    call
			// 2. The execution is synchronous and single-threaded
			// 3. We're only borrowing different parts of self
			unsafe {
				let chain_ptr: *mut InterceptorChain<
					CT,
					dyn $interceptor_trait<CT>,
				> = self.$accessor_method() as *mut _;
				let ctx = $context_type::new(self, $($param),*);
				(*chain_ptr).execute(ctx)?
			}
			Ok(())
		}
	};
}

impl<CT: CommandTransaction + WithInterceptors<CT>> TableInterceptor<CT>
	for CT
{
	impl_interceptor_method!(
		pre_insert,
		table_pre_insert_interceptors,
		TablePreInsertInterceptor,
		TablePreInsertContext,
		(table: &TableDef, row: &EncodedRow)
	);

	impl_interceptor_method!(
		post_insert,
		table_post_insert_interceptors,
		TablePostInsertInterceptor,
		TablePostInsertContext,
		(table: &TableDef, id: RowNumber, row: &EncodedRow)
	);

	impl_interceptor_method!(
		pre_update,
		table_pre_update_interceptors,
		TablePreUpdateInterceptor,
		TablePreUpdateContext,
		(table: &TableDef, id: RowNumber, row: &EncodedRow)
	);

	impl_interceptor_method!(
		post_update,
		table_post_update_interceptors,
		TablePostUpdateInterceptor,
		TablePostUpdateContext,
		(table: &TableDef, id: RowNumber, row: &EncodedRow, old_row: &EncodedRow)
	);

	impl_interceptor_method!(
		pre_delete,
		table_pre_delete_interceptors,
		TablePreDeleteInterceptor,
		TablePreDeleteContext,
		(table: &TableDef, id: RowNumber)
	);

	impl_interceptor_method!(
		post_delete,
		table_post_delete_interceptors,
		TablePostDeleteInterceptor,
		TablePostDeleteContext,
		(table: &TableDef, id: RowNumber, deleted_row: &EncodedRow)
	);
}

impl<CT: CommandTransaction + WithInterceptors<CT>> NamespaceDefInterceptor<CT>
	for CT
{
	impl_interceptor_method!(
		post_create,
		namespace_def_post_create_interceptors,
		NamespaceDefPostCreateInterceptor,
		NamespaceDefPostCreateContext,
		(post: &NamespaceDef)
	);

	impl_interceptor_method!(
		pre_update,
		namespace_def_pre_update_interceptors,
		NamespaceDefPreUpdateInterceptor,
		NamespaceDefPreUpdateContext,
		(pre: &NamespaceDef)
	);

	impl_interceptor_method!(
		post_update,
		namespace_def_post_update_interceptors,
		NamespaceDefPostUpdateInterceptor,
		NamespaceDefPostUpdateContext,
		(pre: &NamespaceDef, post: &NamespaceDef)
	);

	impl_interceptor_method!(
		pre_delete,
		namespace_def_pre_delete_interceptors,
		NamespaceDefPreDeleteInterceptor,
		NamespaceDefPreDeleteContext,
		(pre: &NamespaceDef)
	);
}

impl<CT: CommandTransaction + WithInterceptors<CT>> TableDefInterceptor<CT>
	for CT
{
	impl_interceptor_method!(
		post_create,
		table_def_post_create_interceptors,
		TableDefPostCreateInterceptor,
		TableDefPostCreateContext,
		(post: &TableDef)
	);

	impl_interceptor_method!(
		pre_update,
		table_def_pre_update_interceptors,
		TableDefPreUpdateInterceptor,
		TableDefPreUpdateContext,
		(pre: &TableDef)
	);

	impl_interceptor_method!(
		post_update,
		table_def_post_update_interceptors,
		TableDefPostUpdateInterceptor,
		TableDefPostUpdateContext,
		(pre: &TableDef, post: &TableDef)
	);

	impl_interceptor_method!(
		pre_delete,
		table_def_pre_delete_interceptors,
		TableDefPreDeleteInterceptor,
		TableDefPreDeleteContext,
		(pre: &TableDef)
	);
}

impl<CT: CommandTransaction + WithInterceptors<CT>> ViewDefInterceptor<CT>
	for CT
{
	impl_interceptor_method!(
		post_create,
		view_def_post_create_interceptors,
		ViewDefPostCreateInterceptor,
		ViewDefPostCreateContext,
		(post: &ViewDef)
	);

	impl_interceptor_method!(
		pre_update,
		view_def_pre_update_interceptors,
		ViewDefPreUpdateInterceptor,
		ViewDefPreUpdateContext,
		(pre: &ViewDef)
	);

	impl_interceptor_method!(
		post_update,
		view_def_post_update_interceptors,
		ViewDefPostUpdateInterceptor,
		ViewDefPostUpdateContext,
		(pre: &ViewDef, post: &ViewDef)
	);

	impl_interceptor_method!(
		pre_delete,
		view_def_pre_delete_interceptors,
		ViewDefPreDeleteInterceptor,
		ViewDefPreDeleteContext,
		(pre: &ViewDef)
	);
}

impl<CT: CommandTransaction + WithInterceptors<CT>> TransactionInterceptor<CT>
	for CT
{
	impl_interceptor_method!(
		pre_commit,
		pre_commit_interceptors,
		PreCommitInterceptor,
		PreCommitContext,
		()
	);

	fn post_commit(
		&mut self,
		id: TransactionId,
		version: CommitVersion,
		changes: TransactionalChanges,
	) -> crate::Result<()> {
		if self.post_commit_interceptors().is_empty() {
			return Ok(());
		}
		// We need to use unsafe here to work around the borrow checker
		// This is safe because:
		// 1. We know the interceptor chain won't outlive this function
		//    call
		// 2. The execution is synchronous and single-threaded
		// 3. We're only borrowing different parts of self
		unsafe {
			let chain_ptr: *mut InterceptorChain<
				CT,
				dyn PostCommitInterceptor<CT>,
			> = self.post_commit_interceptors() as *mut _;
			let ctx = PostCommitContext::new(id, version, changes);
			(*chain_ptr).execute(ctx)?
		}
		Ok(())
	}
}
