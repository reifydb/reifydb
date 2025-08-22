// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::interceptor::{
	InterceptorChain, PostCommitContext, PostCommitInterceptor,
	PreCommitContext, PreCommitInterceptor, TablePostDeleteContext,
	TablePostDeleteInterceptor, TablePostInsertContext,
	TablePostInsertInterceptor, TablePostUpdateContext,
	TablePostUpdateInterceptor, TablePreDeleteContext,
	TablePreDeleteInterceptor, TablePreInsertContext,
	TablePreInsertInterceptor, TablePreUpdateContext,
	TablePreUpdateInterceptor,
};
use crate::interface::interceptor::{TableInterceptor, TransactionInterceptor};
use crate::interface::{TableDef, Transaction};
use crate::row::EncodedRow;
use crate::transaction::StandardCommandTransaction;
use crate::RowNumber;

/// Macro to generate interceptor execution methods
macro_rules! impl_interceptor_method {
	(
		$method_name:ident,
		$field:ident,
		$interceptor_trait:ident,
		$context_type:ident,
		($($param:ident: $type:ty),*)
	) => {
		fn $method_name(
			&mut self,
			$($param: $type),*
		) -> crate::Result<()> {
			if self.interceptors.$field.is_empty() {
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
					T,
					dyn $interceptor_trait<T>,
				> = &mut self.interceptors.$field as *mut _;
				let ctx = $context_type::new(self, $($param),*);
				(*chain_ptr).execute(ctx)?
			}
			Ok(())
		}
	};
}

impl<T: Transaction> TableInterceptor<T> for StandardCommandTransaction<T> {
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

impl<T: Transaction> TransactionInterceptor<T> for StandardCommandTransaction<T> {
	impl_interceptor_method!(
		pre_commit,
		pre_commit,
		PreCommitInterceptor,
		PreCommitContext,
		()
	);

	fn post_commit(
		&mut self,
		version: crate::Version,
	) -> crate::Result<()> {
		if self.interceptors.post_commit.is_empty() {
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
				T,
				dyn PostCommitInterceptor<T>,
			> = &mut self.interceptors.post_commit as *mut _;
			let ctx = PostCommitContext::new(version);
			(*chain_ptr).execute(ctx)?
		}
		Ok(())
	}
}
