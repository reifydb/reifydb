// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use crate::{
	impl_interceptor_method,
	interceptor::{
		InterceptorChain,
		transaction::{
			PostCommitInterceptor, PreCommitInterceptor,
			context::{PostCommitContext, PreCommitContext},
		},
	},
	interface::{CommandTransaction, Transaction},
};

/// Extension trait for transaction-level interceptor execution on
/// CommandTransaction
pub trait TransactionInterceptor<T: Transaction> {
	/// Intercept pre-commit operations
	fn pre_commit(&mut self) -> crate::Result<()>;

	/// Intercept post-commit operations
	fn post_commit(&mut self, version: crate::Version)
	-> crate::Result<()>;
}

impl<T: Transaction> TransactionInterceptor<T> for CommandTransaction<T> {
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
