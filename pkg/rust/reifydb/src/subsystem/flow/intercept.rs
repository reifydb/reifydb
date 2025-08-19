// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Result,
	interceptor::{
		PreCommitContext, PreCommitInterceptor, TablePostDeleteContext,
		TablePostDeleteInterceptor, TablePostInsertContext,
		TablePostInsertInterceptor, TablePostUpdateContext,
		TablePostUpdateInterceptor,
	},
	interface::Transaction,
};

#[derive(Clone)]
pub struct TransactionalFlowInterceptor {}

impl TransactionalFlowInterceptor {
	pub fn new() -> Self {
		Self {}
	}
}

impl<T: Transaction> TablePostInsertInterceptor<T>
	for TransactionalFlowInterceptor
{
	fn intercept(&self, ctx: &mut TablePostInsertContext<T>) -> Result<()> {
		Ok(())
	}
}

impl<T: Transaction> TablePostUpdateInterceptor<T>
	for TransactionalFlowInterceptor
{
	fn intercept(&self, ctx: &mut TablePostUpdateContext<T>) -> Result<()> {
		// todo!()
		Ok(())
	}
}

impl<T: Transaction> TablePostDeleteInterceptor<T>
	for TransactionalFlowInterceptor
{
	fn intercept(&self, ctx: &mut TablePostDeleteContext<T>) -> Result<()> {
		// todo!()
		Ok(())
	}
}

impl<T: Transaction> PreCommitInterceptor<T> for TransactionalFlowInterceptor {
	fn intercept(&self, ctx: &mut PreCommitContext<T>) -> Result<()> {
		// todo!()
		Ok(())
	}
}
