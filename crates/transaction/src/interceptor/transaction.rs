// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::common::CommitVersion;
use reifydb_value::Result;

use crate::{
	TransactionId,
	change::{RowChange, TransactionalCatalogChanges},
	interceptor::chain::InterceptorChain,
};

pub struct PrePublishContext<'a> {
	pub id: TransactionId,
	pub version: CommitVersion,
	pub changes: &'a TransactionalCatalogChanges,
}

impl<'a> PrePublishContext<'a> {
	pub fn new(id: TransactionId, version: CommitVersion, changes: &'a TransactionalCatalogChanges) -> Self {
		Self {
			id,
			version,
			changes,
		}
	}
}

pub trait PrePublishInterceptor: Send + Sync {
	fn intercept(&self, ctx: &mut PrePublishContext<'_>) -> Result<()>;
}

impl InterceptorChain<dyn PrePublishInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: PrePublishContext<'_>) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct PostCommitContext {
	pub id: TransactionId,
	pub version: CommitVersion,
	pub changes: TransactionalCatalogChanges,
	pub row_changes: Vec<RowChange>,
}

impl PostCommitContext {
	pub fn new(
		id: TransactionId,
		version: CommitVersion,
		changes: TransactionalCatalogChanges,
		row_changes: Vec<RowChange>,
	) -> Self {
		Self {
			id,
			version,
			changes,
			row_changes,
		}
	}
}

pub trait PostCommitInterceptor: Send + Sync {
	fn intercept(&self, ctx: &mut PostCommitContext) -> Result<()>;
}

impl InterceptorChain<dyn PostCommitInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: PostCommitContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosurePostCommitInterceptor<F>
where
	F: Fn(&mut PostCommitContext) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosurePostCommitInterceptor<F>
where
	F: Fn(&mut PostCommitContext) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosurePostCommitInterceptor<F>
where
	F: Fn(&mut PostCommitContext) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> PostCommitInterceptor for ClosurePostCommitInterceptor<F>
where
	F: Fn(&mut PostCommitContext) -> Result<()> + Send + Sync,
{
	fn intercept(&self, ctx: &mut PostCommitContext) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn post_commit<F>(f: F) -> ClosurePostCommitInterceptor<F>
where
	F: Fn(&mut PostCommitContext) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosurePostCommitInterceptor::new(f)
}
