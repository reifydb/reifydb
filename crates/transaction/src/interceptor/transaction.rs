// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	CommitVersion,
	interface::{RowChange, TransactionId, TransactionalDefChanges},
};

use crate::interceptor::InterceptorChain;

// ============================================================================
// PRE COMMIT
// ============================================================================

/// Context for pre-commit interceptors
pub struct PreCommitContext {}

impl PreCommitContext {
	pub fn new() -> Self {
		Self {}
	}
}

impl Default for PreCommitContext {
	fn default() -> Self {
		Self::new()
	}
}

#[async_trait::async_trait]
pub trait PreCommitInterceptor: Send + Sync {
	async fn intercept(&self, ctx: &mut PreCommitContext) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn PreCommitInterceptor + Send + Sync> {
	pub async fn execute(&self, mut ctx: PreCommitContext) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

/// Closure wrapper for pre-commit interceptors
pub struct ClosurePreCommitInterceptor<F>
where
	F: Fn(&mut PreCommitContext) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosurePreCommitInterceptor<F>
where
	F: Fn(&mut PreCommitContext) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosurePreCommitInterceptor<F>
where
	F: Fn(&mut PreCommitContext) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> PreCommitInterceptor for ClosurePreCommitInterceptor<F>
where
	F: Fn(&mut PreCommitContext) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept(&self, ctx: &mut PreCommitContext) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

/// Helper function to create a closure pre-commit interceptor
pub fn pre_commit<F>(f: F) -> ClosurePreCommitInterceptor<F>
where
	F: Fn(&mut PreCommitContext) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
{
	ClosurePreCommitInterceptor::new(f)
}

// ============================================================================
// POST COMMIT
// ============================================================================

/// Context for post-commit interceptors
pub struct PostCommitContext {
	pub id: TransactionId,
	pub version: CommitVersion,
	pub changes: TransactionalDefChanges,
	pub row_changes: Vec<RowChange>,
}

impl PostCommitContext {
	pub fn new(
		id: TransactionId,
		version: CommitVersion,
		changes: TransactionalDefChanges,
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

#[async_trait::async_trait]
pub trait PostCommitInterceptor: Send + Sync {
	async fn intercept(&self, ctx: &mut PostCommitContext) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn PostCommitInterceptor + Send + Sync> {
	pub async fn execute(&self, mut ctx: PostCommitContext) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

/// Closure wrapper for post-commit interceptors
pub struct ClosurePostCommitInterceptor<F>
where
	F: Fn(&mut PostCommitContext) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosurePostCommitInterceptor<F>
where
	F: Fn(&mut PostCommitContext) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosurePostCommitInterceptor<F>
where
	F: Fn(&mut PostCommitContext) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> PostCommitInterceptor for ClosurePostCommitInterceptor<F>
where
	F: Fn(&mut PostCommitContext) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept(&self, ctx: &mut PostCommitContext) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

/// Helper function to create a closure post-commit interceptor
pub fn post_commit<F>(f: F) -> ClosurePostCommitInterceptor<F>
where
	F: Fn(&mut PostCommitContext) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
{
	ClosurePostCommitInterceptor::new(f)
}
