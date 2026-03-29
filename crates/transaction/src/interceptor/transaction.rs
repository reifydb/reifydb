// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	encoded::{key::EncodedKey, row::EncodedRow},
	interface::{
		catalog::shape::ShapeId,
		change::{Change, Diff},
	},
};
use reifydb_type::Result;

use crate::{
	TransactionId,
	change::{RowChange, TransactionalCatalogChanges},
	interceptor::chain::InterceptorChain,
};

/// `flow_changes` carries the table-level changes accumulated during the transaction
/// (input for transactional flow interceptors).
/// `pending_writes` is populated by interceptors with view writes to be merged
/// back into the transaction before it commits.
pub struct PreCommitContext {
	/// Table changes accumulated during this transaction (input to flow interceptors).
	pub flow_changes: Vec<Change>,
	/// View writes produced by flow interceptors to merge back into the transaction.
	/// `Some(value)` = set the key, `None` = remove the key.
	pub pending_writes: Vec<(EncodedKey, Option<EncodedRow>)>,
	/// Snapshot of the committing transaction's pending KV writes (read-only base for flow processing).
	/// `Some(value)` = set the key, `None` = remove the key.
	pub transaction_writes: Vec<(EncodedKey, Option<EncodedRow>)>,
	/// View-level accumulator entries produced by flow interceptors.
	/// Used by test infrastructure to feed view diffs back into the change accumulator.
	pub view_entries: Vec<(ShapeId, Diff)>,
}

impl PreCommitContext {
	pub fn new() -> Self {
		Self {
			flow_changes: Vec::new(),
			pending_writes: Vec::new(),
			transaction_writes: Vec::new(),
			view_entries: Vec::new(),
		}
	}
}

impl Default for PreCommitContext {
	fn default() -> Self {
		Self::new()
	}
}

pub trait PreCommitInterceptor: Send + Sync {
	fn intercept(&self, ctx: &mut PreCommitContext) -> Result<()>;
}

impl InterceptorChain<dyn PreCommitInterceptor + Send + Sync> {
	pub fn execute(&self, ctx: &mut PreCommitContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(ctx)?;
		}
		Ok(())
	}
}

/// Closure wrapper for pre-commit interceptors
pub struct ClosurePreCommitInterceptor<F>
where
	F: Fn(&mut PreCommitContext) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosurePreCommitInterceptor<F>
where
	F: Fn(&mut PreCommitContext) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosurePreCommitInterceptor<F>
where
	F: Fn(&mut PreCommitContext) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> PreCommitInterceptor for ClosurePreCommitInterceptor<F>
where
	F: Fn(&mut PreCommitContext) -> Result<()> + Send + Sync,
{
	fn intercept(&self, ctx: &mut PreCommitContext) -> Result<()> {
		(self.closure)(ctx)
	}
}

/// Helper function to create a closure pre-commit interceptor
pub fn pre_commit<F>(f: F) -> ClosurePreCommitInterceptor<F>
where
	F: Fn(&mut PreCommitContext) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosurePreCommitInterceptor::new(f)
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

/// Closure wrapper for post-commit interceptors
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

/// Helper function to create a closure post-commit interceptor
pub fn post_commit<F>(f: F) -> ClosurePostCommitInterceptor<F>
where
	F: Fn(&mut PostCommitContext) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosurePostCommitInterceptor::new(f)
}
