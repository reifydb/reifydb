// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::encoded::row::EncodedRow;
use reifydb_core::interface::catalog::segment_tree::SegmentTree;
use reifydb_value::Result;

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

pub struct SegmentTreeRowPreInsertContext<'a> {
	pub segment_tree: &'a SegmentTree,
	pub rows: &'a mut [EncodedRow],
}

impl<'a> SegmentTreeRowPreInsertContext<'a> {
	pub fn new(segment_tree: &'a SegmentTree, rows: &'a mut [EncodedRow]) -> Self {
		Self {
			segment_tree,
			rows,
		}
	}
}

pub trait SegmentTreeRowPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SegmentTreeRowPreInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SegmentTreeRowPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SegmentTreeRowPreInsertContext) -> Result<()> {
		let original_len = ctx.rows.len();
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
			assert_eq!(ctx.rows.len(), original_len, "pre_insert interceptor changed row count");
		}
		Ok(())
	}
}

pub struct ClosureSegmentTreeRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSegmentTreeRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSegmentTreeRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SegmentTreeRowPreInsertInterceptor for ClosureSegmentTreeRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SegmentTreeRowPreInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn segment_tree_row_pre_insert<F>(f: F) -> ClosureSegmentTreeRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSegmentTreeRowPreInsertInterceptor::new(f)
}

pub struct SegmentTreeRowPostInsertContext<'a> {
	pub segment_tree: &'a SegmentTree,
	pub rows: &'a [EncodedRow],
}

impl<'a> SegmentTreeRowPostInsertContext<'a> {
	pub fn new(segment_tree: &'a SegmentTree, rows: &'a [EncodedRow]) -> Self {
		Self {
			segment_tree,
			rows,
		}
	}
}

pub trait SegmentTreeRowPostInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SegmentTreeRowPostInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SegmentTreeRowPostInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SegmentTreeRowPostInsertContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSegmentTreeRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSegmentTreeRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSegmentTreeRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SegmentTreeRowPostInsertInterceptor for ClosureSegmentTreeRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SegmentTreeRowPostInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn segment_tree_row_post_insert<F>(f: F) -> ClosureSegmentTreeRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSegmentTreeRowPostInsertInterceptor::new(f)
}

pub struct SegmentTreeRowPreUpdateContext<'a> {
	pub segment_tree: &'a SegmentTree,
	pub rows: &'a mut [EncodedRow],
}

impl<'a> SegmentTreeRowPreUpdateContext<'a> {
	pub fn new(segment_tree: &'a SegmentTree, rows: &'a mut [EncodedRow]) -> Self {
		Self {
			segment_tree,
			rows,
		}
	}
}

pub trait SegmentTreeRowPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SegmentTreeRowPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SegmentTreeRowPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SegmentTreeRowPreUpdateContext) -> Result<()> {
		let original_len = ctx.rows.len();
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
			assert_eq!(ctx.rows.len(), original_len, "pre_update interceptor changed row count");
		}
		Ok(())
	}
}

pub struct ClosureSegmentTreeRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSegmentTreeRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSegmentTreeRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SegmentTreeRowPreUpdateInterceptor for ClosureSegmentTreeRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SegmentTreeRowPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn segment_tree_row_pre_update<F>(f: F) -> ClosureSegmentTreeRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSegmentTreeRowPreUpdateInterceptor::new(f)
}

pub struct SegmentTreeRowPostUpdateContext<'a> {
	pub segment_tree: &'a SegmentTree,
	pub posts: &'a [EncodedRow],
	pub pres: &'a [EncodedRow],
}

impl<'a> SegmentTreeRowPostUpdateContext<'a> {
	pub fn new(segment_tree: &'a SegmentTree, posts: &'a [EncodedRow], pres: &'a [EncodedRow]) -> Self {
		assert_eq!(posts.len(), pres.len(), "posts/pres length mismatch");
		Self {
			segment_tree,
			posts,
			pres,
		}
	}
}

pub trait SegmentTreeRowPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SegmentTreeRowPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SegmentTreeRowPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SegmentTreeRowPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSegmentTreeRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSegmentTreeRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSegmentTreeRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SegmentTreeRowPostUpdateInterceptor for ClosureSegmentTreeRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SegmentTreeRowPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn segment_tree_row_post_update<F>(f: F) -> ClosureSegmentTreeRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSegmentTreeRowPostUpdateInterceptor::new(f)
}

pub struct SegmentTreeRowPreDeleteContext<'a> {
	pub segment_tree: &'a SegmentTree,
}

impl<'a> SegmentTreeRowPreDeleteContext<'a> {
	pub fn new(segment_tree: &'a SegmentTree) -> Self {
		Self {
			segment_tree,
		}
	}
}

pub trait SegmentTreeRowPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SegmentTreeRowPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SegmentTreeRowPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SegmentTreeRowPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSegmentTreeRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSegmentTreeRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSegmentTreeRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SegmentTreeRowPreDeleteInterceptor for ClosureSegmentTreeRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SegmentTreeRowPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn segment_tree_row_pre_delete<F>(f: F) -> ClosureSegmentTreeRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSegmentTreeRowPreDeleteInterceptor::new(f)
}

pub struct SegmentTreeRowPostDeleteContext<'a> {
	pub segment_tree: &'a SegmentTree,
	pub deleted_rows: &'a [EncodedRow],
}

impl<'a> SegmentTreeRowPostDeleteContext<'a> {
	pub fn new(segment_tree: &'a SegmentTree, deleted_rows: &'a [EncodedRow]) -> Self {
		Self {
			segment_tree,
			deleted_rows,
		}
	}
}

pub trait SegmentTreeRowPostDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SegmentTreeRowPostDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SegmentTreeRowPostDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SegmentTreeRowPostDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSegmentTreeRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSegmentTreeRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSegmentTreeRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SegmentTreeRowPostDeleteInterceptor for ClosureSegmentTreeRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SegmentTreeRowPostDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn segment_tree_row_post_delete<F>(f: F) -> ClosureSegmentTreeRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreeRowPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSegmentTreeRowPostDeleteInterceptor::new(f)
}

pub struct SegmentTreeRowInterceptor;

impl SegmentTreeRowInterceptor {
	pub fn pre_insert(
		txn: &mut impl WithInterceptors,
		segment_tree: &SegmentTree,
		rows: &mut [EncodedRow],
	) -> Result<()> {
		let ctx = SegmentTreeRowPreInsertContext::new(segment_tree, rows);
		txn.segment_tree_row_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(
		txn: &mut impl WithInterceptors,
		segment_tree: &SegmentTree,
		rows: &[EncodedRow],
	) -> Result<()> {
		let ctx = SegmentTreeRowPostInsertContext::new(segment_tree, rows);
		txn.segment_tree_row_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(
		txn: &mut impl WithInterceptors,
		segment_tree: &SegmentTree,
		rows: &mut [EncodedRow],
	) -> Result<()> {
		let ctx = SegmentTreeRowPreUpdateContext::new(segment_tree, rows);
		txn.segment_tree_row_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		segment_tree: &SegmentTree,
		posts: &[EncodedRow],
		pres: &[EncodedRow],
	) -> Result<()> {
		let ctx = SegmentTreeRowPostUpdateContext::new(segment_tree, posts, pres);
		txn.segment_tree_row_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(txn: &mut impl WithInterceptors, segment_tree: &SegmentTree) -> Result<()> {
		let ctx = SegmentTreeRowPreDeleteContext::new(segment_tree);
		txn.segment_tree_row_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		segment_tree: &SegmentTree,
		deleted_rows: &[EncodedRow],
	) -> Result<()> {
		let ctx = SegmentTreeRowPostDeleteContext::new(segment_tree, deleted_rows);
		txn.segment_tree_row_post_delete_interceptors().execute(ctx)
	}
}
