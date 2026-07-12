// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::segment_tree::SegmentTree;
use reifydb_value::Result;

use crate::interceptor::chain::InterceptorChain;

pub struct SegmentTreePostCreateContext<'a> {
	pub post: &'a SegmentTree,
}

impl<'a> SegmentTreePostCreateContext<'a> {
	pub fn new(post: &'a SegmentTree) -> Self {
		Self {
			post,
		}
	}
}

pub trait SegmentTreePostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SegmentTreePostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SegmentTreePostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SegmentTreePostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSegmentTreePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSegmentTreePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSegmentTreePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SegmentTreePostCreateInterceptor for ClosureSegmentTreePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SegmentTreePostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn segment_tree_post_create<F>(f: F) -> ClosureSegmentTreePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSegmentTreePostCreateInterceptor::new(f)
}

pub struct SegmentTreePreUpdateContext<'a> {
	pub pre: &'a SegmentTree,
}

impl<'a> SegmentTreePreUpdateContext<'a> {
	pub fn new(pre: &'a SegmentTree) -> Self {
		Self {
			pre,
		}
	}
}

pub trait SegmentTreePreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SegmentTreePreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SegmentTreePreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SegmentTreePreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSegmentTreePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSegmentTreePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSegmentTreePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SegmentTreePreUpdateInterceptor for ClosureSegmentTreePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SegmentTreePreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn segment_tree_pre_update<F>(f: F) -> ClosureSegmentTreePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSegmentTreePreUpdateInterceptor::new(f)
}

pub struct SegmentTreePostUpdateContext<'a> {
	pub pre: &'a SegmentTree,
	pub post: &'a SegmentTree,
}

impl<'a> SegmentTreePostUpdateContext<'a> {
	pub fn new(pre: &'a SegmentTree, post: &'a SegmentTree) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait SegmentTreePostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SegmentTreePostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SegmentTreePostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SegmentTreePostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSegmentTreePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSegmentTreePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSegmentTreePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SegmentTreePostUpdateInterceptor for ClosureSegmentTreePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SegmentTreePostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn segment_tree_post_update<F>(f: F) -> ClosureSegmentTreePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSegmentTreePostUpdateInterceptor::new(f)
}

pub struct SegmentTreePreDeleteContext<'a> {
	pub pre: &'a SegmentTree,
}

impl<'a> SegmentTreePreDeleteContext<'a> {
	pub fn new(pre: &'a SegmentTree) -> Self {
		Self {
			pre,
		}
	}
}

pub trait SegmentTreePreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SegmentTreePreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SegmentTreePreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SegmentTreePreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSegmentTreePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSegmentTreePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSegmentTreePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SegmentTreePreDeleteInterceptor for ClosureSegmentTreePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SegmentTreePreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn segment_tree_pre_delete<F>(f: F) -> ClosureSegmentTreePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SegmentTreePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSegmentTreePreDeleteInterceptor::new(f)
}
