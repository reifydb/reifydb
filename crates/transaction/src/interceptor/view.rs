// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::view::View;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

// VIEW DEF POST CREATE
/// Context for view def post-create interceptors
pub struct ViewPostCreateContext<'a> {
	pub post: &'a View,
}

impl<'a> ViewPostCreateContext<'a> {
	pub fn new(post: &'a View) -> Self {
		Self {
			post,
		}
	}
}

pub trait ViewPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewPostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn ViewPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewPostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewPostCreateInterceptor for ClosureViewPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewPostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_post_create<F>(f: F) -> ClosureViewPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewPostCreateInterceptor::new(f)
}

// VIEW DEF PRE UPDATE
/// Context for view def pre-update interceptors
pub struct ViewPreUpdateContext<'a> {
	pub pre: &'a View,
}

impl<'a> ViewPreUpdateContext<'a> {
	pub fn new(pre: &'a View) -> Self {
		Self {
			pre,
		}
	}
}

pub trait ViewPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn ViewPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewPreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewPreUpdateInterceptor for ClosureViewPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_pre_update<F>(f: F) -> ClosureViewPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewPreUpdateInterceptor::new(f)
}

// VIEW DEF POST UPDATE
/// Context for view def post-update interceptors
pub struct ViewPostUpdateContext<'a> {
	pub pre: &'a View,
	pub post: &'a View,
}

impl<'a> ViewPostUpdateContext<'a> {
	pub fn new(pre: &'a View, post: &'a View) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait ViewPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn ViewPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewPostUpdateInterceptor for ClosureViewPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_post_update<F>(f: F) -> ClosureViewPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewPostUpdateInterceptor::new(f)
}

// VIEW DEF PRE DELETE
/// Context for view def pre-delete interceptors
pub struct ViewPreDeleteContext<'a> {
	pub pre: &'a View,
}

impl<'a> ViewPreDeleteContext<'a> {
	pub fn new(pre: &'a View) -> Self {
		Self {
			pre,
		}
	}
}

pub trait ViewPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn ViewPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewPreDeleteInterceptor for ClosureViewPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_pre_delete<F>(f: F) -> ClosureViewPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewPreDeleteInterceptor::new(f)
}
