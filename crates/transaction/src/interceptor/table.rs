// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::table::Table;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

// TABLE DEF POST CREATE
/// Context for table def post-create interceptors
pub struct TablePostCreateContext<'a> {
	pub post: &'a Table,
}

impl<'a> TablePostCreateContext<'a> {
	pub fn new(post: &'a Table) -> Self {
		Self {
			post,
		}
	}
}

pub trait TablePostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TablePostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn TablePostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TablePostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTablePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TablePostCreateInterceptor for ClosureTablePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TablePostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_post_create<F>(f: F) -> ClosureTablePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTablePostCreateInterceptor::new(f)
}

// TABLE DEF PRE UPDATE
/// Context for table def pre-update interceptors
pub struct TablePreUpdateContext<'a> {
	pub pre: &'a Table,
}

impl<'a> TablePreUpdateContext<'a> {
	pub fn new(pre: &'a Table) -> Self {
		Self {
			pre,
		}
	}
}

pub trait TablePreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TablePreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn TablePreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TablePreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TablePreUpdateInterceptor for ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TablePreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_pre_update<F>(f: F) -> ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTablePreUpdateInterceptor::new(f)
}

// TABLE DEF POST UPDATE
/// Context for table def post-update interceptors
pub struct TablePostUpdateContext<'a> {
	pub pre: &'a Table,
	pub post: &'a Table,
}

impl<'a> TablePostUpdateContext<'a> {
	pub fn new(pre: &'a Table, post: &'a Table) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait TablePostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TablePostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn TablePostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TablePostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TablePostUpdateInterceptor for ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TablePostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_post_update<F>(f: F) -> ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTablePostUpdateInterceptor::new(f)
}

// TABLE DEF PRE DELETE
/// Context for table def pre-delete interceptors
pub struct TablePreDeleteContext<'a> {
	pub pre: &'a Table,
}

impl<'a> TablePreDeleteContext<'a> {
	pub fn new(pre: &'a Table) -> Self {
		Self {
			pre,
		}
	}
}

pub trait TablePreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TablePreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn TablePreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TablePreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TablePreDeleteInterceptor for ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TablePreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_pre_delete<F>(f: F) -> ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTablePreDeleteInterceptor::new(f)
}
