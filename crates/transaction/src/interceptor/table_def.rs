// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::TableDef;

use crate::interceptor::InterceptorChain;

// TABLE DEF POST CREATE
/// Context for table def post-create interceptors
pub struct TableDefPostCreateContext<'a> {
	pub post: &'a TableDef,
}

impl<'a> TableDefPostCreateContext<'a> {
	pub fn new(post: &'a TableDef) -> Self {
		Self {
			post,
		}
	}
}

#[async_trait::async_trait]
pub trait TableDefPostCreateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut TableDefPostCreateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn TableDefPostCreateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: TableDefPostCreateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureTableDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTableDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTableDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> TableDefPostCreateInterceptor for ClosureTableDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut TableDefPostCreateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_def_post_create<F>(f: F) -> ClosureTableDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTableDefPostCreateInterceptor::new(f)
}

// TABLE DEF PRE UPDATE
/// Context for table def pre-update interceptors
pub struct TableDefPreUpdateContext<'a> {
	pub pre: &'a TableDef,
}

impl<'a> TableDefPreUpdateContext<'a> {
	pub fn new(pre: &'a TableDef) -> Self {
		Self {
			pre,
		}
	}
}

#[async_trait::async_trait]
pub trait TableDefPreUpdateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut TableDefPreUpdateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn TableDefPreUpdateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: TableDefPreUpdateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureTableDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTableDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTableDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> TableDefPreUpdateInterceptor for ClosureTableDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut TableDefPreUpdateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_def_pre_update<F>(f: F) -> ClosureTableDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTableDefPreUpdateInterceptor::new(f)
}

// TABLE DEF POST UPDATE
/// Context for table def post-update interceptors
pub struct TableDefPostUpdateContext<'a> {
	pub pre: &'a TableDef,
	pub post: &'a TableDef,
}

impl<'a> TableDefPostUpdateContext<'a> {
	pub fn new(pre: &'a TableDef, post: &'a TableDef) -> Self {
		Self {
			pre,
			post,
		}
	}
}

#[async_trait::async_trait]
pub trait TableDefPostUpdateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut TableDefPostUpdateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn TableDefPostUpdateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: TableDefPostUpdateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureTableDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTableDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTableDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> TableDefPostUpdateInterceptor for ClosureTableDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut TableDefPostUpdateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_def_post_update<F>(f: F) -> ClosureTableDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTableDefPostUpdateInterceptor::new(f)
}

// TABLE DEF PRE DELETE
/// Context for table def pre-delete interceptors
pub struct TableDefPreDeleteContext<'a> {
	pub pre: &'a TableDef,
}

impl<'a> TableDefPreDeleteContext<'a> {
	pub fn new(pre: &'a TableDef) -> Self {
		Self {
			pre,
		}
	}
}

#[async_trait::async_trait]
pub trait TableDefPreDeleteInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut TableDefPreDeleteContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn TableDefPreDeleteInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: TableDefPreDeleteContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureTableDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTableDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTableDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> TableDefPreDeleteInterceptor for ClosureTableDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut TableDefPreDeleteContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_def_pre_delete<F>(f: F) -> ClosureTableDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTableDefPreDeleteInterceptor::new(f)
}
