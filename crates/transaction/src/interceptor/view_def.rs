// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::ViewDef;

use crate::interceptor::InterceptorChain;

// VIEW DEF POST CREATE
/// Context for view def post-create interceptors
pub struct ViewDefPostCreateContext<'a> {
	pub post: &'a ViewDef,
}

impl<'a> ViewDefPostCreateContext<'a> {
	pub fn new(post: &'a ViewDef) -> Self {
		Self {
			post,
		}
	}
}

#[async_trait::async_trait]
pub trait ViewDefPostCreateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut ViewDefPostCreateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn ViewDefPostCreateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: ViewDefPostCreateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureViewDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> ViewDefPostCreateInterceptor for ClosureViewDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut ViewDefPostCreateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_def_post_create<F>(f: F) -> ClosureViewDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewDefPostCreateInterceptor::new(f)
}

// VIEW DEF PRE UPDATE
/// Context for view def pre-update interceptors
pub struct ViewDefPreUpdateContext<'a> {
	pub pre: &'a ViewDef,
}

impl<'a> ViewDefPreUpdateContext<'a> {
	pub fn new(pre: &'a ViewDef) -> Self {
		Self {
			pre,
		}
	}
}

#[async_trait::async_trait]
pub trait ViewDefPreUpdateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut ViewDefPreUpdateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn ViewDefPreUpdateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: ViewDefPreUpdateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureViewDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> ViewDefPreUpdateInterceptor for ClosureViewDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut ViewDefPreUpdateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_def_pre_update<F>(f: F) -> ClosureViewDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewDefPreUpdateInterceptor::new(f)
}

// VIEW DEF POST UPDATE
/// Context for view def post-update interceptors
pub struct ViewDefPostUpdateContext<'a> {
	pub pre: &'a ViewDef,
	pub post: &'a ViewDef,
}

impl<'a> ViewDefPostUpdateContext<'a> {
	pub fn new(pre: &'a ViewDef, post: &'a ViewDef) -> Self {
		Self {
			pre,
			post,
		}
	}
}

#[async_trait::async_trait]
pub trait ViewDefPostUpdateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut ViewDefPostUpdateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn ViewDefPostUpdateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: ViewDefPostUpdateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureViewDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> ViewDefPostUpdateInterceptor for ClosureViewDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut ViewDefPostUpdateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_def_post_update<F>(f: F) -> ClosureViewDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewDefPostUpdateInterceptor::new(f)
}

// VIEW DEF PRE DELETE
/// Context for view def pre-delete interceptors
pub struct ViewDefPreDeleteContext<'a> {
	pub pre: &'a ViewDef,
}

impl<'a> ViewDefPreDeleteContext<'a> {
	pub fn new(pre: &'a ViewDef) -> Self {
		Self {
			pre,
		}
	}
}

#[async_trait::async_trait]
pub trait ViewDefPreDeleteInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut ViewDefPreDeleteContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn ViewDefPreDeleteInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: ViewDefPreDeleteContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureViewDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> ViewDefPreDeleteInterceptor for ClosureViewDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut ViewDefPreDeleteContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_def_pre_delete<F>(f: F) -> ClosureViewDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewDefPreDeleteInterceptor::new(f)
}
