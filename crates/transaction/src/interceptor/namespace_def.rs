// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::NamespaceDef;

use crate::interceptor::InterceptorChain;

// NAMESPACE POST CREATE
/// Context for namespace post-create interceptors
pub struct NamespaceDefPostCreateContext<'a> {
	pub post: &'a NamespaceDef,
}

impl<'a> NamespaceDefPostCreateContext<'a> {
	pub fn new(post: &'a NamespaceDef) -> Self {
		Self {
			post,
		}
	}
}

#[async_trait::async_trait]
pub trait NamespaceDefPostCreateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut NamespaceDefPostCreateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn NamespaceDefPostCreateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: NamespaceDefPostCreateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureNamespaceDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureNamespaceDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureNamespaceDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> NamespaceDefPostCreateInterceptor for ClosureNamespaceDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut NamespaceDefPostCreateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn namespace_def_post_create<F>(f: F) -> ClosureNamespaceDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostCreateContext<'a>) -> reifydb_core::Result<()>
		+ Send
		+ Sync
		+ Clone
		+ 'static,
{
	ClosureNamespaceDefPostCreateInterceptor::new(f)
}

// NAMESPACE PRE UPDATE
/// Context for namespace pre-update interceptors
pub struct NamespaceDefPreUpdateContext<'a> {
	pub pre: &'a NamespaceDef,
}

impl<'a> NamespaceDefPreUpdateContext<'a> {
	pub fn new(pre: &'a NamespaceDef) -> Self {
		Self {
			pre,
		}
	}
}

#[async_trait::async_trait]
pub trait NamespaceDefPreUpdateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut NamespaceDefPreUpdateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn NamespaceDefPreUpdateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: NamespaceDefPreUpdateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureNamespaceDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureNamespaceDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureNamespaceDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> NamespaceDefPreUpdateInterceptor for ClosureNamespaceDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut NamespaceDefPreUpdateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn namespace_def_pre_update<F>(f: F) -> ClosureNamespaceDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreUpdateContext<'a>) -> reifydb_core::Result<()>
		+ Send
		+ Sync
		+ Clone
		+ 'static,
{
	ClosureNamespaceDefPreUpdateInterceptor::new(f)
}

// NAMESPACE POST UPDATE
/// Context for namespace post-update interceptors
pub struct NamespaceDefPostUpdateContext<'a> {
	pub pre: &'a NamespaceDef,
	pub post: &'a NamespaceDef,
}

impl<'a> NamespaceDefPostUpdateContext<'a> {
	pub fn new(pre: &'a NamespaceDef, post: &'a NamespaceDef) -> Self {
		Self {
			pre,
			post,
		}
	}
}

#[async_trait::async_trait]
pub trait NamespaceDefPostUpdateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut NamespaceDefPostUpdateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn NamespaceDefPostUpdateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: NamespaceDefPostUpdateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureNamespaceDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureNamespaceDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureNamespaceDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> NamespaceDefPostUpdateInterceptor for ClosureNamespaceDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut NamespaceDefPostUpdateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn namespace_def_post_update<F>(f: F) -> ClosureNamespaceDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostUpdateContext<'a>) -> reifydb_core::Result<()>
		+ Send
		+ Sync
		+ Clone
		+ 'static,
{
	ClosureNamespaceDefPostUpdateInterceptor::new(f)
}

// NAMESPACE PRE DELETE
/// Context for namespace pre-delete interceptors
pub struct NamespaceDefPreDeleteContext<'a> {
	pub pre: &'a NamespaceDef,
}

impl<'a> NamespaceDefPreDeleteContext<'a> {
	pub fn new(pre: &'a NamespaceDef) -> Self {
		Self {
			pre,
		}
	}
}

#[async_trait::async_trait]
pub trait NamespaceDefPreDeleteInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut NamespaceDefPreDeleteContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn NamespaceDefPreDeleteInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: NamespaceDefPreDeleteContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureNamespaceDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureNamespaceDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureNamespaceDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> NamespaceDefPreDeleteInterceptor for ClosureNamespaceDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut NamespaceDefPreDeleteContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn namespace_def_pre_delete<F>(f: F) -> ClosureNamespaceDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreDeleteContext<'a>) -> reifydb_core::Result<()>
		+ Send
		+ Sync
		+ Clone
		+ 'static,
{
	ClosureNamespaceDefPreDeleteInterceptor::new(f)
}
