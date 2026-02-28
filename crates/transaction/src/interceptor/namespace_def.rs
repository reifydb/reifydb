// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::namespace::NamespaceDef;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

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

pub trait NamespaceDefPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut NamespaceDefPostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn NamespaceDefPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: NamespaceDefPostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureNamespaceDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureNamespaceDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureNamespaceDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> NamespaceDefPostCreateInterceptor for ClosureNamespaceDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut NamespaceDefPostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn namespace_def_post_create<F>(f: F) -> ClosureNamespaceDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
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

pub trait NamespaceDefPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut NamespaceDefPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn NamespaceDefPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: NamespaceDefPreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureNamespaceDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureNamespaceDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureNamespaceDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> NamespaceDefPreUpdateInterceptor for ClosureNamespaceDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut NamespaceDefPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn namespace_def_pre_update<F>(f: F) -> ClosureNamespaceDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
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

pub trait NamespaceDefPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut NamespaceDefPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn NamespaceDefPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: NamespaceDefPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureNamespaceDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureNamespaceDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureNamespaceDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> NamespaceDefPostUpdateInterceptor for ClosureNamespaceDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut NamespaceDefPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn namespace_def_post_update<F>(f: F) -> ClosureNamespaceDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
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

pub trait NamespaceDefPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut NamespaceDefPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn NamespaceDefPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: NamespaceDefPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureNamespaceDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureNamespaceDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureNamespaceDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> NamespaceDefPreDeleteInterceptor for ClosureNamespaceDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut NamespaceDefPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn namespace_def_pre_delete<F>(f: F) -> ClosureNamespaceDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespaceDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureNamespaceDefPreDeleteInterceptor::new(f)
}
