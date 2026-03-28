// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::namespace::Namespace;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

pub struct NamespacePostCreateContext<'a> {
	pub post: &'a Namespace,
}

impl<'a> NamespacePostCreateContext<'a> {
	pub fn new(post: &'a Namespace) -> Self {
		Self {
			post,
		}
	}
}

pub trait NamespacePostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut NamespacePostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn NamespacePostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: NamespacePostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureNamespacePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureNamespacePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureNamespacePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> NamespacePostCreateInterceptor for ClosureNamespacePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut NamespacePostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn namespace_post_create<F>(f: F) -> ClosureNamespacePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureNamespacePostCreateInterceptor::new(f)
}

pub struct NamespacePreUpdateContext<'a> {
	pub pre: &'a Namespace,
}

impl<'a> NamespacePreUpdateContext<'a> {
	pub fn new(pre: &'a Namespace) -> Self {
		Self {
			pre,
		}
	}
}

pub trait NamespacePreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut NamespacePreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn NamespacePreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: NamespacePreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureNamespacePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureNamespacePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureNamespacePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> NamespacePreUpdateInterceptor for ClosureNamespacePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut NamespacePreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn namespace_pre_update<F>(f: F) -> ClosureNamespacePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureNamespacePreUpdateInterceptor::new(f)
}

pub struct NamespacePostUpdateContext<'a> {
	pub pre: &'a Namespace,
	pub post: &'a Namespace,
}

impl<'a> NamespacePostUpdateContext<'a> {
	pub fn new(pre: &'a Namespace, post: &'a Namespace) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait NamespacePostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut NamespacePostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn NamespacePostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: NamespacePostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureNamespacePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureNamespacePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureNamespacePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> NamespacePostUpdateInterceptor for ClosureNamespacePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut NamespacePostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn namespace_post_update<F>(f: F) -> ClosureNamespacePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureNamespacePostUpdateInterceptor::new(f)
}

pub struct NamespacePreDeleteContext<'a> {
	pub pre: &'a Namespace,
}

impl<'a> NamespacePreDeleteContext<'a> {
	pub fn new(pre: &'a Namespace) -> Self {
		Self {
			pre,
		}
	}
}

pub trait NamespacePreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut NamespacePreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn NamespacePreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: NamespacePreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureNamespacePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureNamespacePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureNamespacePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> NamespacePreDeleteInterceptor for ClosureNamespacePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut NamespacePreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn namespace_pre_delete<F>(f: F) -> ClosureNamespacePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut NamespacePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureNamespacePreDeleteInterceptor::new(f)
}
