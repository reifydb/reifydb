// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::identity::Identity;
use reifydb_value::Result;

use crate::interceptor::chain::InterceptorChain;

pub struct IdentityPostCreateContext<'a> {
	pub post: &'a Identity,
}

impl<'a> IdentityPostCreateContext<'a> {
	pub fn new(post: &'a Identity) -> Self {
		Self {
			post,
		}
	}
}

pub trait IdentityPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityPostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityPostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityPostCreateInterceptor for ClosureIdentityPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityPostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_post_create<F>(f: F) -> ClosureIdentityPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityPostCreateInterceptor::new(f)
}

pub struct IdentityPreDeleteContext<'a> {
	pub pre: &'a Identity,
}

impl<'a> IdentityPreDeleteContext<'a> {
	pub fn new(pre: &'a Identity) -> Self {
		Self {
			pre,
		}
	}
}

pub trait IdentityPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityPreDeleteInterceptor for ClosureIdentityPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_pre_delete<F>(f: F) -> ClosureIdentityPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityPreDeleteInterceptor::new(f)
}
