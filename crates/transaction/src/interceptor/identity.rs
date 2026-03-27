// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::identity::Identity;
use reifydb_type::Result;

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

pub struct IdentityPreUpdateContext<'a> {
	pub pre: &'a Identity,
}

impl<'a> IdentityPreUpdateContext<'a> {
	pub fn new(pre: &'a Identity) -> Self {
		Self {
			pre,
		}
	}
}

pub trait IdentityPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityPreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityPreUpdateInterceptor for ClosureIdentityPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_pre_update<F>(f: F) -> ClosureIdentityPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityPreUpdateInterceptor::new(f)
}

pub struct IdentityPostUpdateContext<'a> {
	pub pre: &'a Identity,
	pub post: &'a Identity,
}

impl<'a> IdentityPostUpdateContext<'a> {
	pub fn new(pre: &'a Identity, post: &'a Identity) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait IdentityPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityPostUpdateInterceptor for ClosureIdentityPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_post_update<F>(f: F) -> ClosureIdentityPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityPostUpdateInterceptor::new(f)
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
