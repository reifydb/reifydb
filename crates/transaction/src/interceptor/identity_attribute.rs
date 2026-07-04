// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::identity::IdentityAttribute;
use reifydb_value::Result;

use crate::interceptor::chain::InterceptorChain;

pub struct IdentityAttributePostCreateContext<'a> {
	pub post: &'a IdentityAttribute,
}

impl<'a> IdentityAttributePostCreateContext<'a> {
	pub fn new(post: &'a IdentityAttribute) -> Self {
		Self {
			post,
		}
	}
}

pub trait IdentityAttributePostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityAttributePostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityAttributePostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityAttributePostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityAttributePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityAttributePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityAttributePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityAttributePostCreateInterceptor for ClosureIdentityAttributePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityAttributePostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_attribute_post_create<F>(f: F) -> ClosureIdentityAttributePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityAttributePostCreateInterceptor::new(f)
}

pub struct IdentityAttributePreDeleteContext<'a> {
	pub pre: &'a IdentityAttribute,
}

impl<'a> IdentityAttributePreDeleteContext<'a> {
	pub fn new(pre: &'a IdentityAttribute) -> Self {
		Self {
			pre,
		}
	}
}

pub trait IdentityAttributePreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityAttributePreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityAttributePreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityAttributePreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityAttributePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityAttributePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityAttributePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityAttributePreDeleteInterceptor for ClosureIdentityAttributePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityAttributePreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_attribute_pre_delete<F>(f: F) -> ClosureIdentityAttributePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityAttributePreDeleteInterceptor::new(f)
}
