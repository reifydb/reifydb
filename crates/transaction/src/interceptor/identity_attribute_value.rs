// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::identity::IdentityAttributeValue;
use reifydb_value::Result;

use crate::interceptor::chain::InterceptorChain;

pub struct IdentityAttributeValuePostCreateContext<'a> {
	pub post: &'a IdentityAttributeValue,
}

impl<'a> IdentityAttributeValuePostCreateContext<'a> {
	pub fn new(post: &'a IdentityAttributeValue) -> Self {
		Self {
			post,
		}
	}
}

pub trait IdentityAttributeValuePostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityAttributeValuePostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityAttributeValuePostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityAttributeValuePostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityAttributeValuePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributeValuePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityAttributeValuePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributeValuePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityAttributeValuePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributeValuePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityAttributeValuePostCreateInterceptor for ClosureIdentityAttributeValuePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributeValuePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityAttributeValuePostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_attribute_value_post_create<F>(f: F) -> ClosureIdentityAttributeValuePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributeValuePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityAttributeValuePostCreateInterceptor::new(f)
}

pub struct IdentityAttributeValuePreDeleteContext<'a> {
	pub pre: &'a IdentityAttributeValue,
}

impl<'a> IdentityAttributeValuePreDeleteContext<'a> {
	pub fn new(pre: &'a IdentityAttributeValue) -> Self {
		Self {
			pre,
		}
	}
}

pub trait IdentityAttributeValuePreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityAttributeValuePreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityAttributeValuePreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityAttributeValuePreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityAttributeValuePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributeValuePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityAttributeValuePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributeValuePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityAttributeValuePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributeValuePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityAttributeValuePreDeleteInterceptor for ClosureIdentityAttributeValuePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributeValuePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityAttributeValuePreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_attribute_value_pre_delete<F>(f: F) -> ClosureIdentityAttributeValuePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityAttributeValuePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityAttributeValuePreDeleteInterceptor::new(f)
}
