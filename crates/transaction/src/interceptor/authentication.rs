// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::authentication::Authentication;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

// AUTHENTICATION DEF POST CREATE
/// Context for authentication def post-create interceptors
pub struct AuthenticationPostCreateContext<'a> {
	pub post: &'a Authentication,
}

impl<'a> AuthenticationPostCreateContext<'a> {
	pub fn new(post: &'a Authentication) -> Self {
		Self {
			post,
		}
	}
}

pub trait AuthenticationPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut AuthenticationPostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn AuthenticationPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: AuthenticationPostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureAuthenticationPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureAuthenticationPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureAuthenticationPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> AuthenticationPostCreateInterceptor for ClosureAuthenticationPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut AuthenticationPostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn authentication_post_create<F>(f: F) -> ClosureAuthenticationPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureAuthenticationPostCreateInterceptor::new(f)
}

// AUTHENTICATION DEF PRE DELETE
/// Context for authentication def pre-delete interceptors
pub struct AuthenticationPreDeleteContext<'a> {
	pub pre: &'a Authentication,
}

impl<'a> AuthenticationPreDeleteContext<'a> {
	pub fn new(pre: &'a Authentication) -> Self {
		Self {
			pre,
		}
	}
}

pub trait AuthenticationPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut AuthenticationPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn AuthenticationPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: AuthenticationPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureAuthenticationPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureAuthenticationPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureAuthenticationPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> AuthenticationPreDeleteInterceptor for ClosureAuthenticationPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut AuthenticationPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn authentication_pre_delete<F>(f: F) -> ClosureAuthenticationPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureAuthenticationPreDeleteInterceptor::new(f)
}
