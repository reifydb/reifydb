// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::authentication::AuthenticationDef;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

// AUTHENTICATION DEF POST CREATE
/// Context for authentication def post-create interceptors
pub struct AuthenticationDefPostCreateContext<'a> {
	pub post: &'a AuthenticationDef,
}

impl<'a> AuthenticationDefPostCreateContext<'a> {
	pub fn new(post: &'a AuthenticationDef) -> Self {
		Self {
			post,
		}
	}
}

pub trait AuthenticationDefPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut AuthenticationDefPostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn AuthenticationDefPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: AuthenticationDefPostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureAuthenticationDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureAuthenticationDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureAuthenticationDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> AuthenticationDefPostCreateInterceptor for ClosureAuthenticationDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut AuthenticationDefPostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn authentication_def_post_create<F>(f: F) -> ClosureAuthenticationDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureAuthenticationDefPostCreateInterceptor::new(f)
}

// AUTHENTICATION DEF PRE DELETE
/// Context for authentication def pre-delete interceptors
pub struct AuthenticationDefPreDeleteContext<'a> {
	pub pre: &'a AuthenticationDef,
}

impl<'a> AuthenticationDefPreDeleteContext<'a> {
	pub fn new(pre: &'a AuthenticationDef) -> Self {
		Self {
			pre,
		}
	}
}

pub trait AuthenticationDefPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut AuthenticationDefPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn AuthenticationDefPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: AuthenticationDefPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureAuthenticationDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureAuthenticationDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureAuthenticationDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> AuthenticationDefPreDeleteInterceptor for ClosureAuthenticationDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut AuthenticationDefPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn authentication_def_pre_delete<F>(f: F) -> ClosureAuthenticationDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut AuthenticationDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureAuthenticationDefPreDeleteInterceptor::new(f)
}
