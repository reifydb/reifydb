// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::identity::IdentityDef;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

// IDENTITY DEF POST CREATE
/// Context for identity def post-create interceptors
pub struct IdentityDefPostCreateContext<'a> {
	pub post: &'a IdentityDef,
}

impl<'a> IdentityDefPostCreateContext<'a> {
	pub fn new(post: &'a IdentityDef) -> Self {
		Self {
			post,
		}
	}
}

pub trait IdentityDefPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityDefPostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityDefPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityDefPostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityDefPostCreateInterceptor for ClosureIdentityDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityDefPostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_def_post_create<F>(f: F) -> ClosureIdentityDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityDefPostCreateInterceptor::new(f)
}

// IDENTITY DEF PRE UPDATE
/// Context for identity def pre-update interceptors
pub struct IdentityDefPreUpdateContext<'a> {
	pub pre: &'a IdentityDef,
}

impl<'a> IdentityDefPreUpdateContext<'a> {
	pub fn new(pre: &'a IdentityDef) -> Self {
		Self {
			pre,
		}
	}
}

pub trait IdentityDefPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityDefPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityDefPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityDefPreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityDefPreUpdateInterceptor for ClosureIdentityDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityDefPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_def_pre_update<F>(f: F) -> ClosureIdentityDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityDefPreUpdateInterceptor::new(f)
}

// IDENTITY DEF POST UPDATE
/// Context for identity def post-update interceptors
pub struct IdentityDefPostUpdateContext<'a> {
	pub pre: &'a IdentityDef,
	pub post: &'a IdentityDef,
}

impl<'a> IdentityDefPostUpdateContext<'a> {
	pub fn new(pre: &'a IdentityDef, post: &'a IdentityDef) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait IdentityDefPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityDefPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityDefPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityDefPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityDefPostUpdateInterceptor for ClosureIdentityDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityDefPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_def_post_update<F>(f: F) -> ClosureIdentityDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityDefPostUpdateInterceptor::new(f)
}

// IDENTITY DEF PRE DELETE
/// Context for identity def pre-delete interceptors
pub struct IdentityDefPreDeleteContext<'a> {
	pub pre: &'a IdentityDef,
}

impl<'a> IdentityDefPreDeleteContext<'a> {
	pub fn new(pre: &'a IdentityDef) -> Self {
		Self {
			pre,
		}
	}
}

pub trait IdentityDefPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityDefPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityDefPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityDefPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityDefPreDeleteInterceptor for ClosureIdentityDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityDefPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_def_pre_delete<F>(f: F) -> ClosureIdentityDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityDefPreDeleteInterceptor::new(f)
}
