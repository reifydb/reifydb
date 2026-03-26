// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::identity::IdentityRoleDef;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

// IDENTITY ROLE DEF POST CREATE
/// Context for identity-role def post-create interceptors
pub struct IdentityRoleDefPostCreateContext<'a> {
	pub post: &'a IdentityRoleDef,
}

impl<'a> IdentityRoleDefPostCreateContext<'a> {
	pub fn new(post: &'a IdentityRoleDef) -> Self {
		Self {
			post,
		}
	}
}

pub trait IdentityRoleDefPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityRoleDefPostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityRoleDefPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityRoleDefPostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityRoleDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityRoleDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityRoleDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityRoleDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityRoleDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityRoleDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityRoleDefPostCreateInterceptor for ClosureIdentityRoleDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityRoleDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityRoleDefPostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_role_def_post_create<F>(f: F) -> ClosureIdentityRoleDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityRoleDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityRoleDefPostCreateInterceptor::new(f)
}

// IDENTITY ROLE DEF PRE DELETE
/// Context for identity-role def pre-delete interceptors
pub struct IdentityRoleDefPreDeleteContext<'a> {
	pub pre: &'a IdentityRoleDef,
}

impl<'a> IdentityRoleDefPreDeleteContext<'a> {
	pub fn new(pre: &'a IdentityRoleDef) -> Self {
		Self {
			pre,
		}
	}
}

pub trait IdentityRoleDefPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut IdentityRoleDefPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn IdentityRoleDefPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: IdentityRoleDefPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureIdentityRoleDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityRoleDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureIdentityRoleDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityRoleDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureIdentityRoleDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityRoleDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> IdentityRoleDefPreDeleteInterceptor for ClosureIdentityRoleDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityRoleDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut IdentityRoleDefPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn identity_role_def_pre_delete<F>(f: F) -> ClosureIdentityRoleDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut IdentityRoleDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureIdentityRoleDefPreDeleteInterceptor::new(f)
}
