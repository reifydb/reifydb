// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::identity::Role;
use reifydb_value::Result;

use crate::interceptor::chain::InterceptorChain;

pub struct RolePostCreateContext<'a> {
	pub post: &'a Role,
}

impl<'a> RolePostCreateContext<'a> {
	pub fn new(post: &'a Role) -> Self {
		Self {
			post,
		}
	}
}

pub trait RolePostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RolePostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RolePostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RolePostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRolePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRolePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRolePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RolePostCreateInterceptor for ClosureRolePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RolePostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn role_post_create<F>(f: F) -> ClosureRolePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRolePostCreateInterceptor::new(f)
}

pub struct RolePreDeleteContext<'a> {
	pub pre: &'a Role,
}

impl<'a> RolePreDeleteContext<'a> {
	pub fn new(pre: &'a Role) -> Self {
		Self {
			pre,
		}
	}
}

pub trait RolePreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RolePreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RolePreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RolePreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRolePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RolePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRolePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RolePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRolePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RolePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RolePreDeleteInterceptor for ClosureRolePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RolePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RolePreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn role_pre_delete<F>(f: F) -> ClosureRolePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RolePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRolePreDeleteInterceptor::new(f)
}
