// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::identity::Role;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

// ROLE DEF POST CREATE
/// Context for role def post-create interceptors
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

// ROLE DEF PRE UPDATE
/// Context for role def pre-update interceptors
pub struct RolePreUpdateContext<'a> {
	pub pre: &'a Role,
}

impl<'a> RolePreUpdateContext<'a> {
	pub fn new(pre: &'a Role) -> Self {
		Self {
			pre,
		}
	}
}

pub trait RolePreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RolePreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RolePreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RolePreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRolePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRolePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRolePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RolePreUpdateInterceptor for ClosureRolePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RolePreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn role_pre_update<F>(f: F) -> ClosureRolePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRolePreUpdateInterceptor::new(f)
}

// ROLE DEF POST UPDATE
/// Context for role def post-update interceptors
pub struct RolePostUpdateContext<'a> {
	pub pre: &'a Role,
	pub post: &'a Role,
}

impl<'a> RolePostUpdateContext<'a> {
	pub fn new(pre: &'a Role, post: &'a Role) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait RolePostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RolePostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RolePostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RolePostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRolePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRolePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRolePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RolePostUpdateInterceptor for ClosureRolePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RolePostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn role_post_update<F>(f: F) -> ClosureRolePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RolePostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRolePostUpdateInterceptor::new(f)
}

// ROLE DEF PRE DELETE
/// Context for role def pre-delete interceptors
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
