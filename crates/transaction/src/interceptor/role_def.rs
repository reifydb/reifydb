// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::identity::RoleDef;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

// ROLE DEF POST CREATE
/// Context for role def post-create interceptors
pub struct RoleDefPostCreateContext<'a> {
	pub post: &'a RoleDef,
}

impl<'a> RoleDefPostCreateContext<'a> {
	pub fn new(post: &'a RoleDef) -> Self {
		Self {
			post,
		}
	}
}

pub trait RoleDefPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RoleDefPostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RoleDefPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RoleDefPostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRoleDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRoleDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRoleDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RoleDefPostCreateInterceptor for ClosureRoleDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RoleDefPostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn role_def_post_create<F>(f: F) -> ClosureRoleDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRoleDefPostCreateInterceptor::new(f)
}

// ROLE DEF PRE UPDATE
/// Context for role def pre-update interceptors
pub struct RoleDefPreUpdateContext<'a> {
	pub pre: &'a RoleDef,
}

impl<'a> RoleDefPreUpdateContext<'a> {
	pub fn new(pre: &'a RoleDef) -> Self {
		Self {
			pre,
		}
	}
}

pub trait RoleDefPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RoleDefPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RoleDefPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RoleDefPreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRoleDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRoleDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRoleDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RoleDefPreUpdateInterceptor for ClosureRoleDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RoleDefPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn role_def_pre_update<F>(f: F) -> ClosureRoleDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRoleDefPreUpdateInterceptor::new(f)
}

// ROLE DEF POST UPDATE
/// Context for role def post-update interceptors
pub struct RoleDefPostUpdateContext<'a> {
	pub pre: &'a RoleDef,
	pub post: &'a RoleDef,
}

impl<'a> RoleDefPostUpdateContext<'a> {
	pub fn new(pre: &'a RoleDef, post: &'a RoleDef) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait RoleDefPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RoleDefPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RoleDefPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RoleDefPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRoleDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRoleDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRoleDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RoleDefPostUpdateInterceptor for ClosureRoleDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RoleDefPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn role_def_post_update<F>(f: F) -> ClosureRoleDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRoleDefPostUpdateInterceptor::new(f)
}

// ROLE DEF PRE DELETE
/// Context for role def pre-delete interceptors
pub struct RoleDefPreDeleteContext<'a> {
	pub pre: &'a RoleDef,
}

impl<'a> RoleDefPreDeleteContext<'a> {
	pub fn new(pre: &'a RoleDef) -> Self {
		Self {
			pre,
		}
	}
}

pub trait RoleDefPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RoleDefPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RoleDefPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RoleDefPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRoleDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRoleDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRoleDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RoleDefPreDeleteInterceptor for ClosureRoleDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RoleDefPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn role_def_pre_delete<F>(f: F) -> ClosureRoleDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RoleDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRoleDefPreDeleteInterceptor::new(f)
}
