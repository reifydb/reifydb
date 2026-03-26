// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::identity::GrantedRole;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

// IDENTITY ROLE DEF POST CREATE
/// Context for identity-role def post-create interceptors
pub struct GrantedRolePostCreateContext<'a> {
	pub post: &'a GrantedRole,
}

impl<'a> GrantedRolePostCreateContext<'a> {
	pub fn new(post: &'a GrantedRole) -> Self {
		Self {
			post,
		}
	}
}

pub trait GrantedRolePostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut GrantedRolePostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn GrantedRolePostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: GrantedRolePostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureGrantedRolePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut GrantedRolePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureGrantedRolePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut GrantedRolePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureGrantedRolePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut GrantedRolePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> GrantedRolePostCreateInterceptor for ClosureGrantedRolePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut GrantedRolePostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut GrantedRolePostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn granted_role_post_create<F>(f: F) -> ClosureGrantedRolePostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut GrantedRolePostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureGrantedRolePostCreateInterceptor::new(f)
}

// IDENTITY ROLE DEF PRE DELETE
/// Context for identity-role def pre-delete interceptors
pub struct GrantedRolePreDeleteContext<'a> {
	pub pre: &'a GrantedRole,
}

impl<'a> GrantedRolePreDeleteContext<'a> {
	pub fn new(pre: &'a GrantedRole) -> Self {
		Self {
			pre,
		}
	}
}

pub trait GrantedRolePreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut GrantedRolePreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn GrantedRolePreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: GrantedRolePreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureGrantedRolePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut GrantedRolePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureGrantedRolePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut GrantedRolePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureGrantedRolePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut GrantedRolePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> GrantedRolePreDeleteInterceptor for ClosureGrantedRolePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut GrantedRolePreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut GrantedRolePreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn granted_role_pre_delete<F>(f: F) -> ClosureGrantedRolePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut GrantedRolePreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureGrantedRolePreDeleteInterceptor::new(f)
}
