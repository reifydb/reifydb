// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::dictionary::Dictionary;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

// DICTIONARY POST CREATE
/// Context for dictionary post-create interceptors
pub struct DictionaryPostCreateContext<'a> {
	pub post: &'a Dictionary,
}

impl<'a> DictionaryPostCreateContext<'a> {
	pub fn new(post: &'a Dictionary) -> Self {
		Self {
			post,
		}
	}
}

pub trait DictionaryPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryPostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryPostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryPostCreateInterceptor for ClosureDictionaryPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryPostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_post_create<F>(f: F) -> ClosureDictionaryPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryPostCreateInterceptor::new(f)
}

// DICTIONARY PRE UPDATE
/// Context for dictionary pre-update interceptors
pub struct DictionaryPreUpdateContext<'a> {
	pub pre: &'a Dictionary,
}

impl<'a> DictionaryPreUpdateContext<'a> {
	pub fn new(pre: &'a Dictionary) -> Self {
		Self {
			pre,
		}
	}
}

pub trait DictionaryPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryPreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryPreUpdateInterceptor for ClosureDictionaryPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_pre_update<F>(f: F) -> ClosureDictionaryPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryPreUpdateInterceptor::new(f)
}

// DICTIONARY POST UPDATE
/// Context for dictionary post-update interceptors
pub struct DictionaryPostUpdateContext<'a> {
	pub pre: &'a Dictionary,
	pub post: &'a Dictionary,
}

impl<'a> DictionaryPostUpdateContext<'a> {
	pub fn new(pre: &'a Dictionary, post: &'a Dictionary) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait DictionaryPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryPostUpdateInterceptor for ClosureDictionaryPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_post_update<F>(f: F) -> ClosureDictionaryPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryPostUpdateInterceptor::new(f)
}

// DICTIONARY PRE DELETE
/// Context for dictionary pre-delete interceptors
pub struct DictionaryPreDeleteContext<'a> {
	pub pre: &'a Dictionary,
}

impl<'a> DictionaryPreDeleteContext<'a> {
	pub fn new(pre: &'a Dictionary) -> Self {
		Self {
			pre,
		}
	}
}

pub trait DictionaryPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryPreDeleteInterceptor for ClosureDictionaryPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_pre_delete<F>(f: F) -> ClosureDictionaryPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryPreDeleteInterceptor::new(f)
}
