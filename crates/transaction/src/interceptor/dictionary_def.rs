// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::dictionary::DictionaryDef;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

// DICTIONARY POST CREATE
/// Context for dictionary post-create interceptors
pub struct DictionaryDefPostCreateContext<'a> {
	pub post: &'a DictionaryDef,
}

impl<'a> DictionaryDefPostCreateContext<'a> {
	pub fn new(post: &'a DictionaryDef) -> Self {
		Self {
			post,
		}
	}
}

pub trait DictionaryDefPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryDefPostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryDefPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryDefPostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryDefPostCreateInterceptor for ClosureDictionaryDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryDefPostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_def_post_create<F>(f: F) -> ClosureDictionaryDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryDefPostCreateInterceptor::new(f)
}

// DICTIONARY PRE UPDATE
/// Context for dictionary pre-update interceptors
pub struct DictionaryDefPreUpdateContext<'a> {
	pub pre: &'a DictionaryDef,
}

impl<'a> DictionaryDefPreUpdateContext<'a> {
	pub fn new(pre: &'a DictionaryDef) -> Self {
		Self {
			pre,
		}
	}
}

pub trait DictionaryDefPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryDefPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryDefPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryDefPreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryDefPreUpdateInterceptor for ClosureDictionaryDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryDefPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_def_pre_update<F>(f: F) -> ClosureDictionaryDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryDefPreUpdateInterceptor::new(f)
}

// DICTIONARY POST UPDATE
/// Context for dictionary post-update interceptors
pub struct DictionaryDefPostUpdateContext<'a> {
	pub pre: &'a DictionaryDef,
	pub post: &'a DictionaryDef,
}

impl<'a> DictionaryDefPostUpdateContext<'a> {
	pub fn new(pre: &'a DictionaryDef, post: &'a DictionaryDef) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait DictionaryDefPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryDefPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryDefPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryDefPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryDefPostUpdateInterceptor for ClosureDictionaryDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryDefPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_def_post_update<F>(f: F) -> ClosureDictionaryDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryDefPostUpdateInterceptor::new(f)
}

// DICTIONARY PRE DELETE
/// Context for dictionary pre-delete interceptors
pub struct DictionaryDefPreDeleteContext<'a> {
	pub pre: &'a DictionaryDef,
}

impl<'a> DictionaryDefPreDeleteContext<'a> {
	pub fn new(pre: &'a DictionaryDef) -> Self {
		Self {
			pre,
		}
	}
}

pub trait DictionaryDefPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut DictionaryDefPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn DictionaryDefPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: DictionaryDefPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureDictionaryDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureDictionaryDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureDictionaryDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> DictionaryDefPreDeleteInterceptor for ClosureDictionaryDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut DictionaryDefPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn dictionary_def_pre_delete<F>(f: F) -> ClosureDictionaryDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut DictionaryDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureDictionaryDefPreDeleteInterceptor::new(f)
}
