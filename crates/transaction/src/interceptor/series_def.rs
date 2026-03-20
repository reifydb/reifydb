// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::series::SeriesDef;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

// SERIES POST CREATE
/// Context for series post-create interceptors
pub struct SeriesDefPostCreateContext<'a> {
	pub post: &'a SeriesDef,
}

impl<'a> SeriesDefPostCreateContext<'a> {
	pub fn new(post: &'a SeriesDef) -> Self {
		Self {
			post,
		}
	}
}

pub trait SeriesDefPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesDefPostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesDefPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesDefPostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesDefPostCreateInterceptor for ClosureSeriesDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesDefPostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_def_post_create<F>(f: F) -> ClosureSeriesDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesDefPostCreateInterceptor::new(f)
}

// SERIES PRE UPDATE
/// Context for series pre-update interceptors
pub struct SeriesDefPreUpdateContext<'a> {
	pub pre: &'a SeriesDef,
}

impl<'a> SeriesDefPreUpdateContext<'a> {
	pub fn new(pre: &'a SeriesDef) -> Self {
		Self {
			pre,
		}
	}
}

pub trait SeriesDefPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesDefPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesDefPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesDefPreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesDefPreUpdateInterceptor for ClosureSeriesDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesDefPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_def_pre_update<F>(f: F) -> ClosureSeriesDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesDefPreUpdateInterceptor::new(f)
}

// SERIES POST UPDATE
/// Context for series post-update interceptors
pub struct SeriesDefPostUpdateContext<'a> {
	pub pre: &'a SeriesDef,
	pub post: &'a SeriesDef,
}

impl<'a> SeriesDefPostUpdateContext<'a> {
	pub fn new(pre: &'a SeriesDef, post: &'a SeriesDef) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait SeriesDefPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesDefPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesDefPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesDefPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesDefPostUpdateInterceptor for ClosureSeriesDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesDefPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_def_post_update<F>(f: F) -> ClosureSeriesDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesDefPostUpdateInterceptor::new(f)
}

// SERIES PRE DELETE
/// Context for series pre-delete interceptors
pub struct SeriesDefPreDeleteContext<'a> {
	pub pre: &'a SeriesDef,
}

impl<'a> SeriesDefPreDeleteContext<'a> {
	pub fn new(pre: &'a SeriesDef) -> Self {
		Self {
			pre,
		}
	}
}

pub trait SeriesDefPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesDefPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesDefPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesDefPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesDefPreDeleteInterceptor for ClosureSeriesDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesDefPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_def_pre_delete<F>(f: F) -> ClosureSeriesDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesDefPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesDefPreDeleteInterceptor::new(f)
}
