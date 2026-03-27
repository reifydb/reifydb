// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::series::Series;
use reifydb_type::Result;

use crate::interceptor::chain::InterceptorChain;

pub struct SeriesPostCreateContext<'a> {
	pub post: &'a Series,
}

impl<'a> SeriesPostCreateContext<'a> {
	pub fn new(post: &'a Series) -> Self {
		Self {
			post,
		}
	}
}

pub trait SeriesPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesPostCreateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesPostCreateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesPostCreateInterceptor for ClosureSeriesPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostCreateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesPostCreateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_post_create<F>(f: F) -> ClosureSeriesPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostCreateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesPostCreateInterceptor::new(f)
}

pub struct SeriesPreUpdateContext<'a> {
	pub pre: &'a Series,
}

impl<'a> SeriesPreUpdateContext<'a> {
	pub fn new(pre: &'a Series) -> Self {
		Self {
			pre,
		}
	}
}

pub trait SeriesPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesPreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesPreUpdateInterceptor for ClosureSeriesPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_pre_update<F>(f: F) -> ClosureSeriesPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesPreUpdateInterceptor::new(f)
}

pub struct SeriesPostUpdateContext<'a> {
	pub pre: &'a Series,
	pub post: &'a Series,
}

impl<'a> SeriesPostUpdateContext<'a> {
	pub fn new(pre: &'a Series, post: &'a Series) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait SeriesPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesPostUpdateInterceptor for ClosureSeriesPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_post_update<F>(f: F) -> ClosureSeriesPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesPostUpdateInterceptor::new(f)
}

pub struct SeriesPreDeleteContext<'a> {
	pub pre: &'a Series,
}

impl<'a> SeriesPreDeleteContext<'a> {
	pub fn new(pre: &'a Series) -> Self {
		Self {
			pre,
		}
	}
}

pub trait SeriesPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesPreDeleteInterceptor for ClosureSeriesPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_pre_delete<F>(f: F) -> ClosureSeriesPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesPreDeleteInterceptor::new(f)
}
