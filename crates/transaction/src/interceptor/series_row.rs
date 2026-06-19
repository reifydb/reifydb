// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{encoded::row::EncodedRow, interface::catalog::series::Series};
use reifydb_value::Result;

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

pub struct SeriesRowPreInsertContext<'a> {
	pub series: &'a Series,
	pub rows: &'a mut [EncodedRow],
}

impl<'a> SeriesRowPreInsertContext<'a> {
	pub fn new(series: &'a Series, rows: &'a mut [EncodedRow]) -> Self {
		Self {
			series,
			rows,
		}
	}
}

pub trait SeriesRowPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesRowPreInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesRowPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesRowPreInsertContext) -> Result<()> {
		let original_len = ctx.rows.len();
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
			assert_eq!(ctx.rows.len(), original_len, "pre_insert interceptor changed row count");
		}
		Ok(())
	}
}

pub struct ClosureSeriesRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesRowPreInsertInterceptor for ClosureSeriesRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesRowPreInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_row_pre_insert<F>(f: F) -> ClosureSeriesRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesRowPreInsertInterceptor::new(f)
}

pub struct SeriesRowPostInsertContext<'a> {
	pub series: &'a Series,
	pub rows: &'a [EncodedRow],
}

impl<'a> SeriesRowPostInsertContext<'a> {
	pub fn new(series: &'a Series, rows: &'a [EncodedRow]) -> Self {
		Self {
			series,
			rows,
		}
	}
}

pub trait SeriesRowPostInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesRowPostInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesRowPostInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesRowPostInsertContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesRowPostInsertInterceptor for ClosureSeriesRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesRowPostInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_row_post_insert<F>(f: F) -> ClosureSeriesRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesRowPostInsertInterceptor::new(f)
}

pub struct SeriesRowPreUpdateContext<'a> {
	pub series: &'a Series,
	pub rows: &'a mut [EncodedRow],
}

impl<'a> SeriesRowPreUpdateContext<'a> {
	pub fn new(series: &'a Series, rows: &'a mut [EncodedRow]) -> Self {
		Self {
			series,
			rows,
		}
	}
}

pub trait SeriesRowPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesRowPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesRowPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesRowPreUpdateContext) -> Result<()> {
		let original_len = ctx.rows.len();
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
			assert_eq!(ctx.rows.len(), original_len, "pre_update interceptor changed row count");
		}
		Ok(())
	}
}

pub struct ClosureSeriesRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesRowPreUpdateInterceptor for ClosureSeriesRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesRowPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_row_pre_update<F>(f: F) -> ClosureSeriesRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesRowPreUpdateInterceptor::new(f)
}

pub struct SeriesRowPostUpdateContext<'a> {
	pub series: &'a Series,
	pub posts: &'a [EncodedRow],
	pub pres: &'a [EncodedRow],
}

impl<'a> SeriesRowPostUpdateContext<'a> {
	pub fn new(series: &'a Series, posts: &'a [EncodedRow], pres: &'a [EncodedRow]) -> Self {
		assert_eq!(posts.len(), pres.len(), "posts/pres length mismatch");
		Self {
			series,
			posts,
			pres,
		}
	}
}

pub trait SeriesRowPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesRowPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesRowPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesRowPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesRowPostUpdateInterceptor for ClosureSeriesRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesRowPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_row_post_update<F>(f: F) -> ClosureSeriesRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesRowPostUpdateInterceptor::new(f)
}

pub struct SeriesRowPreDeleteContext<'a> {
	pub series: &'a Series,
}

impl<'a> SeriesRowPreDeleteContext<'a> {
	pub fn new(series: &'a Series) -> Self {
		Self {
			series,
		}
	}
}

pub trait SeriesRowPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesRowPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesRowPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesRowPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesRowPreDeleteInterceptor for ClosureSeriesRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesRowPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_row_pre_delete<F>(f: F) -> ClosureSeriesRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesRowPreDeleteInterceptor::new(f)
}

pub struct SeriesRowPostDeleteContext<'a> {
	pub series: &'a Series,
	pub deleted_rows: &'a [EncodedRow],
}

impl<'a> SeriesRowPostDeleteContext<'a> {
	pub fn new(series: &'a Series, deleted_rows: &'a [EncodedRow]) -> Self {
		Self {
			series,
			deleted_rows,
		}
	}
}

pub trait SeriesRowPostDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesRowPostDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesRowPostDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesRowPostDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesRowPostDeleteInterceptor for ClosureSeriesRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesRowPostDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_row_post_delete<F>(f: F) -> ClosureSeriesRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesRowPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesRowPostDeleteInterceptor::new(f)
}

pub struct SeriesRowInterceptor;

impl SeriesRowInterceptor {
	pub fn pre_insert(txn: &mut impl WithInterceptors, series: &Series, rows: &mut [EncodedRow]) -> Result<()> {
		let ctx = SeriesRowPreInsertContext::new(series, rows);
		txn.series_row_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(txn: &mut impl WithInterceptors, series: &Series, rows: &[EncodedRow]) -> Result<()> {
		let ctx = SeriesRowPostInsertContext::new(series, rows);
		txn.series_row_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(txn: &mut impl WithInterceptors, series: &Series, rows: &mut [EncodedRow]) -> Result<()> {
		let ctx = SeriesRowPreUpdateContext::new(series, rows);
		txn.series_row_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		series: &Series,
		posts: &[EncodedRow],
		pres: &[EncodedRow],
	) -> Result<()> {
		let ctx = SeriesRowPostUpdateContext::new(series, posts, pres);
		txn.series_row_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(txn: &mut impl WithInterceptors, series: &Series) -> Result<()> {
		let ctx = SeriesRowPreDeleteContext::new(series);
		txn.series_row_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		series: &Series,
		deleted_rows: &[EncodedRow],
	) -> Result<()> {
		let ctx = SeriesRowPostDeleteContext::new(series, deleted_rows);
		txn.series_row_post_delete_interceptors().execute(ctx)
	}
}
