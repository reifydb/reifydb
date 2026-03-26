// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{encoded::row::EncodedRow, interface::catalog::series::Series};
use reifydb_type::Result;

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

// PRE INSERT
/// Context for series pre-insert interceptors
pub struct SeriesRowPreInsertContext<'a> {
	pub series: &'a Series,
	pub row: EncodedRow,
}

impl<'a> SeriesRowPreInsertContext<'a> {
	pub fn new(series: &'a Series, row: EncodedRow) -> Self {
		Self {
			series,
			row,
		}
	}
}

pub trait SeriesRowPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesRowPreInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesRowPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesRowPreInsertContext) -> Result<EncodedRow> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.row)
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

// POST INSERT
/// Context for series post-insert interceptors
pub struct SeriesRowPostInsertContext<'a> {
	pub series: &'a Series,
	pub row: &'a EncodedRow,
}

impl<'a> SeriesRowPostInsertContext<'a> {
	pub fn new(series: &'a Series, row: &'a EncodedRow) -> Self {
		Self {
			series,
			row,
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

// PRE UPDATE
/// Context for series pre-update interceptors
pub struct SeriesRowPreUpdateContext<'a> {
	pub series: &'a Series,
	pub row: EncodedRow,
}

impl<'a> SeriesRowPreUpdateContext<'a> {
	pub fn new(series: &'a Series, row: EncodedRow) -> Self {
		Self {
			series,
			row,
		}
	}
}

pub trait SeriesRowPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesRowPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesRowPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesRowPreUpdateContext) -> Result<EncodedRow> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.row)
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

// POST UPDATE
/// Context for series post-update interceptors
pub struct SeriesRowPostUpdateContext<'a> {
	pub series: &'a Series,
	pub row: &'a EncodedRow,
	pub old_row: &'a EncodedRow,
}

impl<'a> SeriesRowPostUpdateContext<'a> {
	pub fn new(series: &'a Series, row: &'a EncodedRow, old_row: &'a EncodedRow) -> Self {
		Self {
			series,
			row,
			old_row,
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

// PRE DELETE
/// Context for series pre-delete interceptors
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

// POST DELETE
/// Context for series post-delete interceptors
pub struct SeriesRowPostDeleteContext<'a> {
	pub series: &'a Series,
	pub deleted_row: &'a EncodedRow,
}

impl<'a> SeriesRowPostDeleteContext<'a> {
	pub fn new(series: &'a Series, deleted_row: &'a EncodedRow) -> Self {
		Self {
			series,
			deleted_row,
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

/// Helper struct for executing series interceptors via static methods.
pub struct SeriesRowInterceptor;

impl SeriesRowInterceptor {
	pub fn pre_insert(txn: &mut impl WithInterceptors, series: &Series, row: EncodedRow) -> Result<EncodedRow> {
		let ctx = SeriesRowPreInsertContext::new(series, row);
		txn.series_row_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(txn: &mut impl WithInterceptors, series: &Series, row: &EncodedRow) -> Result<()> {
		let ctx = SeriesRowPostInsertContext::new(series, row);
		txn.series_row_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(txn: &mut impl WithInterceptors, series: &Series, row: EncodedRow) -> Result<EncodedRow> {
		let ctx = SeriesRowPreUpdateContext::new(series, row);
		txn.series_row_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		series: &Series,
		row: &EncodedRow,
		old_row: &EncodedRow,
	) -> Result<()> {
		let ctx = SeriesRowPostUpdateContext::new(series, row, old_row);
		txn.series_row_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(txn: &mut impl WithInterceptors, series: &Series) -> Result<()> {
		let ctx = SeriesRowPreDeleteContext::new(series);
		txn.series_row_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(txn: &mut impl WithInterceptors, series: &Series, deleted_row: &EncodedRow) -> Result<()> {
		let ctx = SeriesRowPostDeleteContext::new(series, deleted_row);
		txn.series_row_post_delete_interceptors().execute(ctx)
	}
}
