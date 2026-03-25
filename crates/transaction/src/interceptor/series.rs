// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{encoded::row::EncodedRow, interface::catalog::series::SeriesDef};
use reifydb_type::Result;

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

// PRE INSERT
/// Context for series pre-insert interceptors
pub struct SeriesPreInsertContext<'a> {
	pub series: &'a SeriesDef,
	pub row: EncodedRow,
}

impl<'a> SeriesPreInsertContext<'a> {
	pub fn new(series: &'a SeriesDef, row: EncodedRow) -> Self {
		Self {
			series,
			row,
		}
	}
}

pub trait SeriesPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesPreInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesPreInsertContext) -> Result<EncodedRow> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.row)
	}
}

pub struct ClosureSeriesPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesPreInsertInterceptor for ClosureSeriesPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesPreInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_pre_insert<F>(f: F) -> ClosureSeriesPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesPreInsertInterceptor::new(f)
}

// POST INSERT
/// Context for series post-insert interceptors
pub struct SeriesPostInsertContext<'a> {
	pub series: &'a SeriesDef,
	pub row: &'a EncodedRow,
}

impl<'a> SeriesPostInsertContext<'a> {
	pub fn new(series: &'a SeriesDef, row: &'a EncodedRow) -> Self {
		Self {
			series,
			row,
		}
	}
}

pub trait SeriesPostInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesPostInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesPostInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesPostInsertContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesPostInsertInterceptor for ClosureSeriesPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesPostInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_post_insert<F>(f: F) -> ClosureSeriesPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesPostInsertInterceptor::new(f)
}

// PRE UPDATE
/// Context for series pre-update interceptors
pub struct SeriesPreUpdateContext<'a> {
	pub series: &'a SeriesDef,
	pub row: EncodedRow,
}

impl<'a> SeriesPreUpdateContext<'a> {
	pub fn new(series: &'a SeriesDef, row: EncodedRow) -> Self {
		Self {
			series,
			row,
		}
	}
}

pub trait SeriesPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesPreUpdateContext) -> Result<EncodedRow> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.row)
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

// POST UPDATE
/// Context for series post-update interceptors
pub struct SeriesPostUpdateContext<'a> {
	pub series: &'a SeriesDef,
	pub row: &'a EncodedRow,
	pub old_row: &'a EncodedRow,
}

impl<'a> SeriesPostUpdateContext<'a> {
	pub fn new(series: &'a SeriesDef, row: &'a EncodedRow, old_row: &'a EncodedRow) -> Self {
		Self {
			series,
			row,
			old_row,
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

// PRE DELETE
/// Context for series pre-delete interceptors
pub struct SeriesPreDeleteContext<'a> {
	pub series: &'a SeriesDef,
}

impl<'a> SeriesPreDeleteContext<'a> {
	pub fn new(series: &'a SeriesDef) -> Self {
		Self {
			series,
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

// POST DELETE
/// Context for series post-delete interceptors
pub struct SeriesPostDeleteContext<'a> {
	pub series: &'a SeriesDef,
	pub deleted_row: &'a EncodedRow,
}

impl<'a> SeriesPostDeleteContext<'a> {
	pub fn new(series: &'a SeriesDef, deleted_row: &'a EncodedRow) -> Self {
		Self {
			series,
			deleted_row,
		}
	}
}

pub trait SeriesPostDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut SeriesPostDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn SeriesPostDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: SeriesPostDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureSeriesPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureSeriesPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureSeriesPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> SeriesPostDeleteInterceptor for ClosureSeriesPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut SeriesPostDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn series_post_delete<F>(f: F) -> ClosureSeriesPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut SeriesPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureSeriesPostDeleteInterceptor::new(f)
}

/// Helper struct for executing series interceptors via static methods.
pub struct SeriesInterceptor;

impl SeriesInterceptor {
	pub fn pre_insert(txn: &mut impl WithInterceptors, series: &SeriesDef, row: EncodedRow) -> Result<EncodedRow> {
		let ctx = SeriesPreInsertContext::new(series, row);
		txn.series_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(txn: &mut impl WithInterceptors, series: &SeriesDef, row: &EncodedRow) -> Result<()> {
		let ctx = SeriesPostInsertContext::new(series, row);
		txn.series_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(txn: &mut impl WithInterceptors, series: &SeriesDef, row: EncodedRow) -> Result<EncodedRow> {
		let ctx = SeriesPreUpdateContext::new(series, row);
		txn.series_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		series: &SeriesDef,
		row: &EncodedRow,
		old_row: &EncodedRow,
	) -> Result<()> {
		let ctx = SeriesPostUpdateContext::new(series, row, old_row);
		txn.series_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(txn: &mut impl WithInterceptors, series: &SeriesDef) -> Result<()> {
		let ctx = SeriesPreDeleteContext::new(series);
		txn.series_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		series: &SeriesDef,
		deleted_row: &EncodedRow,
	) -> Result<()> {
		let ctx = SeriesPostDeleteContext::new(series, deleted_row);
		txn.series_post_delete_interceptors().execute(ctx)
	}
}
