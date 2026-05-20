// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_core::{encoded::row::EncodedRow, interface::catalog::view::View};
use reifydb_type::{Result, value::row_number::RowNumber};

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

pub struct ViewRowPreInsertContext<'a> {
	pub view: &'a View,
	pub ids: &'a [RowNumber],
	pub rows: &'a mut [EncodedRow],
}

impl<'a> ViewRowPreInsertContext<'a> {
	pub fn new(view: &'a View, ids: &'a [RowNumber], rows: &'a mut [EncodedRow]) -> Self {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		Self {
			view,
			ids,
			rows,
		}
	}
}

pub trait ViewRowPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewRowPreInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn ViewRowPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewRowPreInsertContext) -> Result<()> {
		let original_len = ctx.rows.len();
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
			assert_eq!(ctx.rows.len(), original_len, "pre_insert interceptor changed row count");
		}
		Ok(())
	}
}

pub struct ClosureViewRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewRowPreInsertInterceptor for ClosureViewRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewRowPreInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_row_pre_insert<F>(f: F) -> ClosureViewRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewRowPreInsertInterceptor::new(f)
}

pub struct ViewRowPostInsertContext<'a> {
	pub view: &'a View,
	pub ids: &'a [RowNumber],
	pub rows: &'a [EncodedRow],
}

impl<'a> ViewRowPostInsertContext<'a> {
	pub fn new(view: &'a View, ids: &'a [RowNumber], rows: &'a [EncodedRow]) -> Self {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		Self {
			view,
			ids,
			rows,
		}
	}
}

pub trait ViewRowPostInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewRowPostInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn ViewRowPostInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewRowPostInsertContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewRowPostInsertInterceptor for ClosureViewRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewRowPostInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_row_post_insert<F>(f: F) -> ClosureViewRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewRowPostInsertInterceptor::new(f)
}

pub struct ViewRowPreUpdateContext<'a> {
	pub view: &'a View,
	pub ids: &'a [RowNumber],
	pub rows: &'a mut [EncodedRow],
}

impl<'a> ViewRowPreUpdateContext<'a> {
	pub fn new(view: &'a View, ids: &'a [RowNumber], rows: &'a mut [EncodedRow]) -> Self {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		Self {
			view,
			ids,
			rows,
		}
	}
}

pub trait ViewRowPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewRowPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn ViewRowPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewRowPreUpdateContext) -> Result<()> {
		let original_len = ctx.rows.len();
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
			assert_eq!(ctx.rows.len(), original_len, "pre_update interceptor changed row count");
		}
		Ok(())
	}
}

pub struct ClosureViewRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewRowPreUpdateInterceptor for ClosureViewRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewRowPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_row_pre_update<F>(f: F) -> ClosureViewRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewRowPreUpdateInterceptor::new(f)
}

pub struct ViewRowPostUpdateContext<'a> {
	pub view: &'a View,
	pub ids: &'a [RowNumber],
	pub posts: &'a [EncodedRow],
	pub pres: &'a [EncodedRow],
}

impl<'a> ViewRowPostUpdateContext<'a> {
	pub fn new(view: &'a View, ids: &'a [RowNumber], posts: &'a [EncodedRow], pres: &'a [EncodedRow]) -> Self {
		assert_eq!(ids.len(), posts.len(), "ids/posts length mismatch");
		assert_eq!(ids.len(), pres.len(), "ids/pres length mismatch");
		Self {
			view,
			ids,
			posts,
			pres,
		}
	}
}

pub trait ViewRowPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewRowPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn ViewRowPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewRowPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewRowPostUpdateInterceptor for ClosureViewRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewRowPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_row_post_update<F>(f: F) -> ClosureViewRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewRowPostUpdateInterceptor::new(f)
}

pub struct ViewRowPreDeleteContext<'a> {
	pub view: &'a View,
	pub ids: &'a [RowNumber],
}

impl<'a> ViewRowPreDeleteContext<'a> {
	pub fn new(view: &'a View, ids: &'a [RowNumber]) -> Self {
		Self {
			view,
			ids,
		}
	}
}

pub trait ViewRowPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewRowPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn ViewRowPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewRowPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewRowPreDeleteInterceptor for ClosureViewRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewRowPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_row_pre_delete<F>(f: F) -> ClosureViewRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewRowPreDeleteInterceptor::new(f)
}

pub struct ViewRowPostDeleteContext<'a> {
	pub view: &'a View,
	pub ids: &'a [RowNumber],
	pub deleted_rows: &'a [EncodedRow],
}

impl<'a> ViewRowPostDeleteContext<'a> {
	pub fn new(view: &'a View, ids: &'a [RowNumber], deleted_rows: &'a [EncodedRow]) -> Self {
		assert_eq!(ids.len(), deleted_rows.len(), "ids/deleted_rows length mismatch");
		Self {
			view,
			ids,
			deleted_rows,
		}
	}
}

pub trait ViewRowPostDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewRowPostDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn ViewRowPostDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewRowPostDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewRowPostDeleteInterceptor for ClosureViewRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewRowPostDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_row_post_delete<F>(f: F) -> ClosureViewRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewRowPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewRowPostDeleteInterceptor::new(f)
}

pub struct ViewRowInterceptor;

impl ViewRowInterceptor {
	pub fn pre_insert(
		txn: &mut impl WithInterceptors,
		view: &View,
		ids: &[RowNumber],
		rows: &mut [EncodedRow],
	) -> Result<()> {
		let ctx = ViewRowPreInsertContext::new(view, ids, rows);
		txn.view_row_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(
		txn: &mut impl WithInterceptors,
		view: &View,
		ids: &[RowNumber],
		rows: &[EncodedRow],
	) -> Result<()> {
		let ctx = ViewRowPostInsertContext::new(view, ids, rows);
		txn.view_row_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(
		txn: &mut impl WithInterceptors,
		view: &View,
		ids: &[RowNumber],
		rows: &mut [EncodedRow],
	) -> Result<()> {
		let ctx = ViewRowPreUpdateContext::new(view, ids, rows);
		txn.view_row_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		view: &View,
		ids: &[RowNumber],
		posts: &[EncodedRow],
		pres: &[EncodedRow],
	) -> Result<()> {
		let ctx = ViewRowPostUpdateContext::new(view, ids, posts, pres);
		txn.view_row_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(txn: &mut impl WithInterceptors, view: &View, ids: &[RowNumber]) -> Result<()> {
		let ctx = ViewRowPreDeleteContext::new(view, ids);
		txn.view_row_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		view: &View,
		ids: &[RowNumber],
		deleted_rows: &[EncodedRow],
	) -> Result<()> {
		let ctx = ViewRowPostDeleteContext::new(view, ids, deleted_rows);
		txn.view_row_post_delete_interceptors().execute(ctx)
	}
}
