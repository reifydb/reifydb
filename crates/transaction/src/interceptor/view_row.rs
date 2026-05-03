// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{encoded::row::EncodedRow, interface::catalog::view::View};
use reifydb_type::{Result, value::row_number::RowNumber};

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

pub struct ViewRowPreInsertContext<'a> {
	pub view: &'a View,
	pub rn: RowNumber,
	pub row: EncodedRow,
}

impl<'a> ViewRowPreInsertContext<'a> {
	pub fn new(view: &'a View, rn: RowNumber, row: EncodedRow) -> Self {
		Self {
			view,
			rn,
			row,
		}
	}
}

pub trait ViewRowPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewRowPreInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn ViewRowPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewRowPreInsertContext) -> Result<EncodedRow> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.row)
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
	pub id: RowNumber,
	pub row: &'a EncodedRow,
}

impl<'a> ViewRowPostInsertContext<'a> {
	pub fn new(view: &'a View, id: RowNumber, row: &'a EncodedRow) -> Self {
		Self {
			view,
			id,
			row,
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
	pub id: RowNumber,
	pub row: EncodedRow,
}

impl<'a> ViewRowPreUpdateContext<'a> {
	pub fn new(view: &'a View, id: RowNumber, row: EncodedRow) -> Self {
		Self {
			view,
			id,
			row,
		}
	}
}

pub trait ViewRowPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewRowPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn ViewRowPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewRowPreUpdateContext) -> Result<EncodedRow> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.row)
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
	pub id: RowNumber,
	pub post: &'a EncodedRow,
	pub pre: &'a EncodedRow,
}

impl<'a> ViewRowPostUpdateContext<'a> {
	pub fn new(view: &'a View, id: RowNumber, post: &'a EncodedRow, pre: &'a EncodedRow) -> Self {
		Self {
			view,
			id,
			post,
			pre,
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
	pub id: RowNumber,
}

impl<'a> ViewRowPreDeleteContext<'a> {
	pub fn new(view: &'a View, id: RowNumber) -> Self {
		Self {
			view,
			id,
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
	pub id: RowNumber,
	pub deleted_row: &'a EncodedRow,
}

impl<'a> ViewRowPostDeleteContext<'a> {
	pub fn new(view: &'a View, id: RowNumber, deleted_row: &'a EncodedRow) -> Self {
		Self {
			view,
			id,
			deleted_row,
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
		rn: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		let ctx = ViewRowPreInsertContext::new(view, rn, row);
		txn.view_row_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(
		txn: &mut impl WithInterceptors,
		view: &View,
		id: RowNumber,
		row: &EncodedRow,
	) -> Result<()> {
		let ctx = ViewRowPostInsertContext::new(view, id, row);
		txn.view_row_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(
		txn: &mut impl WithInterceptors,
		view: &View,
		id: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		let ctx = ViewRowPreUpdateContext::new(view, id, row);
		txn.view_row_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		view: &View,
		id: RowNumber,
		post: &EncodedRow,
		pre: &EncodedRow,
	) -> Result<()> {
		let ctx = ViewRowPostUpdateContext::new(view, id, post, pre);
		txn.view_row_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(txn: &mut impl WithInterceptors, view: &View, id: RowNumber) -> Result<()> {
		let ctx = ViewRowPreDeleteContext::new(view, id);
		txn.view_row_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		view: &View,
		id: RowNumber,
		deleted_row: &EncodedRow,
	) -> Result<()> {
		let ctx = ViewRowPostDeleteContext::new(view, id, deleted_row);
		txn.view_row_post_delete_interceptors().execute(ctx)
	}
}
