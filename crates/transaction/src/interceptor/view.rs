// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{encoded::encoded::EncodedValues, interface::catalog::view::ViewDef};
use reifydb_type::value::row_number::RowNumber;

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

// PRE INSERT
/// Context for view pre-insert interceptors
pub struct ViewPreInsertContext<'a> {
	pub view: &'a ViewDef,
	pub rn: RowNumber,
	pub row: &'a EncodedValues,
}

impl<'a> ViewPreInsertContext<'a> {
	pub fn new(view: &'a ViewDef, rn: RowNumber, row: &'a EncodedValues) -> Self {
		Self {
			view,
			rn,
			row,
		}
	}
}

pub trait ViewPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewPreInsertContext<'a>) -> reifydb_type::Result<()>;
}

impl InterceptorChain<dyn ViewPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewPreInsertContext) -> reifydb_type::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewPreInsertInterceptor for ClosureViewPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewPreInsertContext<'a>) -> reifydb_type::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_pre_insert<F>(f: F) -> ClosureViewPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewPreInsertInterceptor::new(f)
}

// POST INSERT
/// Context for view post-insert interceptors
pub struct ViewPostInsertContext<'a> {
	pub view: &'a ViewDef,
	pub id: RowNumber,
	pub row: &'a EncodedValues,
}

impl<'a> ViewPostInsertContext<'a> {
	pub fn new(view: &'a ViewDef, id: RowNumber, row: &'a EncodedValues) -> Self {
		Self {
			view,
			id,
			row,
		}
	}
}

pub trait ViewPostInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewPostInsertContext<'a>) -> reifydb_type::Result<()>;
}

impl InterceptorChain<dyn ViewPostInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewPostInsertContext) -> reifydb_type::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewPostInsertInterceptor for ClosureViewPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewPostInsertContext<'a>) -> reifydb_type::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_post_insert<F>(f: F) -> ClosureViewPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewPostInsertInterceptor::new(f)
}

// PRE UPDATE
/// Context for view pre-update interceptors
pub struct ViewPreUpdateContext<'a> {
	pub view: &'a ViewDef,
	pub id: RowNumber,
	pub row: &'a EncodedValues,
}

impl<'a> ViewPreUpdateContext<'a> {
	pub fn new(view: &'a ViewDef, id: RowNumber, row: &'a EncodedValues) -> Self {
		Self {
			view,
			id,
			row,
		}
	}
}

pub trait ViewPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewPreUpdateContext<'a>) -> reifydb_type::Result<()>;
}

impl InterceptorChain<dyn ViewPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewPreUpdateContext) -> reifydb_type::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewPreUpdateInterceptor for ClosureViewPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewPreUpdateContext<'a>) -> reifydb_type::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_pre_update<F>(f: F) -> ClosureViewPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewPreUpdateInterceptor::new(f)
}

// POST UPDATE
/// Context for view post-update interceptors
pub struct ViewPostUpdateContext<'a> {
	pub view: &'a ViewDef,
	pub id: RowNumber,
	pub row: &'a EncodedValues,
	pub old_row: &'a EncodedValues,
}

impl<'a> ViewPostUpdateContext<'a> {
	pub fn new(view: &'a ViewDef, id: RowNumber, row: &'a EncodedValues, old_row: &'a EncodedValues) -> Self {
		Self {
			view,
			id,
			row,
			old_row,
		}
	}
}

pub trait ViewPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewPostUpdateContext<'a>) -> reifydb_type::Result<()>;
}

impl InterceptorChain<dyn ViewPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewPostUpdateContext) -> reifydb_type::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewPostUpdateInterceptor for ClosureViewPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewPostUpdateContext<'a>) -> reifydb_type::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_post_update<F>(f: F) -> ClosureViewPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewPostUpdateInterceptor::new(f)
}

// PRE DELETE
/// Context for view pre-delete interceptors
pub struct ViewPreDeleteContext<'a> {
	pub view: &'a ViewDef,
	pub id: RowNumber,
}

impl<'a> ViewPreDeleteContext<'a> {
	pub fn new(view: &'a ViewDef, id: RowNumber) -> Self {
		Self {
			view,
			id,
		}
	}
}

pub trait ViewPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewPreDeleteContext<'a>) -> reifydb_type::Result<()>;
}

impl InterceptorChain<dyn ViewPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewPreDeleteContext) -> reifydb_type::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewPreDeleteInterceptor for ClosureViewPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewPreDeleteContext<'a>) -> reifydb_type::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_pre_delete<F>(f: F) -> ClosureViewPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPreDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewPreDeleteInterceptor::new(f)
}

// POST DELETE
/// Context for view post-delete interceptors
pub struct ViewPostDeleteContext<'a> {
	pub view: &'a ViewDef,
	pub id: RowNumber,
	pub deleted_row: &'a EncodedValues,
}

impl<'a> ViewPostDeleteContext<'a> {
	pub fn new(view: &'a ViewDef, id: RowNumber, deleted_row: &'a EncodedValues) -> Self {
		Self {
			view,
			id,
			deleted_row,
		}
	}
}

pub trait ViewPostDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut ViewPostDeleteContext<'a>) -> reifydb_type::Result<()>;
}

impl InterceptorChain<dyn ViewPostDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: ViewPostDeleteContext) -> reifydb_type::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureViewPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureViewPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureViewPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> ViewPostDeleteInterceptor for ClosureViewPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut ViewPostDeleteContext<'a>) -> reifydb_type::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn view_post_delete<F>(f: F) -> ClosureViewPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut ViewPostDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureViewPostDeleteInterceptor::new(f)
}

/// Helper struct for executing view interceptors via static methods.
pub struct ViewInterceptor;

impl ViewInterceptor {
	pub fn pre_insert(
		txn: &mut impl WithInterceptors,
		view: &ViewDef,
		rn: RowNumber,
		row: &EncodedValues,
	) -> reifydb_type::Result<()> {
		let ctx = ViewPreInsertContext::new(view, rn, row);
		txn.view_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(
		txn: &mut impl WithInterceptors,
		view: &ViewDef,
		id: RowNumber,
		row: &EncodedValues,
	) -> reifydb_type::Result<()> {
		let ctx = ViewPostInsertContext::new(view, id, row);
		txn.view_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(
		txn: &mut impl WithInterceptors,
		view: &ViewDef,
		id: RowNumber,
		row: &EncodedValues,
	) -> reifydb_type::Result<()> {
		let ctx = ViewPreUpdateContext::new(view, id, row);
		txn.view_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		view: &ViewDef,
		id: RowNumber,
		row: &EncodedValues,
		old_row: &EncodedValues,
	) -> reifydb_type::Result<()> {
		let ctx = ViewPostUpdateContext::new(view, id, row, old_row);
		txn.view_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(txn: &mut impl WithInterceptors, view: &ViewDef, id: RowNumber) -> reifydb_type::Result<()> {
		let ctx = ViewPreDeleteContext::new(view, id);
		txn.view_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		view: &ViewDef,
		id: RowNumber,
		deleted_row: &EncodedValues,
	) -> reifydb_type::Result<()> {
		let ctx = ViewPostDeleteContext::new(view, id, deleted_row);
		txn.view_post_delete_interceptors().execute(ctx)
	}
}
