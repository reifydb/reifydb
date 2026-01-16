// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{encoded::encoded::EncodedValues, interface::catalog::table::TableDef};
use reifydb_type::value::row_number::RowNumber;

use crate::interceptor::chain::InterceptorChain;

// PRE INSERT
/// Context for table pre-insert interceptors
pub struct TablePreInsertContext<'a> {
	pub table: &'a TableDef,
	pub rn: RowNumber,
	pub row: &'a EncodedValues,
}

impl<'a> TablePreInsertContext<'a> {
	pub fn new(table: &'a TableDef, rn: RowNumber, row: &'a EncodedValues) -> Self {
		Self {
			table,
			rn,
			row,
		}
	}
}

pub trait TablePreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TablePreInsertContext<'a>) -> reifydb_type::Result<()>;
}

impl InterceptorChain<dyn TablePreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TablePreInsertContext) -> reifydb_type::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTablePreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TablePreInsertInterceptor for ClosureTablePreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TablePreInsertContext<'a>) -> reifydb_type::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_pre_insert<F>(f: F) -> ClosureTablePreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTablePreInsertInterceptor::new(f)
}

// POST INSERT
/// Context for table post-insert interceptors
pub struct TablePostInsertContext<'a> {
	pub table: &'a TableDef,
	pub id: RowNumber,
	pub row: &'a EncodedValues,
}

impl<'a> TablePostInsertContext<'a> {
	pub fn new(table: &'a TableDef, id: RowNumber, row: &'a EncodedValues) -> Self {
		Self {
			table,
			id,
			row,
		}
	}
}

pub trait TablePostInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TablePostInsertContext<'a>) -> reifydb_type::Result<()>;
}

impl InterceptorChain<dyn TablePostInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TablePostInsertContext) -> reifydb_type::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTablePostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TablePostInsertInterceptor for ClosureTablePostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TablePostInsertContext<'a>) -> reifydb_type::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_post_insert<F>(f: F) -> ClosureTablePostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostInsertContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTablePostInsertInterceptor::new(f)
}

// PRE UPDATE
/// Context for table pre-update interceptors
pub struct TablePreUpdateContext<'a> {
	pub table: &'a TableDef,
	pub id: RowNumber,
	pub row: &'a EncodedValues,
}

impl<'a> TablePreUpdateContext<'a> {
	pub fn new(table: &'a TableDef, id: RowNumber, row: &'a EncodedValues) -> Self {
		Self {
			table,
			id,
			row,
		}
	}
}

pub trait TablePreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TablePreUpdateContext<'a>) -> reifydb_type::Result<()>;
}

impl InterceptorChain<dyn TablePreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TablePreUpdateContext) -> reifydb_type::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TablePreUpdateInterceptor for ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TablePreUpdateContext<'a>) -> reifydb_type::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_pre_update<F>(f: F) -> ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTablePreUpdateInterceptor::new(f)
}

// POST UPDATE
/// Context for table post-update interceptors
pub struct TablePostUpdateContext<'a> {
	pub table: &'a TableDef,
	pub id: RowNumber,
	pub row: &'a EncodedValues,
	pub old_row: &'a EncodedValues,
}

impl<'a> TablePostUpdateContext<'a> {
	pub fn new(table: &'a TableDef, id: RowNumber, row: &'a EncodedValues, old_row: &'a EncodedValues) -> Self {
		Self {
			table,
			id,
			row,
			old_row,
		}
	}
}

pub trait TablePostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TablePostUpdateContext<'a>) -> reifydb_type::Result<()>;
}

impl InterceptorChain<dyn TablePostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TablePostUpdateContext) -> reifydb_type::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TablePostUpdateInterceptor for ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TablePostUpdateContext<'a>) -> reifydb_type::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_post_update<F>(f: F) -> ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTablePostUpdateInterceptor::new(f)
}

// PRE DELETE
/// Context for table pre-delete interceptors
pub struct TablePreDeleteContext<'a> {
	pub table: &'a TableDef,
	pub id: RowNumber,
}

impl<'a> TablePreDeleteContext<'a> {
	pub fn new(table: &'a TableDef, id: RowNumber) -> Self {
		Self {
			table,
			id,
		}
	}
}

pub trait TablePreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TablePreDeleteContext<'a>) -> reifydb_type::Result<()>;
}

impl InterceptorChain<dyn TablePreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TablePreDeleteContext) -> reifydb_type::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TablePreDeleteInterceptor for ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TablePreDeleteContext<'a>) -> reifydb_type::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_pre_delete<F>(f: F) -> ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTablePreDeleteInterceptor::new(f)
}

// POST DELETE
/// Context for table post-delete interceptors
pub struct TablePostDeleteContext<'a> {
	pub table: &'a TableDef,
	pub id: RowNumber,
	pub deleted_row: &'a EncodedValues,
}

impl<'a> TablePostDeleteContext<'a> {
	pub fn new(table: &'a TableDef, id: RowNumber, deleted_row: &'a EncodedValues) -> Self {
		Self {
			table,
			id,
			deleted_row,
		}
	}
}

pub trait TablePostDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TablePostDeleteContext<'a>) -> reifydb_type::Result<()>;
}

impl InterceptorChain<dyn TablePostDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TablePostDeleteContext) -> reifydb_type::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTablePostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TablePostDeleteInterceptor for ClosureTablePostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TablePostDeleteContext<'a>) -> reifydb_type::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_post_delete<F>(f: F) -> ClosureTablePostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostDeleteContext<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTablePostDeleteInterceptor::new(f)
}

/// Helper struct for executing table interceptors via static methods.
pub struct TableInterceptor;

impl TableInterceptor {
	pub fn pre_insert(
		txn: &mut impl super::WithInterceptors,
		table: &TableDef,
		rn: RowNumber,
		row: &EncodedValues,
	) -> reifydb_type::Result<()> {
		let ctx = TablePreInsertContext::new(table, rn, row);
		txn.table_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(
		txn: &mut impl super::WithInterceptors,
		table: &TableDef,
		id: RowNumber,
		row: &EncodedValues,
	) -> reifydb_type::Result<()> {
		let ctx = TablePostInsertContext::new(table, id, row);
		txn.table_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(
		txn: &mut impl super::WithInterceptors,
		table: &TableDef,
		id: RowNumber,
		row: &EncodedValues,
	) -> reifydb_type::Result<()> {
		let ctx = TablePreUpdateContext::new(table, id, row);
		txn.table_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl super::WithInterceptors,
		table: &TableDef,
		id: RowNumber,
		row: &EncodedValues,
		old_row: &EncodedValues,
	) -> reifydb_type::Result<()> {
		let ctx = TablePostUpdateContext::new(table, id, row, old_row);
		txn.table_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(
		txn: &mut impl super::WithInterceptors,
		table: &TableDef,
		id: RowNumber,
	) -> reifydb_type::Result<()> {
		let ctx = TablePreDeleteContext::new(table, id);
		txn.table_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl super::WithInterceptors,
		table: &TableDef,
		id: RowNumber,
		deleted_row: &EncodedValues,
	) -> reifydb_type::Result<()> {
		let ctx = TablePostDeleteContext::new(table, id, deleted_row);
		txn.table_post_delete_interceptors().execute(ctx)
	}
}
