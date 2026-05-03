// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{encoded::row::EncodedRow, interface::catalog::table::Table};
use reifydb_type::{Result, value::row_number::RowNumber};

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

pub struct TableRowPreInsertContext<'a> {
	pub table: &'a Table,
	pub rn: RowNumber,
	pub row: EncodedRow,
}

impl<'a> TableRowPreInsertContext<'a> {
	pub fn new(table: &'a Table, rn: RowNumber, row: EncodedRow) -> Self {
		Self {
			table,
			rn,
			row,
		}
	}
}

pub trait TableRowPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TableRowPreInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn TableRowPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TableRowPreInsertContext) -> Result<EncodedRow> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.row)
	}
}

pub struct ClosureTableRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTableRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTableRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TableRowPreInsertInterceptor for ClosureTableRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TableRowPreInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_row_pre_insert<F>(f: F) -> ClosureTableRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTableRowPreInsertInterceptor::new(f)
}

pub struct TableRowPostInsertContext<'a> {
	pub table: &'a Table,
	pub id: RowNumber,
	pub row: &'a EncodedRow,
}

impl<'a> TableRowPostInsertContext<'a> {
	pub fn new(table: &'a Table, id: RowNumber, row: &'a EncodedRow) -> Self {
		Self {
			table,
			id,
			row,
		}
	}
}

pub trait TableRowPostInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TableRowPostInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn TableRowPostInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TableRowPostInsertContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTableRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTableRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTableRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TableRowPostInsertInterceptor for ClosureTableRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TableRowPostInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_row_post_insert<F>(f: F) -> ClosureTableRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTableRowPostInsertInterceptor::new(f)
}

pub struct TableRowPreUpdateContext<'a> {
	pub table: &'a Table,
	pub id: RowNumber,
	pub row: EncodedRow,
}

impl<'a> TableRowPreUpdateContext<'a> {
	pub fn new(table: &'a Table, id: RowNumber, row: EncodedRow) -> Self {
		Self {
			table,
			id,
			row,
		}
	}
}

pub trait TableRowPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TableRowPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn TableRowPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TableRowPreUpdateContext) -> Result<EncodedRow> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.row)
	}
}

pub struct ClosureTableRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTableRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTableRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TableRowPreUpdateInterceptor for ClosureTableRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TableRowPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_row_pre_update<F>(f: F) -> ClosureTableRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTableRowPreUpdateInterceptor::new(f)
}

pub struct TableRowPostUpdateContext<'a> {
	pub table: &'a Table,
	pub id: RowNumber,
	pub post: &'a EncodedRow,
	pub pre: &'a EncodedRow,
}

impl<'a> TableRowPostUpdateContext<'a> {
	pub fn new(table: &'a Table, id: RowNumber, post: &'a EncodedRow, pre: &'a EncodedRow) -> Self {
		Self {
			table,
			id,
			post,
			pre,
		}
	}
}

pub trait TableRowPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TableRowPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn TableRowPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TableRowPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTableRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTableRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTableRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TableRowPostUpdateInterceptor for ClosureTableRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TableRowPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_row_post_update<F>(f: F) -> ClosureTableRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTableRowPostUpdateInterceptor::new(f)
}

pub struct TableRowPreDeleteContext<'a> {
	pub table: &'a Table,
	pub id: RowNumber,
}

impl<'a> TableRowPreDeleteContext<'a> {
	pub fn new(table: &'a Table, id: RowNumber) -> Self {
		Self {
			table,
			id,
		}
	}
}

pub trait TableRowPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TableRowPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn TableRowPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TableRowPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTableRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTableRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTableRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TableRowPreDeleteInterceptor for ClosureTableRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TableRowPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_row_pre_delete<F>(f: F) -> ClosureTableRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTableRowPreDeleteInterceptor::new(f)
}

pub struct TableRowPostDeleteContext<'a> {
	pub table: &'a Table,
	pub id: RowNumber,
	pub deleted_row: &'a EncodedRow,
}

impl<'a> TableRowPostDeleteContext<'a> {
	pub fn new(table: &'a Table, id: RowNumber, deleted_row: &'a EncodedRow) -> Self {
		Self {
			table,
			id,
			deleted_row,
		}
	}
}

pub trait TableRowPostDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut TableRowPostDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn TableRowPostDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: TableRowPostDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureTableRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTableRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTableRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> TableRowPostDeleteInterceptor for ClosureTableRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut TableRowPostDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_row_post_delete<F>(f: F) -> ClosureTableRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TableRowPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTableRowPostDeleteInterceptor::new(f)
}

pub struct TableRowInterceptor;

impl TableRowInterceptor {
	pub fn pre_insert(
		txn: &mut impl WithInterceptors,
		table: &Table,
		rn: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		let ctx = TableRowPreInsertContext::new(table, rn, row);
		txn.table_row_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(
		txn: &mut impl WithInterceptors,
		table: &Table,
		id: RowNumber,
		row: &EncodedRow,
	) -> Result<()> {
		let ctx = TableRowPostInsertContext::new(table, id, row);
		txn.table_row_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(
		txn: &mut impl WithInterceptors,
		table: &Table,
		id: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		let ctx = TableRowPreUpdateContext::new(table, id, row);
		txn.table_row_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		table: &Table,
		id: RowNumber,
		post: &EncodedRow,
		pre: &EncodedRow,
	) -> Result<()> {
		let ctx = TableRowPostUpdateContext::new(table, id, post, pre);
		txn.table_row_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(txn: &mut impl WithInterceptors, table: &Table, id: RowNumber) -> Result<()> {
		let ctx = TableRowPreDeleteContext::new(table, id);
		txn.table_row_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		table: &Table,
		id: RowNumber,
		deleted_row: &EncodedRow,
	) -> Result<()> {
		let ctx = TableRowPostDeleteContext::new(table, id, deleted_row);
		txn.table_row_post_delete_interceptors().execute(ctx)
	}
}
