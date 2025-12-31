// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{interface::TableDef, value::encoded::EncodedValues};
use reifydb_type::RowNumber;

use crate::interceptor::InterceptorChain;

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

#[async_trait::async_trait]
pub trait TablePreInsertInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut TablePreInsertContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn TablePreInsertInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: TablePreInsertContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureTablePreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> TablePreInsertInterceptor for ClosureTablePreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut TablePreInsertContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_pre_insert<F>(f: F) -> ClosureTablePreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
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

#[async_trait::async_trait]
pub trait TablePostInsertInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut TablePostInsertContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn TablePostInsertInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: TablePostInsertContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureTablePostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> TablePostInsertInterceptor for ClosureTablePostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut TablePostInsertContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_post_insert<F>(f: F) -> ClosureTablePostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
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

#[async_trait::async_trait]
pub trait TablePreUpdateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut TablePreUpdateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn TablePreUpdateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: TablePreUpdateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> TablePreUpdateInterceptor for ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut TablePreUpdateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_pre_update<F>(f: F) -> ClosureTablePreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
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

#[async_trait::async_trait]
pub trait TablePostUpdateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut TablePostUpdateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn TablePostUpdateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: TablePostUpdateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> TablePostUpdateInterceptor for ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut TablePostUpdateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_post_update<F>(f: F) -> ClosureTablePostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
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

#[async_trait::async_trait]
pub trait TablePreDeleteInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut TablePreDeleteContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn TablePreDeleteInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: TablePreDeleteContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> TablePreDeleteInterceptor for ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut TablePreDeleteContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_pre_delete<F>(f: F) -> ClosureTablePreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
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

#[async_trait::async_trait]
pub trait TablePostDeleteInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut TablePostDeleteContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn TablePostDeleteInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: TablePostDeleteContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureTablePostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureTablePostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureTablePostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> TablePostDeleteInterceptor for ClosureTablePostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut TablePostDeleteContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn table_post_delete<F>(f: F) -> ClosureTablePostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut TablePostDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureTablePostDeleteInterceptor::new(f)
}
