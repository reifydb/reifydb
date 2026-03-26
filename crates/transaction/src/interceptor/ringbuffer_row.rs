// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{encoded::row::EncodedRow, interface::catalog::ringbuffer::RingBuffer};
use reifydb_type::{Result, value::row_number::RowNumber};

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

// PRE INSERT
/// Context for ringbuffer pre-insert interceptors
pub struct RingBufferRowPreInsertContext<'a> {
	pub ringbuffer: &'a RingBuffer,
	pub row: EncodedRow,
}

impl<'a> RingBufferRowPreInsertContext<'a> {
	pub fn new(ringbuffer: &'a RingBuffer, row: EncodedRow) -> Self {
		Self {
			ringbuffer,
			row,
		}
	}
}

pub trait RingBufferRowPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPreInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RingBufferRowPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferRowPreInsertContext) -> Result<EncodedRow> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.row)
	}
}

pub struct ClosureRingBufferRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferRowPreInsertInterceptor for ClosureRingBufferRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPreInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_row_pre_insert<F>(f: F) -> ClosureRingBufferRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferRowPreInsertInterceptor::new(f)
}

// POST INSERT
/// Context for ringbuffer post-insert interceptors
pub struct RingBufferRowPostInsertContext<'a> {
	pub ringbuffer: &'a RingBuffer,
	pub id: RowNumber,
	pub row: &'a EncodedRow,
}

impl<'a> RingBufferRowPostInsertContext<'a> {
	pub fn new(ringbuffer: &'a RingBuffer, id: RowNumber, row: &'a EncodedRow) -> Self {
		Self {
			ringbuffer,
			id,
			row,
		}
	}
}

pub trait RingBufferRowPostInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPostInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RingBufferRowPostInsertInterceptor + Send + Sync> {
	pub fn execute<'a>(&self, mut ctx: RingBufferRowPostInsertContext<'a>) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferRowPostInsertInterceptor for ClosureRingBufferRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPostInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_row_post_insert<F>(f: F) -> ClosureRingBufferRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferRowPostInsertInterceptor::new(f)
}

// PRE UPDATE
/// Context for ringbuffer pre-update interceptors
pub struct RingBufferRowPreUpdateContext<'a> {
	pub ringbuffer: &'a RingBuffer,
	pub id: RowNumber,
	pub row: EncodedRow,
}

impl<'a> RingBufferRowPreUpdateContext<'a> {
	pub fn new(ringbuffer: &'a RingBuffer, id: RowNumber, row: EncodedRow) -> Self {
		Self {
			ringbuffer,
			id,
			row,
		}
	}
}

pub trait RingBufferRowPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RingBufferRowPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferRowPreUpdateContext) -> Result<EncodedRow> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(ctx.row)
	}
}

pub struct ClosureRingBufferRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferRowPreUpdateInterceptor for ClosureRingBufferRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_row_pre_update<F>(f: F) -> ClosureRingBufferRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferRowPreUpdateInterceptor::new(f)
}

// POST UPDATE
/// Context for ringbuffer post-update interceptors
pub struct RingBufferRowPostUpdateContext<'a> {
	pub ringbuffer: &'a RingBuffer,
	pub id: RowNumber,
	pub row: &'a EncodedRow,
	pub old_row: &'a EncodedRow,
}

impl<'a> RingBufferRowPostUpdateContext<'a> {
	pub fn new(ringbuffer: &'a RingBuffer, id: RowNumber, row: &'a EncodedRow, old_row: &'a EncodedRow) -> Self {
		Self {
			ringbuffer,
			id,
			row,
			old_row,
		}
	}
}

pub trait RingBufferRowPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RingBufferRowPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferRowPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferRowPostUpdateInterceptor for ClosureRingBufferRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_row_post_update<F>(f: F) -> ClosureRingBufferRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferRowPostUpdateInterceptor::new(f)
}

// PRE DELETE
/// Context for ringbuffer pre-delete interceptors
pub struct RingBufferRowPreDeleteContext<'a> {
	pub ringbuffer: &'a RingBuffer,
	pub id: RowNumber,
}

impl<'a> RingBufferRowPreDeleteContext<'a> {
	pub fn new(ringbuffer: &'a RingBuffer, id: RowNumber) -> Self {
		Self {
			ringbuffer,
			id,
		}
	}
}

pub trait RingBufferRowPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RingBufferRowPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferRowPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferRowPreDeleteInterceptor for ClosureRingBufferRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_row_pre_delete<F>(f: F) -> ClosureRingBufferRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferRowPreDeleteInterceptor::new(f)
}

// POST DELETE
/// Context for ringbuffer post-delete interceptors
pub struct RingBufferRowPostDeleteContext<'a> {
	pub ringbuffer: &'a RingBuffer,
	pub id: RowNumber,
	pub deleted_row: &'a EncodedRow,
}

impl<'a> RingBufferRowPostDeleteContext<'a> {
	pub fn new(ringbuffer: &'a RingBuffer, id: RowNumber, deleted_row: &'a EncodedRow) -> Self {
		Self {
			ringbuffer,
			id,
			deleted_row,
		}
	}
}

pub trait RingBufferRowPostDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPostDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RingBufferRowPostDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferRowPostDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferRowPostDeleteInterceptor for ClosureRingBufferRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPostDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_row_post_delete<F>(f: F) -> ClosureRingBufferRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferRowPostDeleteInterceptor::new(f)
}

/// Helper struct for executing ring buffer interceptors via static methods.
pub struct RingBufferRowInterceptor;

impl RingBufferRowInterceptor {
	pub fn pre_insert(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBuffer,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		let ctx = RingBufferRowPreInsertContext::new(ringbuffer, row);
		txn.ringbuffer_row_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBuffer,
		id: RowNumber,
		row: &EncodedRow,
	) -> Result<()> {
		let ctx = RingBufferRowPostInsertContext::new(ringbuffer, id, row);
		txn.ringbuffer_row_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBuffer,
		id: RowNumber,
		row: EncodedRow,
	) -> Result<EncodedRow> {
		let ctx = RingBufferRowPreUpdateContext::new(ringbuffer, id, row);
		txn.ringbuffer_row_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBuffer,
		id: RowNumber,
		row: &EncodedRow,
		old_row: &EncodedRow,
	) -> Result<()> {
		let ctx = RingBufferRowPostUpdateContext::new(ringbuffer, id, row, old_row);
		txn.ringbuffer_row_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(txn: &mut impl WithInterceptors, ringbuffer: &RingBuffer, id: RowNumber) -> Result<()> {
		let ctx = RingBufferRowPreDeleteContext::new(ringbuffer, id);
		txn.ringbuffer_row_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBuffer,
		id: RowNumber,
		deleted_row: &EncodedRow,
	) -> Result<()> {
		let ctx = RingBufferRowPostDeleteContext::new(ringbuffer, id, deleted_row);
		txn.ringbuffer_row_post_delete_interceptors().execute(ctx)
	}
}
