// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{encoded::encoded::EncodedValues, interface::catalog::ringbuffer::RingBufferDef};
use reifydb_type::{Result, value::row_number::RowNumber};

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

// PRE INSERT
/// Context for ringbuffer pre-insert interceptors
pub struct RingBufferPreInsertContext<'a> {
	pub ringbuffer: &'a RingBufferDef,
	pub row: &'a EncodedValues,
}

impl<'a> RingBufferPreInsertContext<'a> {
	pub fn new(ringbuffer: &'a RingBufferDef, row: &'a EncodedValues) -> Self {
		Self {
			ringbuffer,
			row,
		}
	}
}

pub trait RingBufferPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferPreInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RingBufferPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferPreInsertContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferPreInsertInterceptor for ClosureRingBufferPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferPreInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_pre_insert<F>(f: F) -> ClosureRingBufferPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferPreInsertInterceptor::new(f)
}

// POST INSERT
/// Context for ringbuffer post-insert interceptors
pub struct RingBufferPostInsertContext<'a> {
	pub ringbuffer: &'a RingBufferDef,
	pub id: RowNumber,
	pub row: &'a EncodedValues,
}

impl<'a> RingBufferPostInsertContext<'a> {
	pub fn new(ringbuffer: &'a RingBufferDef, id: RowNumber, row: &'a EncodedValues) -> Self {
		Self {
			ringbuffer,
			id,
			row,
		}
	}
}

pub trait RingBufferPostInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferPostInsertContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RingBufferPostInsertInterceptor + Send + Sync> {
	pub fn execute<'a>(&self, mut ctx: RingBufferPostInsertContext<'a>) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferPostInsertInterceptor for ClosureRingBufferPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostInsertContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferPostInsertContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_post_insert<F>(f: F) -> ClosureRingBufferPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostInsertContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferPostInsertInterceptor::new(f)
}

// PRE UPDATE
/// Context for ringbuffer pre-update interceptors
pub struct RingBufferPreUpdateContext<'a> {
	pub ringbuffer: &'a RingBufferDef,
	pub id: RowNumber,
	pub row: &'a EncodedValues,
}

impl<'a> RingBufferPreUpdateContext<'a> {
	pub fn new(ringbuffer: &'a RingBufferDef, id: RowNumber, row: &'a EncodedValues) -> Self {
		Self {
			ringbuffer,
			id,
			row,
		}
	}
}

pub trait RingBufferPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferPreUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RingBufferPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferPreUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferPreUpdateInterceptor for ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferPreUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_pre_update<F>(f: F) -> ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferPreUpdateInterceptor::new(f)
}

// POST UPDATE
/// Context for ringbuffer post-update interceptors
pub struct RingBufferPostUpdateContext<'a> {
	pub ringbuffer: &'a RingBufferDef,
	pub id: RowNumber,
	pub row: &'a EncodedValues,
	pub old_row: &'a EncodedValues,
}

impl<'a> RingBufferPostUpdateContext<'a> {
	pub fn new(
		ringbuffer: &'a RingBufferDef,
		id: RowNumber,
		row: &'a EncodedValues,
		old_row: &'a EncodedValues,
	) -> Self {
		Self {
			ringbuffer,
			id,
			row,
			old_row,
		}
	}
}

pub trait RingBufferPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferPostUpdateContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RingBufferPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferPostUpdateContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferPostUpdateInterceptor for ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferPostUpdateContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_post_update<F>(f: F) -> ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferPostUpdateInterceptor::new(f)
}

// PRE DELETE
/// Context for ringbuffer pre-delete interceptors
pub struct RingBufferPreDeleteContext<'a> {
	pub ringbuffer: &'a RingBufferDef,
	pub id: RowNumber,
}

impl<'a> RingBufferPreDeleteContext<'a> {
	pub fn new(ringbuffer: &'a RingBufferDef, id: RowNumber) -> Self {
		Self {
			ringbuffer,
			id,
		}
	}
}

pub trait RingBufferPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferPreDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RingBufferPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferPreDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferPreDeleteInterceptor for ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferPreDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_pre_delete<F>(f: F) -> ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferPreDeleteInterceptor::new(f)
}

// POST DELETE
/// Context for ringbuffer post-delete interceptors
pub struct RingBufferPostDeleteContext<'a> {
	pub ringbuffer: &'a RingBufferDef,
	pub id: RowNumber,
	pub deleted_row: &'a EncodedValues,
}

impl<'a> RingBufferPostDeleteContext<'a> {
	pub fn new(ringbuffer: &'a RingBufferDef, id: RowNumber, deleted_row: &'a EncodedValues) -> Self {
		Self {
			ringbuffer,
			id,
			deleted_row,
		}
	}
}

pub trait RingBufferPostDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferPostDeleteContext<'a>) -> Result<()>;
}

impl InterceptorChain<dyn RingBufferPostDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferPostDeleteContext) -> Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferPostDeleteInterceptor for ClosureRingBufferPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostDeleteContext<'a>) -> Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferPostDeleteContext<'a>) -> Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_post_delete<F>(f: F) -> ClosureRingBufferPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostDeleteContext<'a>) -> Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferPostDeleteInterceptor::new(f)
}

/// Helper struct for executing ring buffer interceptors via static methods.
pub struct RingBufferInterceptor;

impl RingBufferInterceptor {
	pub fn pre_insert(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBufferDef,
		row: &EncodedValues,
	) -> Result<()> {
		let ctx = RingBufferPreInsertContext::new(ringbuffer, row);
		txn.ringbuffer_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBufferDef,
		id: RowNumber,
		row: &EncodedValues,
	) -> Result<()> {
		let ctx = RingBufferPostInsertContext::new(ringbuffer, id, row);
		txn.ringbuffer_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBufferDef,
		id: RowNumber,
		row: &EncodedValues,
	) -> Result<()> {
		let ctx = RingBufferPreUpdateContext::new(ringbuffer, id, row);
		txn.ringbuffer_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBufferDef,
		id: RowNumber,
		row: &EncodedValues,
		old_row: &EncodedValues,
	) -> Result<()> {
		let ctx = RingBufferPostUpdateContext::new(ringbuffer, id, row, old_row);
		txn.ringbuffer_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(txn: &mut impl WithInterceptors, ringbuffer: &RingBufferDef, id: RowNumber) -> Result<()> {
		let ctx = RingBufferPreDeleteContext::new(ringbuffer, id);
		txn.ringbuffer_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBufferDef,
		id: RowNumber,
		deleted_row: &EncodedValues,
	) -> Result<()> {
		let ctx = RingBufferPostDeleteContext::new(ringbuffer, id, deleted_row);
		txn.ringbuffer_post_delete_interceptors().execute(ctx)
	}
}
