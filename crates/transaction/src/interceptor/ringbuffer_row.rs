// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{bytes::EncodedBytes, ringbuffer::EncodedRingBufferRowBuilder};
use reifydb_core::interface::catalog::ringbuffer::RingBuffer;
use reifydb_value::{error::ValueError, value::row_number::RowNumber};

use super::WithInterceptors;
use crate::interceptor::chain::InterceptorChain;

pub struct RingBufferRowPreInsertContext<'a> {
	pub ringbuffer: &'a RingBuffer,
	pub rows: &'a mut [EncodedRingBufferRowBuilder],
}

impl<'a> RingBufferRowPreInsertContext<'a> {
	pub fn new(ringbuffer: &'a RingBuffer, rows: &'a mut [EncodedRingBufferRowBuilder]) -> Self {
		Self {
			ringbuffer,
			rows,
		}
	}
}

pub trait RingBufferRowPreInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPreInsertContext<'a>) -> Result<(), ValueError>;
}

impl InterceptorChain<dyn RingBufferRowPreInsertInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferRowPreInsertContext) -> Result<(), ValueError> {
		let original_len = ctx.rows.len();
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
			assert_eq!(ctx.rows.len(), original_len, "pre_insert interceptor changed row count");
		}
		Ok(())
	}
}

pub struct ClosureRingBufferRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreInsertContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreInsertContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreInsertContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferRowPreInsertInterceptor for ClosureRingBufferRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreInsertContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPreInsertContext<'a>) -> Result<(), ValueError> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_row_pre_insert<F>(f: F) -> ClosureRingBufferRowPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreInsertContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferRowPreInsertInterceptor::new(f)
}

pub struct RingBufferRowPostInsertContext<'a> {
	pub ringbuffer: &'a RingBuffer,
	pub ids: &'a [RowNumber],
	pub rows: &'a [EncodedBytes],
}

impl<'a> RingBufferRowPostInsertContext<'a> {
	pub fn new(ringbuffer: &'a RingBuffer, ids: &'a [RowNumber], rows: &'a [EncodedBytes]) -> Self {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		Self {
			ringbuffer,
			ids,
			rows,
		}
	}
}

pub trait RingBufferRowPostInsertInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPostInsertContext<'a>) -> Result<(), ValueError>;
}

impl InterceptorChain<dyn RingBufferRowPostInsertInterceptor + Send + Sync> {
	pub fn execute<'a>(&self, mut ctx: RingBufferRowPostInsertContext<'a>) -> Result<(), ValueError> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostInsertContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostInsertContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostInsertContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferRowPostInsertInterceptor for ClosureRingBufferRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostInsertContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPostInsertContext<'a>) -> Result<(), ValueError> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_row_post_insert<F>(f: F) -> ClosureRingBufferRowPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostInsertContext<'a>) -> Result<(), ValueError>
		+ Send
		+ Sync
		+ Clone
		+ 'static,
{
	ClosureRingBufferRowPostInsertInterceptor::new(f)
}

pub struct RingBufferRowPreUpdateContext<'a> {
	pub ringbuffer: &'a RingBuffer,
	pub ids: &'a [RowNumber],
	pub rows: &'a mut [EncodedRingBufferRowBuilder],
}

impl<'a> RingBufferRowPreUpdateContext<'a> {
	pub fn new(
		ringbuffer: &'a RingBuffer,
		ids: &'a [RowNumber],
		rows: &'a mut [EncodedRingBufferRowBuilder],
	) -> Self {
		assert_eq!(ids.len(), rows.len(), "ids/rows length mismatch");
		Self {
			ringbuffer,
			ids,
			rows,
		}
	}
}

pub trait RingBufferRowPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPreUpdateContext<'a>) -> Result<(), ValueError>;
}

impl InterceptorChain<dyn RingBufferRowPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferRowPreUpdateContext) -> Result<(), ValueError> {
		let original_len = ctx.rows.len();
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
			assert_eq!(ctx.rows.len(), original_len, "pre_update interceptor changed row count");
		}
		Ok(())
	}
}

pub struct ClosureRingBufferRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferRowPreUpdateInterceptor for ClosureRingBufferRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPreUpdateContext<'a>) -> Result<(), ValueError> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_row_pre_update<F>(f: F) -> ClosureRingBufferRowPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferRowPreUpdateInterceptor::new(f)
}

pub struct RingBufferRowPostUpdateContext<'a> {
	pub ringbuffer: &'a RingBuffer,
	pub ids: &'a [RowNumber],
	pub posts: &'a [EncodedBytes],
	pub pres: &'a [EncodedBytes],
}

impl<'a> RingBufferRowPostUpdateContext<'a> {
	pub fn new(
		ringbuffer: &'a RingBuffer,
		ids: &'a [RowNumber],
		posts: &'a [EncodedBytes],
		pres: &'a [EncodedBytes],
	) -> Self {
		assert_eq!(ids.len(), posts.len(), "ids/posts length mismatch");
		assert_eq!(ids.len(), pres.len(), "ids/pres length mismatch");
		Self {
			ringbuffer,
			ids,
			posts,
			pres,
		}
	}
}

pub trait RingBufferRowPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPostUpdateContext<'a>) -> Result<(), ValueError>;
}

impl InterceptorChain<dyn RingBufferRowPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferRowPostUpdateContext) -> Result<(), ValueError> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferRowPostUpdateInterceptor for ClosureRingBufferRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPostUpdateContext<'a>) -> Result<(), ValueError> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_row_post_update<F>(f: F) -> ClosureRingBufferRowPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostUpdateContext<'a>) -> Result<(), ValueError>
		+ Send
		+ Sync
		+ Clone
		+ 'static,
{
	ClosureRingBufferRowPostUpdateInterceptor::new(f)
}

pub struct RingBufferRowPreDeleteContext<'a> {
	pub ringbuffer: &'a RingBuffer,
	pub ids: &'a [RowNumber],
}

impl<'a> RingBufferRowPreDeleteContext<'a> {
	pub fn new(ringbuffer: &'a RingBuffer, ids: &'a [RowNumber]) -> Self {
		Self {
			ringbuffer,
			ids,
		}
	}
}

pub trait RingBufferRowPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPreDeleteContext<'a>) -> Result<(), ValueError>;
}

impl InterceptorChain<dyn RingBufferRowPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferRowPreDeleteContext) -> Result<(), ValueError> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferRowPreDeleteInterceptor for ClosureRingBufferRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPreDeleteContext<'a>) -> Result<(), ValueError> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_row_pre_delete<F>(f: F) -> ClosureRingBufferRowPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPreDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferRowPreDeleteInterceptor::new(f)
}

pub struct RingBufferRowPostDeleteContext<'a> {
	pub ringbuffer: &'a RingBuffer,
	pub ids: &'a [RowNumber],
	pub deleted_rows: &'a [EncodedBytes],
}

impl<'a> RingBufferRowPostDeleteContext<'a> {
	pub fn new(ringbuffer: &'a RingBuffer, ids: &'a [RowNumber], deleted_rows: &'a [EncodedBytes]) -> Self {
		assert_eq!(ids.len(), deleted_rows.len(), "ids/deleted_rows length mismatch");
		Self {
			ringbuffer,
			ids,
			deleted_rows,
		}
	}
}

pub trait RingBufferRowPostDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPostDeleteContext<'a>) -> Result<(), ValueError>;
}

impl InterceptorChain<dyn RingBufferRowPostDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferRowPostDeleteContext) -> Result<(), ValueError> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferRowPostDeleteInterceptor for ClosureRingBufferRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferRowPostDeleteContext<'a>) -> Result<(), ValueError> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_row_post_delete<F>(f: F) -> ClosureRingBufferRowPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferRowPostDeleteContext<'a>) -> Result<(), ValueError>
		+ Send
		+ Sync
		+ Clone
		+ 'static,
{
	ClosureRingBufferRowPostDeleteInterceptor::new(f)
}

pub struct RingBufferRowInterceptor;

impl RingBufferRowInterceptor {
	pub fn pre_insert(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBuffer,
		rows: &mut [EncodedRingBufferRowBuilder],
	) -> Result<(), ValueError> {
		let ctx = RingBufferRowPreInsertContext::new(ringbuffer, rows);
		txn.ringbuffer_row_pre_insert_interceptors().execute(ctx)
	}

	pub fn post_insert(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBuffer,
		ids: &[RowNumber],
		bytes_slice: &[EncodedBytes],
	) -> Result<(), ValueError> {
		let ctx = RingBufferRowPostInsertContext::new(ringbuffer, ids, bytes_slice);
		txn.ringbuffer_row_post_insert_interceptors().execute(ctx)
	}

	pub fn pre_update(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBuffer,
		ids: &[RowNumber],
		rows: &mut [EncodedRingBufferRowBuilder],
	) -> Result<(), ValueError> {
		let ctx = RingBufferRowPreUpdateContext::new(ringbuffer, ids, rows);
		txn.ringbuffer_row_pre_update_interceptors().execute(ctx)
	}

	pub fn post_update(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBuffer,
		ids: &[RowNumber],
		posts: &[EncodedBytes],
		pres: &[EncodedBytes],
	) -> Result<(), ValueError> {
		let ctx = RingBufferRowPostUpdateContext::new(ringbuffer, ids, posts, pres);
		txn.ringbuffer_row_post_update_interceptors().execute(ctx)
	}

	pub fn pre_delete(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBuffer,
		ids: &[RowNumber],
	) -> Result<(), ValueError> {
		let ctx = RingBufferRowPreDeleteContext::new(ringbuffer, ids);
		txn.ringbuffer_row_pre_delete_interceptors().execute(ctx)
	}

	pub fn post_delete(
		txn: &mut impl WithInterceptors,
		ringbuffer: &RingBuffer,
		ids: &[RowNumber],
		deleted: &[EncodedBytes],
	) -> Result<(), ValueError> {
		let ctx = RingBufferRowPostDeleteContext::new(ringbuffer, ids, deleted);
		txn.ringbuffer_row_post_delete_interceptors().execute(ctx)
	}
}
