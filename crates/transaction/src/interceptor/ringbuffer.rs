// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{interface::RingBufferDef, value::encoded::EncodedValues};
use reifydb_type::RowNumber;

use crate::interceptor::InterceptorChain;

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

#[async_trait::async_trait]
pub trait RingBufferPreInsertInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut RingBufferPreInsertContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn RingBufferPreInsertInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: RingBufferPreInsertContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> RingBufferPreInsertInterceptor for ClosureRingBufferPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut RingBufferPreInsertContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_pre_insert<F>(f: F) -> ClosureRingBufferPreInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
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

#[async_trait::async_trait]
pub trait RingBufferPostInsertInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut RingBufferPostInsertContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn RingBufferPostInsertInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: RingBufferPostInsertContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> RingBufferPostInsertInterceptor for ClosureRingBufferPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut RingBufferPostInsertContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_post_insert<F>(f: F) -> ClosureRingBufferPostInsertInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostInsertContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
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

#[async_trait::async_trait]
pub trait RingBufferPreUpdateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut RingBufferPreUpdateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn RingBufferPreUpdateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: RingBufferPreUpdateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> RingBufferPreUpdateInterceptor for ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut RingBufferPreUpdateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_pre_update<F>(f: F) -> ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
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

#[async_trait::async_trait]
pub trait RingBufferPostUpdateInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut RingBufferPostUpdateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn RingBufferPostUpdateInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: RingBufferPostUpdateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> RingBufferPostUpdateInterceptor for ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut RingBufferPostUpdateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_post_update<F>(f: F) -> ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
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

#[async_trait::async_trait]
pub trait RingBufferPreDeleteInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut RingBufferPreDeleteContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn RingBufferPreDeleteInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: RingBufferPreDeleteContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> RingBufferPreDeleteInterceptor for ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut RingBufferPreDeleteContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_pre_delete<F>(f: F) -> ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
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

#[async_trait::async_trait]
pub trait RingBufferPostDeleteInterceptor: Send + Sync {
	async fn intercept<'a>(&self, ctx: &mut RingBufferPostDeleteContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn RingBufferPostDeleteInterceptor + Send + Sync> {
	pub async fn execute<'a>(&self, mut ctx: RingBufferPostDeleteContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx).await?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

#[async_trait::async_trait]
impl<F> RingBufferPostDeleteInterceptor for ClosureRingBufferPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	async fn intercept<'a>(&self, ctx: &mut RingBufferPostDeleteContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_post_delete<F>(f: F) -> ClosureRingBufferPostDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferPostDeleteInterceptor::new(f)
}
