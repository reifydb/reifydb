// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::RingBufferDef;

use crate::interceptor::InterceptorChain;

// RING BUFFER POST CREATE
/// Context for ring buffer post-create interceptors
pub struct RingBufferDefPostCreateContext<'a> {
	pub post: &'a RingBufferDef,
}

impl<'a> RingBufferDefPostCreateContext<'a> {
	pub fn new(post: &'a RingBufferDef) -> Self {
		Self {
			post,
		}
	}
}

pub trait RingBufferDefPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferDefPostCreateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn RingBufferDefPostCreateInterceptor + Send + Sync> {
	pub fn execute<'a>(&self, mut ctx: RingBufferDefPostCreateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferDefPostCreateInterceptor for ClosureRingBufferDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPostCreateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferDefPostCreateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_def_post_create<F>(f: F) -> ClosureRingBufferDefPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPostCreateContext<'a>) -> reifydb_core::Result<()>
		+ Send
		+ Sync
		+ Clone
		+ 'static,
{
	ClosureRingBufferDefPostCreateInterceptor::new(f)
}

// RING BUFFER PRE UPDATE
/// Context for ring buffer pre-update interceptors
pub struct RingBufferDefPreUpdateContext<'a> {
	pub pre: &'a RingBufferDef,
}

impl<'a> RingBufferDefPreUpdateContext<'a> {
	pub fn new(pre: &'a RingBufferDef) -> Self {
		Self {
			pre,
		}
	}
}

pub trait RingBufferDefPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferDefPreUpdateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn RingBufferDefPreUpdateInterceptor + Send + Sync> {
	pub fn execute<'a>(&self, mut ctx: RingBufferDefPreUpdateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferDefPreUpdateInterceptor for ClosureRingBufferDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPreUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferDefPreUpdateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_def_pre_update<F>(f: F) -> ClosureRingBufferDefPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPreUpdateContext<'a>) -> reifydb_core::Result<()>
		+ Send
		+ Sync
		+ Clone
		+ 'static,
{
	ClosureRingBufferDefPreUpdateInterceptor::new(f)
}

// RING BUFFER POST UPDATE
/// Context for ring buffer post-update interceptors
pub struct RingBufferDefPostUpdateContext<'a> {
	pub pre: &'a RingBufferDef,
	pub post: &'a RingBufferDef,
}

impl<'a> RingBufferDefPostUpdateContext<'a> {
	pub fn new(pre: &'a RingBufferDef, post: &'a RingBufferDef) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait RingBufferDefPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferDefPostUpdateContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn RingBufferDefPostUpdateInterceptor + Send + Sync> {
	pub fn execute<'a>(&self, mut ctx: RingBufferDefPostUpdateContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferDefPostUpdateInterceptor for ClosureRingBufferDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPostUpdateContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferDefPostUpdateContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_def_post_update<F>(f: F) -> ClosureRingBufferDefPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPostUpdateContext<'a>) -> reifydb_core::Result<()>
		+ Send
		+ Sync
		+ Clone
		+ 'static,
{
	ClosureRingBufferDefPostUpdateInterceptor::new(f)
}

// RING BUFFER PRE DELETE
/// Context for ring buffer pre-delete interceptors
pub struct RingBufferDefPreDeleteContext<'a> {
	pub pre: &'a RingBufferDef,
}

impl<'a> RingBufferDefPreDeleteContext<'a> {
	pub fn new(pre: &'a RingBufferDef) -> Self {
		Self {
			pre,
		}
	}
}

pub trait RingBufferDefPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferDefPreDeleteContext<'a>) -> reifydb_core::Result<()>;
}

impl InterceptorChain<dyn RingBufferDefPreDeleteInterceptor + Send + Sync> {
	pub fn execute<'a>(&self, mut ctx: RingBufferDefPreDeleteContext<'a>) -> reifydb_core::Result<()> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferDefPreDeleteInterceptor for ClosureRingBufferDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPreDeleteContext<'a>) -> reifydb_core::Result<()> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferDefPreDeleteContext<'a>) -> reifydb_core::Result<()> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_def_pre_delete<F>(f: F) -> ClosureRingBufferDefPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferDefPreDeleteContext<'a>) -> reifydb_core::Result<()>
		+ Send
		+ Sync
		+ Clone
		+ 'static,
{
	ClosureRingBufferDefPreDeleteInterceptor::new(f)
}
