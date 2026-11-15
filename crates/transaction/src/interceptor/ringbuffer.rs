// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::ringbuffer::RingBuffer;
use reifydb_value::error::ValueError;

use crate::interceptor::chain::InterceptorChain;

pub struct RingBufferPostCreateContext<'a> {
	pub post: &'a RingBuffer,
}

impl<'a> RingBufferPostCreateContext<'a> {
	pub fn new(post: &'a RingBuffer) -> Self {
		Self {
			post,
		}
	}
}

pub trait RingBufferPostCreateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferPostCreateContext<'a>) -> Result<(), ValueError>;
}

impl InterceptorChain<dyn RingBufferPostCreateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferPostCreateContext) -> Result<(), ValueError> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostCreateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostCreateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostCreateContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferPostCreateInterceptor for ClosureRingBufferPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostCreateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferPostCreateContext<'a>) -> Result<(), ValueError> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_post_create<F>(f: F) -> ClosureRingBufferPostCreateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostCreateContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferPostCreateInterceptor::new(f)
}

pub struct RingBufferPreUpdateContext<'a> {
	pub pre: &'a RingBuffer,
}

impl<'a> RingBufferPreUpdateContext<'a> {
	pub fn new(pre: &'a RingBuffer) -> Self {
		Self {
			pre,
		}
	}
}

pub trait RingBufferPreUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferPreUpdateContext<'a>) -> Result<(), ValueError>;
}

impl InterceptorChain<dyn RingBufferPreUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferPreUpdateContext) -> Result<(), ValueError> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferPreUpdateInterceptor for ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferPreUpdateContext<'a>) -> Result<(), ValueError> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_pre_update<F>(f: F) -> ClosureRingBufferPreUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferPreUpdateInterceptor::new(f)
}

pub struct RingBufferPostUpdateContext<'a> {
	pub pre: &'a RingBuffer,
	pub post: &'a RingBuffer,
}

impl<'a> RingBufferPostUpdateContext<'a> {
	pub fn new(pre: &'a RingBuffer, post: &'a RingBuffer) -> Self {
		Self {
			pre,
			post,
		}
	}
}

pub trait RingBufferPostUpdateInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferPostUpdateContext<'a>) -> Result<(), ValueError>;
}

impl InterceptorChain<dyn RingBufferPostUpdateInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferPostUpdateContext) -> Result<(), ValueError> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferPostUpdateInterceptor for ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferPostUpdateContext<'a>) -> Result<(), ValueError> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_post_update<F>(f: F) -> ClosureRingBufferPostUpdateInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPostUpdateContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferPostUpdateInterceptor::new(f)
}

pub struct RingBufferPreDeleteContext<'a> {
	pub pre: &'a RingBuffer,
}

impl<'a> RingBufferPreDeleteContext<'a> {
	pub fn new(pre: &'a RingBuffer) -> Self {
		Self {
			pre,
		}
	}
}

pub trait RingBufferPreDeleteInterceptor: Send + Sync {
	fn intercept<'a>(&self, ctx: &mut RingBufferPreDeleteContext<'a>) -> Result<(), ValueError>;
}

impl InterceptorChain<dyn RingBufferPreDeleteInterceptor + Send + Sync> {
	pub fn execute(&self, mut ctx: RingBufferPreDeleteContext) -> Result<(), ValueError> {
		for interceptor in &self.interceptors {
			interceptor.intercept(&mut ctx)?;
		}
		Ok(())
	}
}

pub struct ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	closure: F,
}

impl<F> ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	pub fn new(closure: F) -> Self {
		Self {
			closure,
		}
	}
}

impl<F> Clone for ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone,
{
	fn clone(&self) -> Self {
		Self {
			closure: self.closure.clone(),
		}
	}
}

impl<F> RingBufferPreDeleteInterceptor for ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync,
{
	fn intercept<'a>(&self, ctx: &mut RingBufferPreDeleteContext<'a>) -> Result<(), ValueError> {
		(self.closure)(ctx)
	}
}

pub fn ringbuffer_pre_delete<F>(f: F) -> ClosureRingBufferPreDeleteInterceptor<F>
where
	F: for<'a> Fn(&mut RingBufferPreDeleteContext<'a>) -> Result<(), ValueError> + Send + Sync + Clone + 'static,
{
	ClosureRingBufferPreDeleteInterceptor::new(f)
}
