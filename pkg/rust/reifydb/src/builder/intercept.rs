// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Fluent builders for registering interceptors.
//!
//! Provides a chainable API for registering filtered interceptors:
//!
//! ```ignore
//! builder
//!     .intercept_table("myns.users")
//!         .pre_insert(|ctx| { ... })
//!         .post_update(|ctx| { ... })
//!     .intercept_table("myns.orders")
//!         .post_insert(|ctx| { ... })
//!     .build()
//! ```

use std::sync::Arc;

use reifydb_core::interceptor::{
	FilteredRingBufferPostDeleteInterceptor, FilteredRingBufferPostInsertInterceptor,
	FilteredRingBufferPostUpdateInterceptor, FilteredRingBufferPreDeleteInterceptor,
	FilteredRingBufferPreInsertInterceptor, FilteredRingBufferPreUpdateInterceptor,
	FilteredTablePostDeleteInterceptor, FilteredTablePostInsertInterceptor, FilteredTablePostUpdateInterceptor,
	FilteredTablePreDeleteInterceptor, FilteredTablePreInsertInterceptor, FilteredTablePreUpdateInterceptor,
	InterceptFilter, Interceptors, RingBufferPostDeleteContext, RingBufferPostInsertContext,
	RingBufferPostUpdateContext, RingBufferPreDeleteContext, RingBufferPreInsertContext,
	RingBufferPreUpdateContext, StandardInterceptorBuilder, TablePostDeleteContext, TablePostInsertContext,
	TablePostUpdateContext, TablePreDeleteContext, TablePreInsertContext, TablePreUpdateContext,
};
use reifydb_engine::StandardCommandTransaction;

/// Trait for builders that support interceptor registration.
pub trait WithInterceptorBuilder: Sized {
	/// Get mutable access to the interceptor builder.
	fn interceptor_builder_mut(&mut self) -> &mut StandardInterceptorBuilder<StandardCommandTransaction>;

	/// Start building interceptors for a specific table.
	fn intercept_table(self, spec: &str) -> TableInterceptBuilder<Self> {
		TableInterceptBuilder::new(self, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific ring buffer.
	fn intercept_ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<Self> {
		RingBufferInterceptBuilder::new(self, InterceptFilter::parse(spec))
	}
}

// =============================================================================
// Table Intercept Builder
// =============================================================================

/// Fluent builder for table interceptors.
pub struct TableInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> TableInterceptBuilder<B> {
	/// Create a new table intercept builder.
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	/// Register a pre-insert interceptor.
	pub fn pre_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePreInsertContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.table_pre_insert.add(Arc::new(FilteredTablePreInsertInterceptor::new(
					filter.clone(),
					f.clone(),
				)));
			},
		);
		self
	}

	/// Register a post-insert interceptor.
	pub fn post_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePostInsertContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.table_post_insert.add(Arc::new(FilteredTablePostInsertInterceptor::new(
					filter.clone(),
					f.clone(),
				)));
			},
		);
		self
	}

	/// Register a pre-update interceptor.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePreUpdateContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.table_pre_update.add(Arc::new(FilteredTablePreUpdateInterceptor::new(
					filter.clone(),
					f.clone(),
				)));
			},
		);
		self
	}

	/// Register a post-update interceptor.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePostUpdateContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.table_post_update.add(Arc::new(FilteredTablePostUpdateInterceptor::new(
					filter.clone(),
					f.clone(),
				)));
			},
		);
		self
	}

	/// Register a pre-delete interceptor.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePreDeleteContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.table_pre_delete.add(Arc::new(FilteredTablePreDeleteInterceptor::new(
					filter.clone(),
					f.clone(),
				)));
			},
		);
		self
	}

	/// Register a post-delete interceptor.
	pub fn post_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePostDeleteContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.table_post_delete.add(Arc::new(FilteredTablePostDeleteInterceptor::new(
					filter.clone(),
					f.clone(),
				)));
			},
		);
		self
	}

	/// Switch to intercepting a different table.
	pub fn intercept_table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn intercept_ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

// =============================================================================
// Ring Buffer Intercept Builder
// =============================================================================

/// Fluent builder for ring buffer interceptors.
pub struct RingBufferInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> RingBufferInterceptBuilder<B> {
	/// Create a new ring buffer intercept builder.
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	/// Register a pre-insert interceptor.
	pub fn pre_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPreInsertContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.ringbuffer_pre_insert.add(Arc::new(
					FilteredRingBufferPreInsertInterceptor::new(filter.clone(), f.clone()),
				));
			},
		);
		self
	}

	/// Register a post-insert interceptor.
	pub fn post_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPostInsertContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.ringbuffer_post_insert.add(Arc::new(
					FilteredRingBufferPostInsertInterceptor::new(filter.clone(), f.clone()),
				));
			},
		);
		self
	}

	/// Register a pre-update interceptor.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPreUpdateContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.ringbuffer_pre_update.add(Arc::new(
					FilteredRingBufferPreUpdateInterceptor::new(filter.clone(), f.clone()),
				));
			},
		);
		self
	}

	/// Register a post-update interceptor.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPostUpdateContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.ringbuffer_post_update.add(Arc::new(
					FilteredRingBufferPostUpdateInterceptor::new(filter.clone(), f.clone()),
				));
			},
		);
		self
	}

	/// Register a pre-delete interceptor.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPreDeleteContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.ringbuffer_pre_delete.add(Arc::new(
					FilteredRingBufferPreDeleteInterceptor::new(filter.clone(), f.clone()),
				));
			},
		);
		self
	}

	/// Register a post-delete interceptor.
	pub fn post_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPostDeleteContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.ringbuffer_post_delete.add(Arc::new(
					FilteredRingBufferPostDeleteInterceptor::new(filter.clone(), f.clone()),
				));
			},
		);
		self
	}

	/// Switch to intercepting a table.
	pub fn intercept_table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different ring buffer.
	pub fn intercept_ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}
