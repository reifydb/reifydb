// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

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

use std::rc::Rc;

use reifydb_core::interceptor::{
	FilteredRingBufferPostDeleteInterceptor, FilteredRingBufferPostInsertInterceptor,
	FilteredRingBufferPostUpdateInterceptor, FilteredRingBufferPreDeleteInterceptor,
	FilteredRingBufferPreInsertInterceptor, FilteredRingBufferPreUpdateInterceptor,
	FilteredTablePostDeleteInterceptor, FilteredTablePostInsertInterceptor, FilteredTablePostUpdateInterceptor,
	FilteredTablePreDeleteInterceptor, FilteredTablePreInsertInterceptor, FilteredTablePreUpdateInterceptor,
	FilteredViewPostDeleteInterceptor, FilteredViewPostInsertInterceptor, FilteredViewPostUpdateInterceptor,
	FilteredViewPreDeleteInterceptor, FilteredViewPreInsertInterceptor, FilteredViewPreUpdateInterceptor,
	InterceptFilter, Interceptors, RingBufferPostDeleteContext, RingBufferPostInsertContext,
	RingBufferPostUpdateContext, RingBufferPreDeleteContext, RingBufferPreInsertContext,
	RingBufferPreUpdateContext, StandardInterceptorBuilder, TablePostDeleteContext, TablePostInsertContext,
	TablePostUpdateContext, TablePreDeleteContext, TablePreInsertContext, TablePreUpdateContext,
	ViewPostDeleteContext, ViewPostInsertContext, ViewPostUpdateContext, ViewPreDeleteContext,
	ViewPreInsertContext, ViewPreUpdateContext,
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
	fn intercept_ring_buffer(self, spec: &str) -> RingBufferInterceptBuilder<Self> {
		RingBufferInterceptBuilder::new(self, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific view.
	fn intercept_view(self, spec: &str) -> ViewInterceptBuilder<Self> {
		ViewInterceptBuilder::new(self, InterceptFilter::parse(spec))
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
				interceptors.table_pre_insert.add(Rc::new(FilteredTablePreInsertInterceptor::new(
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
				interceptors.table_post_insert.add(Rc::new(FilteredTablePostInsertInterceptor::new(
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
				interceptors.table_pre_update.add(Rc::new(FilteredTablePreUpdateInterceptor::new(
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
				interceptors.table_post_update.add(Rc::new(FilteredTablePostUpdateInterceptor::new(
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
				interceptors.table_pre_delete.add(Rc::new(FilteredTablePreDeleteInterceptor::new(
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
				interceptors.table_post_delete.add(Rc::new(FilteredTablePostDeleteInterceptor::new(
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
	pub fn intercept_ring_buffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn intercept_view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
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
				interceptors.ring_buffer_pre_insert.add(Rc::new(
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
				interceptors.ring_buffer_post_insert.add(Rc::new(
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
				interceptors.ring_buffer_pre_update.add(Rc::new(
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
				interceptors.ring_buffer_post_update.add(Rc::new(
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
				interceptors.ring_buffer_pre_delete.add(Rc::new(
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
				interceptors.ring_buffer_post_delete.add(Rc::new(
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
	pub fn intercept_ring_buffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn intercept_view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

// =============================================================================
// View Intercept Builder
// =============================================================================

/// Fluent builder for view interceptors.
pub struct ViewInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> ViewInterceptBuilder<B> {
	/// Create a new view intercept builder.
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	/// Register a pre-insert interceptor.
	pub fn pre_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPreInsertContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors
					.view_pre_insert
					.add(Rc::new(FilteredViewPreInsertInterceptor::new(filter.clone(), f.clone())));
			},
		);
		self
	}

	/// Register a post-insert interceptor.
	pub fn post_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPostInsertContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.view_post_insert.add(Rc::new(FilteredViewPostInsertInterceptor::new(
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
		F: Fn(&mut ViewPreUpdateContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors
					.view_pre_update
					.add(Rc::new(FilteredViewPreUpdateInterceptor::new(filter.clone(), f.clone())));
			},
		);
		self
	}

	/// Register a post-update interceptor.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPostUpdateContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.view_post_update.add(Rc::new(FilteredViewPostUpdateInterceptor::new(
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
		F: Fn(&mut ViewPreDeleteContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors
					.view_pre_delete
					.add(Rc::new(FilteredViewPreDeleteInterceptor::new(filter.clone(), f.clone())));
			},
		);
		self
	}

	/// Register a post-delete interceptor.
	pub fn post_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPostDeleteContext<StandardCommandTransaction>) -> reifydb_core::Result<()>
			+ Send
			+ Sync
			+ Clone
			+ 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(
			move |interceptors: &mut Interceptors<StandardCommandTransaction>| {
				interceptors.view_post_delete.add(Rc::new(FilteredViewPostDeleteInterceptor::new(
					filter.clone(),
					f.clone(),
				)));
			},
		);
		self
	}

	/// Switch to intercepting a table.
	pub fn intercept_table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn intercept_ring_buffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different view.
	pub fn intercept_view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}
