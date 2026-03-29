// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Fluent builders for registering interceptors.
//!
//! Provides a chainable API for registering filtered interceptors:
//!
//! ```ignore
//! builder
//!     .intercept()
//!         .table("myns.users")
//!             .pre_insert(|ctx| { ... })
//!             .post_update(|ctx| { ... })
//!         .table("myns.orders")
//!             .post_insert(|ctx| { ... })
//!         .table("myns.users")
//!             .post_create(|ctx| { ... })
//!     .done()
//!     .build()
//! ```

use std::sync::Arc;

use reifydb_transaction::interceptor::{
	builder::InterceptorBuilder,
	filter::InterceptFilter,
	filtered::{
		FilteredNamespacePostCreateInterceptor, FilteredNamespacePostUpdateInterceptor,
		FilteredNamespacePreDeleteInterceptor, FilteredNamespacePreUpdateInterceptor,
		FilteredRingBufferPostCreateInterceptor, FilteredRingBufferPostUpdateInterceptor,
		FilteredRingBufferPreDeleteInterceptor, FilteredRingBufferPreUpdateInterceptor,
		FilteredRingBufferRowPostDeleteInterceptor, FilteredRingBufferRowPostInsertInterceptor,
		FilteredRingBufferRowPostUpdateInterceptor, FilteredRingBufferRowPreDeleteInterceptor,
		FilteredRingBufferRowPreInsertInterceptor, FilteredRingBufferRowPreUpdateInterceptor,
		FilteredTablePostCreateInterceptor, FilteredTablePostUpdateInterceptor,
		FilteredTablePreDeleteInterceptor, FilteredTablePreUpdateInterceptor,
		FilteredTableRowPostDeleteInterceptor, FilteredTableRowPostInsertInterceptor,
		FilteredTableRowPostUpdateInterceptor, FilteredTableRowPreDeleteInterceptor,
		FilteredTableRowPreInsertInterceptor, FilteredTableRowPreUpdateInterceptor,
		FilteredViewPostCreateInterceptor, FilteredViewPostUpdateInterceptor, FilteredViewPreDeleteInterceptor,
		FilteredViewPreUpdateInterceptor, FilteredViewRowPostDeleteInterceptor,
		FilteredViewRowPostInsertInterceptor, FilteredViewRowPostUpdateInterceptor,
		FilteredViewRowPreDeleteInterceptor, FilteredViewRowPreInsertInterceptor,
		FilteredViewRowPreUpdateInterceptor,
	},
	interceptors::Interceptors,
	namespace::{
		NamespacePostCreateContext, NamespacePostUpdateContext, NamespacePreDeleteContext,
		NamespacePreUpdateContext,
	},
	ringbuffer::{
		RingBufferPostCreateContext, RingBufferPostUpdateContext, RingBufferPreDeleteContext,
		RingBufferPreUpdateContext,
	},
	ringbuffer_row::{
		RingBufferRowPostDeleteContext, RingBufferRowPostInsertContext, RingBufferRowPostUpdateContext,
		RingBufferRowPreDeleteContext, RingBufferRowPreInsertContext, RingBufferRowPreUpdateContext,
	},
	table::{TablePostCreateContext, TablePostUpdateContext, TablePreDeleteContext, TablePreUpdateContext},
	table_row::{
		TableRowPostDeleteContext, TableRowPostInsertContext, TableRowPostUpdateContext,
		TableRowPreDeleteContext, TableRowPreInsertContext, TableRowPreUpdateContext,
	},
	view::{ViewPostCreateContext, ViewPostUpdateContext, ViewPreDeleteContext, ViewPreUpdateContext},
	view_row::{
		ViewRowPostDeleteContext, ViewRowPostInsertContext, ViewRowPostUpdateContext, ViewRowPreDeleteContext,
		ViewRowPreInsertContext, ViewRowPreUpdateContext,
	},
};

/// Trait for builders that support interceptor registration.
pub trait WithInterceptorBuilder: Sized {
	/// Get mutable access to the interceptor builder.
	fn interceptor_builder_mut(&mut self) -> &mut InterceptorBuilder;

	/// Start building interceptors.
	fn intercept(self) -> InterceptBuilder<Self> {
		InterceptBuilder::new(self)
	}
}

/// Intermediate builder returned by `.intercept()`.
///
/// Use `.table(spec)`, `.ringbuffer(spec)`, or `.view(spec)` to select data operations,
/// or `.table(spec)`, `.view(spec)`, `.ringbuffer(spec)`, `.namespace(spec)`
/// for shape lifecycle operations.
pub struct InterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
}

impl<B: WithInterceptorBuilder> InterceptBuilder<B> {
	/// Create a new intercept builder.
	pub fn new(builder: B) -> Self {
		Self {
			builder,
		}
	}

	/// Start building interceptors for a specific table.
	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific table definition.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific ring buffer.
	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific ring buffer definition.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific view.
	pub fn view_row(self, spec: &str) -> ViewRowInterceptBuilder<B> {
		ViewRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific view definition.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific namespace definition.
	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

/// Fluent builder for table interceptors.
pub struct TableRowInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> TableRowInterceptBuilder<B> {
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
		F: Fn(&mut TableRowPreInsertContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_row_pre_insert
				.add(Arc::new(FilteredTableRowPreInsertInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-insert interceptor.
	pub fn post_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableRowPostInsertContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_row_post_insert
				.add(Arc::new(FilteredTableRowPostInsertInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-update interceptor.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableRowPreUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_row_pre_update
				.add(Arc::new(FilteredTableRowPreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-update interceptor.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableRowPostUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_row_post_update
				.add(Arc::new(FilteredTableRowPostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-delete interceptor.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableRowPreDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_row_pre_delete
				.add(Arc::new(FilteredTableRowPreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-delete interceptor.
	pub fn post_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableRowPostDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_row_post_delete
				.add(Arc::new(FilteredTableRowPostDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Switch to intercepting a different table.
	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a table definition.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer definition.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn view_row(self, spec: &str) -> ViewRowInterceptBuilder<B> {
		ViewRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view definition.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a namespace definition.
	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

/// Fluent builder for ring buffer interceptors.
pub struct RingBufferRowInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> RingBufferRowInterceptBuilder<B> {
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
		F: Fn(&mut RingBufferRowPreInsertContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_row_pre_insert.add(Arc::new(
				FilteredRingBufferRowPreInsertInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Register a post-insert interceptor.
	pub fn post_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferRowPostInsertContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_row_post_insert.add(Arc::new(
				FilteredRingBufferRowPostInsertInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Register a pre-update interceptor.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferRowPreUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_row_pre_update.add(Arc::new(
				FilteredRingBufferRowPreUpdateInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Register a post-update interceptor.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferRowPostUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_row_post_update.add(Arc::new(
				FilteredRingBufferRowPostUpdateInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Register a pre-delete interceptor.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferRowPreDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_row_pre_delete.add(Arc::new(
				FilteredRingBufferRowPreDeleteInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Register a post-delete interceptor.
	pub fn post_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferRowPostDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_row_post_delete.add(Arc::new(
				FilteredRingBufferRowPostDeleteInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Switch to intercepting a table.
	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a table definition.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different ring buffer.
	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer definition.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn view_row(self, spec: &str) -> ViewRowInterceptBuilder<B> {
		ViewRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view definition.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a namespace definition.
	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

/// Fluent builder for view interceptors.
pub struct ViewRowInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> ViewRowInterceptBuilder<B> {
	/// Create a new view intercept builder.
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	/// Register a pre-insert interceptor for view data.
	pub fn pre_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewRowPreInsertContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_row_pre_insert
				.add(Arc::new(FilteredViewRowPreInsertInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-insert interceptor for view data.
	pub fn post_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewRowPostInsertContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_row_post_insert
				.add(Arc::new(FilteredViewRowPostInsertInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-update interceptor for view data.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewRowPreUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_row_pre_update
				.add(Arc::new(FilteredViewRowPreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-update interceptor for view data.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewRowPostUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_row_post_update
				.add(Arc::new(FilteredViewRowPostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-delete interceptor for view data.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewRowPreDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_row_pre_delete
				.add(Arc::new(FilteredViewRowPreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-delete interceptor for view data.
	pub fn post_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewRowPostDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_row_post_delete
				.add(Arc::new(FilteredViewRowPostDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Switch to intercepting a table.
	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a table definition.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer definition.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different view.
	pub fn view_row(self, spec: &str) -> ViewRowInterceptBuilder<B> {
		ViewRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view definition.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a namespace definition.
	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

/// Fluent builder for table definition interceptors.
pub struct TableInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> TableInterceptBuilder<B> {
	/// Create a new table def intercept builder.
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	/// Register a post-create interceptor for the table definition.
	pub fn post_create<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePostCreateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_post_create
				.add(Arc::new(FilteredTablePostCreateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-update interceptor for the table definition.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePreUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_pre_update
				.add(Arc::new(FilteredTablePreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-update interceptor for the table definition.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePostUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_post_update
				.add(Arc::new(FilteredTablePostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-delete interceptor for the table definition.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePreDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_pre_delete
				.add(Arc::new(FilteredTablePreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Switch to intercepting a table.
	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different table definition.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer definition.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn view_row(self, spec: &str) -> ViewRowInterceptBuilder<B> {
		ViewRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view definition.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a namespace definition.
	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

/// Fluent builder for view definition interceptors.
pub struct ViewInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> ViewInterceptBuilder<B> {
	/// Create a new view def intercept builder.
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	/// Register a post-create interceptor for the view definition.
	pub fn post_create<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPostCreateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_post_create
				.add(Arc::new(FilteredViewPostCreateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-update interceptor for the view definition.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPreUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_pre_update
				.add(Arc::new(FilteredViewPreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-update interceptor for the view definition.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPostUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_post_update
				.add(Arc::new(FilteredViewPostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-delete interceptor for the view definition.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPreDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_pre_delete
				.add(Arc::new(FilteredViewPreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Switch to intercepting a table.
	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a table definition.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer definition.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn view_row(self, spec: &str) -> ViewRowInterceptBuilder<B> {
		ViewRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different view definition.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a namespace definition.
	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

/// Fluent builder for ring buffer definition interceptors.
pub struct RingBufferInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> RingBufferInterceptBuilder<B> {
	/// Create a new ring buffer def intercept builder.
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	/// Register a post-create interceptor for the ring buffer definition.
	pub fn post_create<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPostCreateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.ringbuffer_post_create
				.add(Arc::new(FilteredRingBufferPostCreateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-update interceptor for the ring buffer definition.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPreUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.ringbuffer_pre_update
				.add(Arc::new(FilteredRingBufferPreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-update interceptor for the ring buffer definition.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPostUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.ringbuffer_post_update
				.add(Arc::new(FilteredRingBufferPostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-delete interceptor for the ring buffer definition.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPreDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.ringbuffer_pre_delete
				.add(Arc::new(FilteredRingBufferPreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Switch to intercepting a table.
	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a table definition.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different ring buffer definition.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn view_row(self, spec: &str) -> ViewRowInterceptBuilder<B> {
		ViewRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view definition.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a namespace definition.
	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

/// Fluent builder for namespace definition interceptors.
pub struct NamespaceInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> NamespaceInterceptBuilder<B> {
	/// Create a new namespace def intercept builder.
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	/// Register a post-create interceptor for the namespace definition.
	pub fn post_create<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut NamespacePostCreateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.namespace_post_create
				.add(Arc::new(FilteredNamespacePostCreateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-update interceptor for the namespace definition.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut NamespacePreUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.namespace_pre_update
				.add(Arc::new(FilteredNamespacePreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-update interceptor for the namespace definition.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut NamespacePostUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.namespace_post_update
				.add(Arc::new(FilteredNamespacePostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-delete interceptor for the namespace definition.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut NamespacePreDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.namespace_pre_delete
				.add(Arc::new(FilteredNamespacePreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Switch to intercepting a table.
	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a table definition.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer definition.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn view_row(self, spec: &str) -> ViewRowInterceptBuilder<B> {
		ViewRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view definition.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different namespace definition.
	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}
