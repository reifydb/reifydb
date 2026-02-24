// SPDX-License-Identifier: AGPL-3.0-or-later
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
//!         .table_def("myns.users")
//!             .post_create(|ctx| { ... })
//!     .done()
//!     .build()
//! ```

use std::sync::Arc;

use reifydb_transaction::interceptor::{
	builder::InterceptorBuilder,
	filter::InterceptFilter,
	filtered::{
		FilteredNamespaceDefPostCreateInterceptor, FilteredNamespaceDefPostUpdateInterceptor,
		FilteredNamespaceDefPreDeleteInterceptor, FilteredNamespaceDefPreUpdateInterceptor,
		FilteredRingBufferDefPostCreateInterceptor, FilteredRingBufferDefPostUpdateInterceptor,
		FilteredRingBufferDefPreDeleteInterceptor, FilteredRingBufferDefPreUpdateInterceptor,
		FilteredRingBufferPostDeleteInterceptor, FilteredRingBufferPostInsertInterceptor,
		FilteredRingBufferPostUpdateInterceptor, FilteredRingBufferPreDeleteInterceptor,
		FilteredRingBufferPreInsertInterceptor, FilteredRingBufferPreUpdateInterceptor,
		FilteredTableDefPostCreateInterceptor, FilteredTableDefPostUpdateInterceptor,
		FilteredTableDefPreDeleteInterceptor, FilteredTableDefPreUpdateInterceptor,
		FilteredTablePostDeleteInterceptor, FilteredTablePostInsertInterceptor,
		FilteredTablePostUpdateInterceptor, FilteredTablePreDeleteInterceptor,
		FilteredTablePreInsertInterceptor, FilteredTablePreUpdateInterceptor,
		FilteredViewDefPostCreateInterceptor, FilteredViewDefPostUpdateInterceptor,
		FilteredViewDefPreDeleteInterceptor, FilteredViewDefPreUpdateInterceptor,
		FilteredViewPostDeleteInterceptor, FilteredViewPostInsertInterceptor,
		FilteredViewPostUpdateInterceptor, FilteredViewPreDeleteInterceptor, FilteredViewPreInsertInterceptor,
		FilteredViewPreUpdateInterceptor,
	},
	interceptors::Interceptors,
	namespace_def::{
		NamespaceDefPostCreateContext, NamespaceDefPostUpdateContext, NamespaceDefPreDeleteContext,
		NamespaceDefPreUpdateContext,
	},
	ringbuffer::{
		RingBufferPostDeleteContext, RingBufferPostInsertContext, RingBufferPostUpdateContext,
		RingBufferPreDeleteContext, RingBufferPreInsertContext, RingBufferPreUpdateContext,
	},
	ringbuffer_def::{
		RingBufferDefPostCreateContext, RingBufferDefPostUpdateContext, RingBufferDefPreDeleteContext,
		RingBufferDefPreUpdateContext,
	},
	table::{
		TablePostDeleteContext, TablePostInsertContext, TablePostUpdateContext, TablePreDeleteContext,
		TablePreInsertContext, TablePreUpdateContext,
	},
	table_def::{
		TableDefPostCreateContext, TableDefPostUpdateContext, TableDefPreDeleteContext,
		TableDefPreUpdateContext,
	},
	view::{
		ViewPostDeleteContext, ViewPostInsertContext, ViewPostUpdateContext, ViewPreDeleteContext,
		ViewPreInsertContext, ViewPreUpdateContext,
	},
	view_def::{
		ViewDefPostCreateContext, ViewDefPostUpdateContext, ViewDefPreDeleteContext, ViewDefPreUpdateContext,
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
/// or `.table_def(spec)`, `.view_def(spec)`, `.ringbuffer_def(spec)`, `.namespace_def(spec)`
/// for schema lifecycle operations.
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
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific table definition.
	pub fn table_def(self, spec: &str) -> TableDefInterceptBuilder<B> {
		TableDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific ring buffer.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific ring buffer definition.
	pub fn ringbuffer_def(self, spec: &str) -> RingBufferDefInterceptBuilder<B> {
		RingBufferDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific view.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific view definition.
	pub fn view_def(self, spec: &str) -> ViewDefInterceptBuilder<B> {
		ViewDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Start building interceptors for a specific namespace definition.
	pub fn namespace_def(self, spec: &str) -> NamespaceDefInterceptBuilder<B> {
		NamespaceDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

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
		F: Fn(&mut TablePreInsertContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_pre_insert
				.add(Arc::new(FilteredTablePreInsertInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-insert interceptor.
	pub fn post_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePostInsertContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_post_insert
				.add(Arc::new(FilteredTablePostInsertInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-update interceptor.
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

	/// Register a post-update interceptor.
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

	/// Register a pre-delete interceptor.
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

	/// Register a post-delete interceptor.
	pub fn post_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePostDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_post_delete
				.add(Arc::new(FilteredTablePostDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Switch to intercepting a different table.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a table definition.
	pub fn table_def(self, spec: &str) -> TableDefInterceptBuilder<B> {
		TableDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer definition.
	pub fn ringbuffer_def(self, spec: &str) -> RingBufferDefInterceptBuilder<B> {
		RingBufferDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view definition.
	pub fn view_def(self, spec: &str) -> ViewDefInterceptBuilder<B> {
		ViewDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a namespace definition.
	pub fn namespace_def(self, spec: &str) -> NamespaceDefInterceptBuilder<B> {
		NamespaceDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

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
		F: Fn(&mut RingBufferPreInsertContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.ringbuffer_pre_insert
				.add(Arc::new(FilteredRingBufferPreInsertInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-insert interceptor.
	pub fn post_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPostInsertContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.ringbuffer_post_insert
				.add(Arc::new(FilteredRingBufferPostInsertInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-update interceptor.
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

	/// Register a post-update interceptor.
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

	/// Register a pre-delete interceptor.
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

	/// Register a post-delete interceptor.
	pub fn post_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPostDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.ringbuffer_post_delete
				.add(Arc::new(FilteredRingBufferPostDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Switch to intercepting a table.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a table definition.
	pub fn table_def(self, spec: &str) -> TableDefInterceptBuilder<B> {
		TableDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different ring buffer.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer definition.
	pub fn ringbuffer_def(self, spec: &str) -> RingBufferDefInterceptBuilder<B> {
		RingBufferDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view definition.
	pub fn view_def(self, spec: &str) -> ViewDefInterceptBuilder<B> {
		ViewDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a namespace definition.
	pub fn namespace_def(self, spec: &str) -> NamespaceDefInterceptBuilder<B> {
		NamespaceDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

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

	/// Register a pre-insert interceptor for view data.
	pub fn pre_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPreInsertContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_pre_insert
				.add(Arc::new(FilteredViewPreInsertInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-insert interceptor for view data.
	pub fn post_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPostInsertContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_post_insert
				.add(Arc::new(FilteredViewPostInsertInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-update interceptor for view data.
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

	/// Register a post-update interceptor for view data.
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

	/// Register a pre-delete interceptor for view data.
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

	/// Register a post-delete interceptor for view data.
	pub fn post_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPostDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_post_delete
				.add(Arc::new(FilteredViewPostDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Switch to intercepting a table.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a table definition.
	pub fn table_def(self, spec: &str) -> TableDefInterceptBuilder<B> {
		TableDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer definition.
	pub fn ringbuffer_def(self, spec: &str) -> RingBufferDefInterceptBuilder<B> {
		RingBufferDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different view.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view definition.
	pub fn view_def(self, spec: &str) -> ViewDefInterceptBuilder<B> {
		ViewDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a namespace definition.
	pub fn namespace_def(self, spec: &str) -> NamespaceDefInterceptBuilder<B> {
		NamespaceDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

/// Fluent builder for table definition interceptors.
pub struct TableDefInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> TableDefInterceptBuilder<B> {
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
		F: Fn(&mut TableDefPostCreateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_def_post_create
				.add(Arc::new(FilteredTableDefPostCreateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-update interceptor for the table definition.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableDefPreUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_def_pre_update
				.add(Arc::new(FilteredTableDefPreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-update interceptor for the table definition.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableDefPostUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_def_post_update
				.add(Arc::new(FilteredTableDefPostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-delete interceptor for the table definition.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableDefPreDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_def_pre_delete
				.add(Arc::new(FilteredTableDefPreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Switch to intercepting a table.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different table definition.
	pub fn table_def(self, spec: &str) -> TableDefInterceptBuilder<B> {
		TableDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer definition.
	pub fn ringbuffer_def(self, spec: &str) -> RingBufferDefInterceptBuilder<B> {
		RingBufferDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view definition.
	pub fn view_def(self, spec: &str) -> ViewDefInterceptBuilder<B> {
		ViewDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a namespace definition.
	pub fn namespace_def(self, spec: &str) -> NamespaceDefInterceptBuilder<B> {
		NamespaceDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

/// Fluent builder for view definition interceptors.
pub struct ViewDefInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> ViewDefInterceptBuilder<B> {
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
		F: Fn(&mut ViewDefPostCreateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_def_post_create
				.add(Arc::new(FilteredViewDefPostCreateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-update interceptor for the view definition.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewDefPreUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_def_pre_update
				.add(Arc::new(FilteredViewDefPreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a post-update interceptor for the view definition.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewDefPostUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_def_post_update
				.add(Arc::new(FilteredViewDefPostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Register a pre-delete interceptor for the view definition.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewDefPreDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_def_pre_delete
				.add(Arc::new(FilteredViewDefPreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	/// Switch to intercepting a table.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a table definition.
	pub fn table_def(self, spec: &str) -> TableDefInterceptBuilder<B> {
		TableDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer definition.
	pub fn ringbuffer_def(self, spec: &str) -> RingBufferDefInterceptBuilder<B> {
		RingBufferDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different view definition.
	pub fn view_def(self, spec: &str) -> ViewDefInterceptBuilder<B> {
		ViewDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a namespace definition.
	pub fn namespace_def(self, spec: &str) -> NamespaceDefInterceptBuilder<B> {
		NamespaceDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

/// Fluent builder for ring buffer definition interceptors.
pub struct RingBufferDefInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> RingBufferDefInterceptBuilder<B> {
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
		F: Fn(&mut RingBufferDefPostCreateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_def_post_create.add(Arc::new(
				FilteredRingBufferDefPostCreateInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Register a pre-update interceptor for the ring buffer definition.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferDefPreUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_def_pre_update.add(Arc::new(
				FilteredRingBufferDefPreUpdateInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Register a post-update interceptor for the ring buffer definition.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferDefPostUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_def_post_update.add(Arc::new(
				FilteredRingBufferDefPostUpdateInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Register a pre-delete interceptor for the ring buffer definition.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferDefPreDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_def_pre_delete.add(Arc::new(
				FilteredRingBufferDefPreDeleteInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Switch to intercepting a table.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a table definition.
	pub fn table_def(self, spec: &str) -> TableDefInterceptBuilder<B> {
		TableDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different ring buffer definition.
	pub fn ringbuffer_def(self, spec: &str) -> RingBufferDefInterceptBuilder<B> {
		RingBufferDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view definition.
	pub fn view_def(self, spec: &str) -> ViewDefInterceptBuilder<B> {
		ViewDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a namespace definition.
	pub fn namespace_def(self, spec: &str) -> NamespaceDefInterceptBuilder<B> {
		NamespaceDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}

/// Fluent builder for namespace definition interceptors.
pub struct NamespaceDefInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> NamespaceDefInterceptBuilder<B> {
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
		F: Fn(&mut NamespaceDefPostCreateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.namespace_def_post_create.add(Arc::new(
				FilteredNamespaceDefPostCreateInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Register a pre-update interceptor for the namespace definition.
	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut NamespaceDefPreUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.namespace_def_pre_update.add(Arc::new(
				FilteredNamespaceDefPreUpdateInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Register a post-update interceptor for the namespace definition.
	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut NamespaceDefPostUpdateContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.namespace_def_post_update.add(Arc::new(
				FilteredNamespaceDefPostUpdateInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Register a pre-delete interceptor for the namespace definition.
	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut NamespaceDefPreDeleteContext) -> reifydb_type::Result<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = std::mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.namespace_def_pre_delete.add(Arc::new(
				FilteredNamespaceDefPreDeleteInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	/// Switch to intercepting a table.
	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a table definition.
	pub fn table_def(self, spec: &str) -> TableDefInterceptBuilder<B> {
		TableDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer.
	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a ring buffer definition.
	pub fn ringbuffer_def(self, spec: &str) -> RingBufferDefInterceptBuilder<B> {
		RingBufferDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view.
	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a view definition.
	pub fn view_def(self, spec: &str) -> ViewDefInterceptBuilder<B> {
		ViewDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Switch to intercepting a different namespace definition.
	pub fn namespace_def(self, spec: &str) -> NamespaceDefInterceptBuilder<B> {
		NamespaceDefInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	/// Finish and return the underlying builder.
	pub fn done(self) -> B {
		self.builder
	}
}
