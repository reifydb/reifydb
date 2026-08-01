// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{mem, sync::Arc};

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
		FilteredViewPreUpdateInterceptor,
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
};
use reifydb_value::Result as TypeResult;

pub trait WithInterceptorBuilder: Sized {
	fn interceptor_builder_mut(&mut self) -> &mut InterceptorBuilder;

	fn intercept(self) -> InterceptBuilder<Self> {
		InterceptBuilder::new(self)
	}
}

pub struct InterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
}

impl<B: WithInterceptorBuilder> InterceptBuilder<B> {
	pub fn new(builder: B) -> Self {
		Self {
			builder,
		}
	}

	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn done(self) -> B {
		self.builder
	}
}

pub struct TableRowInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> TableRowInterceptBuilder<B> {
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	pub fn pre_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableRowPreInsertContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_row_pre_insert
				.add(Arc::new(FilteredTableRowPreInsertInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn post_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableRowPostInsertContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_row_post_insert
				.add(Arc::new(FilteredTableRowPostInsertInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableRowPreUpdateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_row_pre_update
				.add(Arc::new(FilteredTableRowPreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableRowPostUpdateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_row_post_update
				.add(Arc::new(FilteredTableRowPostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableRowPreDeleteContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_row_pre_delete
				.add(Arc::new(FilteredTableRowPreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn post_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TableRowPostDeleteContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_row_post_delete
				.add(Arc::new(FilteredTableRowPostDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn done(self) -> B {
		self.builder
	}
}

pub struct RingBufferRowInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> RingBufferRowInterceptBuilder<B> {
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	pub fn pre_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferRowPreInsertContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_row_pre_insert.add(Arc::new(
				FilteredRingBufferRowPreInsertInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	pub fn post_insert<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferRowPostInsertContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_row_post_insert.add(Arc::new(
				FilteredRingBufferRowPostInsertInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferRowPreUpdateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_row_pre_update.add(Arc::new(
				FilteredRingBufferRowPreUpdateInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferRowPostUpdateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_row_post_update.add(Arc::new(
				FilteredRingBufferRowPostUpdateInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferRowPreDeleteContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_row_pre_delete.add(Arc::new(
				FilteredRingBufferRowPreDeleteInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	pub fn post_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferRowPostDeleteContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors.ringbuffer_row_post_delete.add(Arc::new(
				FilteredRingBufferRowPostDeleteInterceptor::new(filter.clone(), f.clone()),
			));
		});
		self
	}

	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn done(self) -> B {
		self.builder
	}
}

pub struct TableInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> TableInterceptBuilder<B> {
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	pub fn post_create<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePostCreateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_post_create
				.add(Arc::new(FilteredTablePostCreateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePreUpdateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_pre_update
				.add(Arc::new(FilteredTablePreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePostUpdateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_post_update
				.add(Arc::new(FilteredTablePostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut TablePreDeleteContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.table_pre_delete
				.add(Arc::new(FilteredTablePreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn done(self) -> B {
		self.builder
	}
}

pub struct ViewInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> ViewInterceptBuilder<B> {
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	pub fn post_create<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPostCreateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_post_create
				.add(Arc::new(FilteredViewPostCreateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPreUpdateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_pre_update
				.add(Arc::new(FilteredViewPreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPostUpdateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_post_update
				.add(Arc::new(FilteredViewPostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut ViewPreDeleteContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.view_pre_delete
				.add(Arc::new(FilteredViewPreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn done(self) -> B {
		self.builder
	}
}

pub struct RingBufferInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> RingBufferInterceptBuilder<B> {
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	pub fn post_create<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPostCreateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.ringbuffer_post_create
				.add(Arc::new(FilteredRingBufferPostCreateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPreUpdateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.ringbuffer_pre_update
				.add(Arc::new(FilteredRingBufferPreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPostUpdateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.ringbuffer_post_update
				.add(Arc::new(FilteredRingBufferPostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut RingBufferPreDeleteContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.ringbuffer_pre_delete
				.add(Arc::new(FilteredRingBufferPreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn done(self) -> B {
		self.builder
	}
}

pub struct NamespaceInterceptBuilder<B: WithInterceptorBuilder> {
	builder: B,
	filter: InterceptFilter,
}

impl<B: WithInterceptorBuilder> NamespaceInterceptBuilder<B> {
	pub fn new(builder: B, filter: InterceptFilter) -> Self {
		Self {
			builder,
			filter,
		}
	}

	pub fn post_create<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut NamespacePostCreateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.namespace_post_create
				.add(Arc::new(FilteredNamespacePostCreateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn pre_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut NamespacePreUpdateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.namespace_pre_update
				.add(Arc::new(FilteredNamespacePreUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn post_update<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut NamespacePostUpdateContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.namespace_post_update
				.add(Arc::new(FilteredNamespacePostUpdateInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn pre_delete<F>(mut self, f: F) -> Self
	where
		F: Fn(&mut NamespacePreDeleteContext) -> TypeResult<()> + Send + Sync + Clone + 'static,
	{
		let filter = self.filter.clone();
		let builder = self.builder.interceptor_builder_mut();
		*builder = mem::take(builder).add_factory(move |interceptors: &mut Interceptors| {
			interceptors
				.namespace_pre_delete
				.add(Arc::new(FilteredNamespacePreDeleteInterceptor::new(filter.clone(), f.clone())));
		});
		self
	}

	pub fn table_row(self, spec: &str) -> TableRowInterceptBuilder<B> {
		TableRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn table(self, spec: &str) -> TableInterceptBuilder<B> {
		TableInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer_row(self, spec: &str) -> RingBufferRowInterceptBuilder<B> {
		RingBufferRowInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn ringbuffer(self, spec: &str) -> RingBufferInterceptBuilder<B> {
		RingBufferInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn view(self, spec: &str) -> ViewInterceptBuilder<B> {
		ViewInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn namespace(self, spec: &str) -> NamespaceInterceptBuilder<B> {
		NamespaceInterceptBuilder::new(self.builder, InterceptFilter::parse(spec))
	}

	pub fn done(self) -> B {
		self.builder
	}
}
