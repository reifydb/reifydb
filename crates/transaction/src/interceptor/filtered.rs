// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_type::Result;

use super::{
	dictionary::{
		DictionaryPostCreateContext, DictionaryPostCreateInterceptor, DictionaryPostUpdateContext,
		DictionaryPostUpdateInterceptor, DictionaryPreDeleteContext, DictionaryPreDeleteInterceptor,
		DictionaryPreUpdateContext, DictionaryPreUpdateInterceptor,
	},
	dictionary_row::{
		DictionaryRowPostDeleteContext, DictionaryRowPostDeleteInterceptor, DictionaryRowPostInsertContext,
		DictionaryRowPostInsertInterceptor, DictionaryRowPostUpdateContext, DictionaryRowPostUpdateInterceptor,
		DictionaryRowPreDeleteContext, DictionaryRowPreDeleteInterceptor, DictionaryRowPreInsertContext,
		DictionaryRowPreInsertInterceptor, DictionaryRowPreUpdateContext, DictionaryRowPreUpdateInterceptor,
	},
	filter::InterceptFilter,
	identity::{
		IdentityPostCreateContext, IdentityPostCreateInterceptor, IdentityPostUpdateContext,
		IdentityPostUpdateInterceptor, IdentityPreDeleteContext, IdentityPreDeleteInterceptor,
		IdentityPreUpdateContext, IdentityPreUpdateInterceptor,
	},
	namespace::{
		NamespacePostCreateContext, NamespacePostCreateInterceptor, NamespacePostUpdateContext,
		NamespacePostUpdateInterceptor, NamespacePreDeleteContext, NamespacePreDeleteInterceptor,
		NamespacePreUpdateContext, NamespacePreUpdateInterceptor,
	},
	ringbuffer::{
		RingBufferPostCreateContext, RingBufferPostCreateInterceptor, RingBufferPostUpdateContext,
		RingBufferPostUpdateInterceptor, RingBufferPreDeleteContext, RingBufferPreDeleteInterceptor,
		RingBufferPreUpdateContext, RingBufferPreUpdateInterceptor,
	},
	ringbuffer_row::{
		RingBufferRowPostDeleteContext, RingBufferRowPostDeleteInterceptor, RingBufferRowPostInsertContext,
		RingBufferRowPostInsertInterceptor, RingBufferRowPostUpdateContext, RingBufferRowPostUpdateInterceptor,
		RingBufferRowPreDeleteContext, RingBufferRowPreDeleteInterceptor, RingBufferRowPreInsertContext,
		RingBufferRowPreInsertInterceptor, RingBufferRowPreUpdateContext, RingBufferRowPreUpdateInterceptor,
	},
	role::{
		RolePostCreateContext, RolePostCreateInterceptor, RolePostUpdateContext, RolePostUpdateInterceptor,
		RolePreDeleteContext, RolePreDeleteInterceptor, RolePreUpdateContext, RolePreUpdateInterceptor,
	},
	series::{
		SeriesPostCreateContext, SeriesPostCreateInterceptor, SeriesPostUpdateContext,
		SeriesPostUpdateInterceptor, SeriesPreDeleteContext, SeriesPreDeleteInterceptor,
		SeriesPreUpdateContext, SeriesPreUpdateInterceptor,
	},
	series_row::{
		SeriesRowPostDeleteContext, SeriesRowPostDeleteInterceptor, SeriesRowPostInsertContext,
		SeriesRowPostInsertInterceptor, SeriesRowPostUpdateContext, SeriesRowPostUpdateInterceptor,
		SeriesRowPreDeleteContext, SeriesRowPreDeleteInterceptor, SeriesRowPreInsertContext,
		SeriesRowPreInsertInterceptor, SeriesRowPreUpdateContext, SeriesRowPreUpdateInterceptor,
	},
	table::{
		TablePostCreateContext, TablePostCreateInterceptor, TablePostUpdateContext, TablePostUpdateInterceptor,
		TablePreDeleteContext, TablePreDeleteInterceptor, TablePreUpdateContext, TablePreUpdateInterceptor,
	},
	table_row::{
		TableRowPostDeleteContext, TableRowPostDeleteInterceptor, TableRowPostInsertContext,
		TableRowPostInsertInterceptor, TableRowPostUpdateContext, TableRowPostUpdateInterceptor,
		TableRowPreDeleteContext, TableRowPreDeleteInterceptor, TableRowPreInsertContext,
		TableRowPreInsertInterceptor, TableRowPreUpdateContext, TableRowPreUpdateInterceptor,
	},
	view::{
		ViewPostCreateContext, ViewPostCreateInterceptor, ViewPostUpdateContext, ViewPostUpdateInterceptor,
		ViewPreDeleteContext, ViewPreDeleteInterceptor, ViewPreUpdateContext, ViewPreUpdateInterceptor,
	},
	view_row::{
		ViewRowPostDeleteContext, ViewRowPostDeleteInterceptor, ViewRowPostInsertContext,
		ViewRowPostInsertInterceptor, ViewRowPostUpdateContext, ViewRowPostUpdateInterceptor,
		ViewRowPreDeleteContext, ViewRowPreDeleteInterceptor, ViewRowPreInsertContext,
		ViewRowPreInsertInterceptor, ViewRowPreUpdateContext, ViewRowPreUpdateInterceptor,
	},
};

macro_rules! define_filtered_interceptor {
	(
		$wrapper_name:ident,
		$trait_name:ident,
		$context_type:ident,
		$entity_field:ident
	) => {
		pub struct $wrapper_name<F>
		where
			F: for<'a> Fn(&mut $context_type<'a>) -> Result<()> + Send + Sync,
		{
			filter: InterceptFilter,
			handler: F,
		}

		impl<F> $wrapper_name<F>
		where
			F: for<'a> Fn(&mut $context_type<'a>) -> Result<()> + Send + Sync,
		{
			pub fn new(filter: InterceptFilter, handler: F) -> Self {
				Self {
					filter,
					handler,
				}
			}
		}

		impl<F> Clone for $wrapper_name<F>
		where
			F: for<'a> Fn(&mut $context_type<'a>) -> Result<()> + Send + Sync + Clone,
		{
			fn clone(&self) -> Self {
				Self {
					filter: self.filter.clone(),
					handler: self.handler.clone(),
				}
			}
		}

		impl<F> $trait_name for $wrapper_name<F>
		where
			F: for<'a> Fn(&mut $context_type<'a>) -> Result<()> + Send + Sync,
		{
			fn intercept<'a>(&self, ctx: &mut $context_type<'a>) -> Result<()> {
				let entity_name = ctx.$entity_field.name();
				let name_matches =
					self.filter.name.as_ref().map_or(true, |n| n.as_str() == entity_name);
				if name_matches {
					(self.handler)(ctx)
				} else {
					Ok(())
				}
			}
		}
	};
	(
		$wrapper_name:ident,
		$trait_name:ident,
		$context_type:ident,
		$entity_field:ident,
		$name_method:ident
	) => {
		pub struct $wrapper_name<F>
		where
			F: for<'a> Fn(&mut $context_type<'a>) -> Result<()> + Send + Sync,
		{
			filter: InterceptFilter,
			handler: F,
		}

		impl<F> $wrapper_name<F>
		where
			F: for<'a> Fn(&mut $context_type<'a>) -> Result<()> + Send + Sync,
		{
			pub fn new(filter: InterceptFilter, handler: F) -> Self {
				Self {
					filter,
					handler,
				}
			}
		}

		impl<F> Clone for $wrapper_name<F>
		where
			F: for<'a> Fn(&mut $context_type<'a>) -> Result<()> + Send + Sync + Clone,
		{
			fn clone(&self) -> Self {
				Self {
					filter: self.filter.clone(),
					handler: self.handler.clone(),
				}
			}
		}

		impl<F> $trait_name for $wrapper_name<F>
		where
			F: for<'a> Fn(&mut $context_type<'a>) -> Result<()> + Send + Sync,
		{
			fn intercept<'a>(&self, ctx: &mut $context_type<'a>) -> Result<()> {
				let entity_name = ctx.$entity_field.$name_method();
				let name_matches =
					self.filter.name.as_ref().map_or(true, |n| n.as_str() == entity_name);
				if name_matches {
					(self.handler)(ctx)
				} else {
					Ok(())
				}
			}
		}
	};
}

define_filtered_interceptor!(
	FilteredTableRowPreInsertInterceptor,
	TableRowPreInsertInterceptor,
	TableRowPreInsertContext,
	table
);

define_filtered_interceptor!(
	FilteredTableRowPostInsertInterceptor,
	TableRowPostInsertInterceptor,
	TableRowPostInsertContext,
	table
);

define_filtered_interceptor!(
	FilteredTableRowPreUpdateInterceptor,
	TableRowPreUpdateInterceptor,
	TableRowPreUpdateContext,
	table
);

define_filtered_interceptor!(
	FilteredTableRowPostUpdateInterceptor,
	TableRowPostUpdateInterceptor,
	TableRowPostUpdateContext,
	table
);

define_filtered_interceptor!(
	FilteredTableRowPreDeleteInterceptor,
	TableRowPreDeleteInterceptor,
	TableRowPreDeleteContext,
	table
);

define_filtered_interceptor!(
	FilteredTableRowPostDeleteInterceptor,
	TableRowPostDeleteInterceptor,
	TableRowPostDeleteContext,
	table
);

define_filtered_interceptor!(
	FilteredRingBufferRowPreInsertInterceptor,
	RingBufferRowPreInsertInterceptor,
	RingBufferRowPreInsertContext,
	ringbuffer
);

define_filtered_interceptor!(
	FilteredRingBufferRowPostInsertInterceptor,
	RingBufferRowPostInsertInterceptor,
	RingBufferRowPostInsertContext,
	ringbuffer
);

define_filtered_interceptor!(
	FilteredRingBufferRowPreUpdateInterceptor,
	RingBufferRowPreUpdateInterceptor,
	RingBufferRowPreUpdateContext,
	ringbuffer
);

define_filtered_interceptor!(
	FilteredRingBufferRowPostUpdateInterceptor,
	RingBufferRowPostUpdateInterceptor,
	RingBufferRowPostUpdateContext,
	ringbuffer
);

define_filtered_interceptor!(
	FilteredRingBufferRowPreDeleteInterceptor,
	RingBufferRowPreDeleteInterceptor,
	RingBufferRowPreDeleteContext,
	ringbuffer
);

define_filtered_interceptor!(
	FilteredRingBufferRowPostDeleteInterceptor,
	RingBufferRowPostDeleteInterceptor,
	RingBufferRowPostDeleteContext,
	ringbuffer
);

define_filtered_interceptor!(
	FilteredViewRowPreInsertInterceptor,
	ViewRowPreInsertInterceptor,
	ViewRowPreInsertContext,
	view
);

define_filtered_interceptor!(
	FilteredViewRowPostInsertInterceptor,
	ViewRowPostInsertInterceptor,
	ViewRowPostInsertContext,
	view
);

define_filtered_interceptor!(
	FilteredViewRowPreUpdateInterceptor,
	ViewRowPreUpdateInterceptor,
	ViewRowPreUpdateContext,
	view
);

define_filtered_interceptor!(
	FilteredViewRowPostUpdateInterceptor,
	ViewRowPostUpdateInterceptor,
	ViewRowPostUpdateContext,
	view
);

define_filtered_interceptor!(
	FilteredViewRowPreDeleteInterceptor,
	ViewRowPreDeleteInterceptor,
	ViewRowPreDeleteContext,
	view
);

define_filtered_interceptor!(
	FilteredViewRowPostDeleteInterceptor,
	ViewRowPostDeleteInterceptor,
	ViewRowPostDeleteContext,
	view
);

define_filtered_interceptor!(FilteredViewPostCreateInterceptor, ViewPostCreateInterceptor, ViewPostCreateContext, post);

define_filtered_interceptor!(FilteredViewPreUpdateInterceptor, ViewPreUpdateInterceptor, ViewPreUpdateContext, pre);

define_filtered_interceptor!(FilteredViewPostUpdateInterceptor, ViewPostUpdateInterceptor, ViewPostUpdateContext, pre);

define_filtered_interceptor!(FilteredViewPreDeleteInterceptor, ViewPreDeleteInterceptor, ViewPreDeleteContext, pre);

define_filtered_interceptor!(
	FilteredTablePostCreateInterceptor,
	TablePostCreateInterceptor,
	TablePostCreateContext,
	post
);

define_filtered_interceptor!(FilteredTablePreUpdateInterceptor, TablePreUpdateInterceptor, TablePreUpdateContext, pre);

define_filtered_interceptor!(
	FilteredTablePostUpdateInterceptor,
	TablePostUpdateInterceptor,
	TablePostUpdateContext,
	pre
);

define_filtered_interceptor!(FilteredTablePreDeleteInterceptor, TablePreDeleteInterceptor, TablePreDeleteContext, pre);

define_filtered_interceptor!(
	FilteredRingBufferPostCreateInterceptor,
	RingBufferPostCreateInterceptor,
	RingBufferPostCreateContext,
	post
);

define_filtered_interceptor!(
	FilteredRingBufferPreUpdateInterceptor,
	RingBufferPreUpdateInterceptor,
	RingBufferPreUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredRingBufferPostUpdateInterceptor,
	RingBufferPostUpdateInterceptor,
	RingBufferPostUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredRingBufferPreDeleteInterceptor,
	RingBufferPreDeleteInterceptor,
	RingBufferPreDeleteContext,
	pre
);

define_filtered_interceptor!(
	FilteredSeriesRowPreInsertInterceptor,
	SeriesRowPreInsertInterceptor,
	SeriesRowPreInsertContext,
	series
);

define_filtered_interceptor!(
	FilteredSeriesRowPostInsertInterceptor,
	SeriesRowPostInsertInterceptor,
	SeriesRowPostInsertContext,
	series
);

define_filtered_interceptor!(
	FilteredSeriesRowPreUpdateInterceptor,
	SeriesRowPreUpdateInterceptor,
	SeriesRowPreUpdateContext,
	series
);

define_filtered_interceptor!(
	FilteredSeriesRowPostUpdateInterceptor,
	SeriesRowPostUpdateInterceptor,
	SeriesRowPostUpdateContext,
	series
);

define_filtered_interceptor!(
	FilteredSeriesRowPreDeleteInterceptor,
	SeriesRowPreDeleteInterceptor,
	SeriesRowPreDeleteContext,
	series
);

define_filtered_interceptor!(
	FilteredSeriesRowPostDeleteInterceptor,
	SeriesRowPostDeleteInterceptor,
	SeriesRowPostDeleteContext,
	series
);

define_filtered_interceptor!(
	FilteredSeriesPostCreateInterceptor,
	SeriesPostCreateInterceptor,
	SeriesPostCreateContext,
	post
);

define_filtered_interceptor!(
	FilteredSeriesPreUpdateInterceptor,
	SeriesPreUpdateInterceptor,
	SeriesPreUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredSeriesPostUpdateInterceptor,
	SeriesPostUpdateInterceptor,
	SeriesPostUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredSeriesPreDeleteInterceptor,
	SeriesPreDeleteInterceptor,
	SeriesPreDeleteContext,
	pre
);

define_filtered_interceptor!(
	FilteredDictionaryRowPreInsertInterceptor,
	DictionaryRowPreInsertInterceptor,
	DictionaryRowPreInsertContext,
	dictionary
);

define_filtered_interceptor!(
	FilteredDictionaryRowPostInsertInterceptor,
	DictionaryRowPostInsertInterceptor,
	DictionaryRowPostInsertContext,
	dictionary
);

define_filtered_interceptor!(
	FilteredDictionaryRowPreUpdateInterceptor,
	DictionaryRowPreUpdateInterceptor,
	DictionaryRowPreUpdateContext,
	dictionary
);

define_filtered_interceptor!(
	FilteredDictionaryRowPostUpdateInterceptor,
	DictionaryRowPostUpdateInterceptor,
	DictionaryRowPostUpdateContext,
	dictionary
);

define_filtered_interceptor!(
	FilteredDictionaryRowPreDeleteInterceptor,
	DictionaryRowPreDeleteInterceptor,
	DictionaryRowPreDeleteContext,
	dictionary
);

define_filtered_interceptor!(
	FilteredDictionaryRowPostDeleteInterceptor,
	DictionaryRowPostDeleteInterceptor,
	DictionaryRowPostDeleteContext,
	dictionary
);

define_filtered_interceptor!(
	FilteredDictionaryPostCreateInterceptor,
	DictionaryPostCreateInterceptor,
	DictionaryPostCreateContext,
	post
);

define_filtered_interceptor!(
	FilteredDictionaryPreUpdateInterceptor,
	DictionaryPreUpdateInterceptor,
	DictionaryPreUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredDictionaryPostUpdateInterceptor,
	DictionaryPostUpdateInterceptor,
	DictionaryPostUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredDictionaryPreDeleteInterceptor,
	DictionaryPreDeleteInterceptor,
	DictionaryPreDeleteContext,
	pre
);

define_filtered_interceptor!(
	FilteredNamespacePostCreateInterceptor,
	NamespacePostCreateInterceptor,
	NamespacePostCreateContext,
	post,
	name
);

define_filtered_interceptor!(
	FilteredNamespacePreUpdateInterceptor,
	NamespacePreUpdateInterceptor,
	NamespacePreUpdateContext,
	pre,
	name
);

define_filtered_interceptor!(
	FilteredNamespacePostUpdateInterceptor,
	NamespacePostUpdateInterceptor,
	NamespacePostUpdateContext,
	pre,
	name
);

define_filtered_interceptor!(
	FilteredNamespacePreDeleteInterceptor,
	NamespacePreDeleteInterceptor,
	NamespacePreDeleteContext,
	pre,
	name
);

define_filtered_interceptor!(
	FilteredIdentityPostCreateInterceptor,
	IdentityPostCreateInterceptor,
	IdentityPostCreateContext,
	post
);

define_filtered_interceptor!(
	FilteredIdentityPreUpdateInterceptor,
	IdentityPreUpdateInterceptor,
	IdentityPreUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredIdentityPostUpdateInterceptor,
	IdentityPostUpdateInterceptor,
	IdentityPostUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredIdentityPreDeleteInterceptor,
	IdentityPreDeleteInterceptor,
	IdentityPreDeleteContext,
	pre
);

define_filtered_interceptor!(FilteredRolePostCreateInterceptor, RolePostCreateInterceptor, RolePostCreateContext, post);

define_filtered_interceptor!(FilteredRolePreUpdateInterceptor, RolePreUpdateInterceptor, RolePreUpdateContext, pre);

define_filtered_interceptor!(FilteredRolePostUpdateInterceptor, RolePostUpdateInterceptor, RolePostUpdateContext, pre);

define_filtered_interceptor!(FilteredRolePreDeleteInterceptor, RolePreDeleteInterceptor, RolePreDeleteContext, pre);
