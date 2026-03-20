// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Filtered interceptor wrappers that only execute when the filter matches.
//!
//! These wrappers check the entity name against the filter before invoking the handler.
//! Note: Namespace filtering requires namespace name resolution which is currently a TODO.

use reifydb_type::Result;

use super::{
	dictionary::{
		DictionaryPostDeleteContext, DictionaryPostDeleteInterceptor, DictionaryPostInsertContext,
		DictionaryPostInsertInterceptor, DictionaryPostUpdateContext, DictionaryPostUpdateInterceptor,
		DictionaryPreDeleteContext, DictionaryPreDeleteInterceptor, DictionaryPreInsertContext,
		DictionaryPreInsertInterceptor, DictionaryPreUpdateContext, DictionaryPreUpdateInterceptor,
	},
	dictionary_def::{
		DictionaryDefPostCreateContext, DictionaryDefPostCreateInterceptor, DictionaryDefPostUpdateContext,
		DictionaryDefPostUpdateInterceptor, DictionaryDefPreDeleteContext, DictionaryDefPreDeleteInterceptor,
		DictionaryDefPreUpdateContext, DictionaryDefPreUpdateInterceptor,
	},
	filter::InterceptFilter,
	namespace::{
		NamespacePostCreateContext, NamespacePostCreateInterceptor, NamespacePostUpdateContext,
		NamespacePostUpdateInterceptor, NamespacePreDeleteContext, NamespacePreDeleteInterceptor,
		NamespacePreUpdateContext, NamespacePreUpdateInterceptor,
	},
	ringbuffer::{
		RingBufferPostDeleteContext, RingBufferPostDeleteInterceptor, RingBufferPostInsertContext,
		RingBufferPostInsertInterceptor, RingBufferPostUpdateContext, RingBufferPostUpdateInterceptor,
		RingBufferPreDeleteContext, RingBufferPreDeleteInterceptor, RingBufferPreInsertContext,
		RingBufferPreInsertInterceptor, RingBufferPreUpdateContext, RingBufferPreUpdateInterceptor,
	},
	ringbuffer_def::{
		RingBufferDefPostCreateContext, RingBufferDefPostCreateInterceptor, RingBufferDefPostUpdateContext,
		RingBufferDefPostUpdateInterceptor, RingBufferDefPreDeleteContext, RingBufferDefPreDeleteInterceptor,
		RingBufferDefPreUpdateContext, RingBufferDefPreUpdateInterceptor,
	},
	series::{
		SeriesPostDeleteContext, SeriesPostDeleteInterceptor, SeriesPostInsertContext,
		SeriesPostInsertInterceptor, SeriesPostUpdateContext, SeriesPostUpdateInterceptor,
		SeriesPreDeleteContext, SeriesPreDeleteInterceptor, SeriesPreInsertContext, SeriesPreInsertInterceptor,
		SeriesPreUpdateContext, SeriesPreUpdateInterceptor,
	},
	series_def::{
		SeriesDefPostCreateContext, SeriesDefPostCreateInterceptor, SeriesDefPostUpdateContext,
		SeriesDefPostUpdateInterceptor, SeriesDefPreDeleteContext, SeriesDefPreDeleteInterceptor,
		SeriesDefPreUpdateContext, SeriesDefPreUpdateInterceptor,
	},
	table::{
		TablePostDeleteContext, TablePostDeleteInterceptor, TablePostInsertContext, TablePostInsertInterceptor,
		TablePostUpdateContext, TablePostUpdateInterceptor, TablePreDeleteContext, TablePreDeleteInterceptor,
		TablePreInsertContext, TablePreInsertInterceptor, TablePreUpdateContext, TablePreUpdateInterceptor,
	},
	table_def::{
		TableDefPostCreateContext, TableDefPostCreateInterceptor, TableDefPostUpdateContext,
		TableDefPostUpdateInterceptor, TableDefPreDeleteContext, TableDefPreDeleteInterceptor,
		TableDefPreUpdateContext, TableDefPreUpdateInterceptor,
	},
	view::{
		ViewPostDeleteContext, ViewPostDeleteInterceptor, ViewPostInsertContext, ViewPostInsertInterceptor,
		ViewPostUpdateContext, ViewPostUpdateInterceptor, ViewPreDeleteContext, ViewPreDeleteInterceptor,
		ViewPreInsertContext, ViewPreInsertInterceptor, ViewPreUpdateContext, ViewPreUpdateInterceptor,
	},
	view_def::{
		ViewDefPostCreateContext, ViewDefPostCreateInterceptor, ViewDefPostUpdateContext,
		ViewDefPostUpdateInterceptor, ViewDefPreDeleteContext, ViewDefPreDeleteInterceptor,
		ViewDefPreUpdateContext, ViewDefPreUpdateInterceptor,
	},
};

/// Macro to generate filtered interceptor wrapper types.
///
/// The 4-arg form accesses the entity name via `ctx.$entity_field.name` (for struct types).
/// The 5-arg form accesses it via `ctx.$entity_field.$name_method()` (for enum types like Namespace).
macro_rules! define_filtered_interceptor {
	(
		$wrapper_name:ident,
		$trait_name:ident,
		$context_type:ident,
		$entity_field:ident
	) => {
		/// Filtered interceptor wrapper that checks entity name before executing.
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
		/// Filtered interceptor wrapper that checks entity name before executing.
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

// Table filtered interceptors
define_filtered_interceptor!(
	FilteredTablePreInsertInterceptor,
	TablePreInsertInterceptor,
	TablePreInsertContext,
	table
);

define_filtered_interceptor!(
	FilteredTablePostInsertInterceptor,
	TablePostInsertInterceptor,
	TablePostInsertContext,
	table
);

define_filtered_interceptor!(
	FilteredTablePreUpdateInterceptor,
	TablePreUpdateInterceptor,
	TablePreUpdateContext,
	table
);

define_filtered_interceptor!(
	FilteredTablePostUpdateInterceptor,
	TablePostUpdateInterceptor,
	TablePostUpdateContext,
	table
);

define_filtered_interceptor!(
	FilteredTablePreDeleteInterceptor,
	TablePreDeleteInterceptor,
	TablePreDeleteContext,
	table
);

define_filtered_interceptor!(
	FilteredTablePostDeleteInterceptor,
	TablePostDeleteInterceptor,
	TablePostDeleteContext,
	table
);

// Ring buffer filtered interceptors
define_filtered_interceptor!(
	FilteredRingBufferPreInsertInterceptor,
	RingBufferPreInsertInterceptor,
	RingBufferPreInsertContext,
	ringbuffer
);

define_filtered_interceptor!(
	FilteredRingBufferPostInsertInterceptor,
	RingBufferPostInsertInterceptor,
	RingBufferPostInsertContext,
	ringbuffer
);

define_filtered_interceptor!(
	FilteredRingBufferPreUpdateInterceptor,
	RingBufferPreUpdateInterceptor,
	RingBufferPreUpdateContext,
	ringbuffer
);

define_filtered_interceptor!(
	FilteredRingBufferPostUpdateInterceptor,
	RingBufferPostUpdateInterceptor,
	RingBufferPostUpdateContext,
	ringbuffer
);

define_filtered_interceptor!(
	FilteredRingBufferPreDeleteInterceptor,
	RingBufferPreDeleteInterceptor,
	RingBufferPreDeleteContext,
	ringbuffer
);

define_filtered_interceptor!(
	FilteredRingBufferPostDeleteInterceptor,
	RingBufferPostDeleteInterceptor,
	RingBufferPostDeleteContext,
	ringbuffer
);

// View data filtered interceptors
define_filtered_interceptor!(FilteredViewPreInsertInterceptor, ViewPreInsertInterceptor, ViewPreInsertContext, view);

define_filtered_interceptor!(FilteredViewPostInsertInterceptor, ViewPostInsertInterceptor, ViewPostInsertContext, view);

define_filtered_interceptor!(FilteredViewPreUpdateInterceptor, ViewPreUpdateInterceptor, ViewPreUpdateContext, view);

define_filtered_interceptor!(FilteredViewPostUpdateInterceptor, ViewPostUpdateInterceptor, ViewPostUpdateContext, view);

define_filtered_interceptor!(FilteredViewPreDeleteInterceptor, ViewPreDeleteInterceptor, ViewPreDeleteContext, view);

define_filtered_interceptor!(FilteredViewPostDeleteInterceptor, ViewPostDeleteInterceptor, ViewPostDeleteContext, view);

// View definition filtered interceptors
define_filtered_interceptor!(
	FilteredViewDefPostCreateInterceptor,
	ViewDefPostCreateInterceptor,
	ViewDefPostCreateContext,
	post
);

define_filtered_interceptor!(
	FilteredViewDefPreUpdateInterceptor,
	ViewDefPreUpdateInterceptor,
	ViewDefPreUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredViewDefPostUpdateInterceptor,
	ViewDefPostUpdateInterceptor,
	ViewDefPostUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredViewDefPreDeleteInterceptor,
	ViewDefPreDeleteInterceptor,
	ViewDefPreDeleteContext,
	pre
);

// Table definition filtered interceptors
define_filtered_interceptor!(
	FilteredTableDefPostCreateInterceptor,
	TableDefPostCreateInterceptor,
	TableDefPostCreateContext,
	post
);

define_filtered_interceptor!(
	FilteredTableDefPreUpdateInterceptor,
	TableDefPreUpdateInterceptor,
	TableDefPreUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredTableDefPostUpdateInterceptor,
	TableDefPostUpdateInterceptor,
	TableDefPostUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredTableDefPreDeleteInterceptor,
	TableDefPreDeleteInterceptor,
	TableDefPreDeleteContext,
	pre
);

// Ring buffer definition filtered interceptors
define_filtered_interceptor!(
	FilteredRingBufferDefPostCreateInterceptor,
	RingBufferDefPostCreateInterceptor,
	RingBufferDefPostCreateContext,
	post
);

define_filtered_interceptor!(
	FilteredRingBufferDefPreUpdateInterceptor,
	RingBufferDefPreUpdateInterceptor,
	RingBufferDefPreUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredRingBufferDefPostUpdateInterceptor,
	RingBufferDefPostUpdateInterceptor,
	RingBufferDefPostUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredRingBufferDefPreDeleteInterceptor,
	RingBufferDefPreDeleteInterceptor,
	RingBufferDefPreDeleteContext,
	pre
);

// Series data filtered interceptors
define_filtered_interceptor!(
	FilteredSeriesPreInsertInterceptor,
	SeriesPreInsertInterceptor,
	SeriesPreInsertContext,
	series
);

define_filtered_interceptor!(
	FilteredSeriesPostInsertInterceptor,
	SeriesPostInsertInterceptor,
	SeriesPostInsertContext,
	series
);

define_filtered_interceptor!(
	FilteredSeriesPreUpdateInterceptor,
	SeriesPreUpdateInterceptor,
	SeriesPreUpdateContext,
	series
);

define_filtered_interceptor!(
	FilteredSeriesPostUpdateInterceptor,
	SeriesPostUpdateInterceptor,
	SeriesPostUpdateContext,
	series
);

define_filtered_interceptor!(
	FilteredSeriesPreDeleteInterceptor,
	SeriesPreDeleteInterceptor,
	SeriesPreDeleteContext,
	series
);

define_filtered_interceptor!(
	FilteredSeriesPostDeleteInterceptor,
	SeriesPostDeleteInterceptor,
	SeriesPostDeleteContext,
	series
);

// Series definition filtered interceptors
define_filtered_interceptor!(
	FilteredSeriesDefPostCreateInterceptor,
	SeriesDefPostCreateInterceptor,
	SeriesDefPostCreateContext,
	post
);

define_filtered_interceptor!(
	FilteredSeriesDefPreUpdateInterceptor,
	SeriesDefPreUpdateInterceptor,
	SeriesDefPreUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredSeriesDefPostUpdateInterceptor,
	SeriesDefPostUpdateInterceptor,
	SeriesDefPostUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredSeriesDefPreDeleteInterceptor,
	SeriesDefPreDeleteInterceptor,
	SeriesDefPreDeleteContext,
	pre
);

// Dictionary data filtered interceptors
define_filtered_interceptor!(
	FilteredDictionaryPreInsertInterceptor,
	DictionaryPreInsertInterceptor,
	DictionaryPreInsertContext,
	dictionary
);

define_filtered_interceptor!(
	FilteredDictionaryPostInsertInterceptor,
	DictionaryPostInsertInterceptor,
	DictionaryPostInsertContext,
	dictionary
);

define_filtered_interceptor!(
	FilteredDictionaryPreUpdateInterceptor,
	DictionaryPreUpdateInterceptor,
	DictionaryPreUpdateContext,
	dictionary
);

define_filtered_interceptor!(
	FilteredDictionaryPostUpdateInterceptor,
	DictionaryPostUpdateInterceptor,
	DictionaryPostUpdateContext,
	dictionary
);

define_filtered_interceptor!(
	FilteredDictionaryPreDeleteInterceptor,
	DictionaryPreDeleteInterceptor,
	DictionaryPreDeleteContext,
	dictionary
);

define_filtered_interceptor!(
	FilteredDictionaryPostDeleteInterceptor,
	DictionaryPostDeleteInterceptor,
	DictionaryPostDeleteContext,
	dictionary
);

// Dictionary definition filtered interceptors
define_filtered_interceptor!(
	FilteredDictionaryDefPostCreateInterceptor,
	DictionaryDefPostCreateInterceptor,
	DictionaryDefPostCreateContext,
	post
);

define_filtered_interceptor!(
	FilteredDictionaryDefPreUpdateInterceptor,
	DictionaryDefPreUpdateInterceptor,
	DictionaryDefPreUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredDictionaryDefPostUpdateInterceptor,
	DictionaryDefPostUpdateInterceptor,
	DictionaryDefPostUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredDictionaryDefPreDeleteInterceptor,
	DictionaryDefPreDeleteInterceptor,
	DictionaryDefPreDeleteContext,
	pre
);

// Namespace filtered interceptors
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
