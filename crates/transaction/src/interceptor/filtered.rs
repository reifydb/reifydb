// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Filtered interceptor wrappers that only execute when the filter matches.
//!
//! These wrappers check the entity name against the filter before invoking the handler.
//! Note: Namespace filtering requires namespace name resolution which is currently a TODO.

use super::{
	filter::InterceptFilter,
	namespace_def::{
		NamespaceDefPostCreateContext, NamespaceDefPostCreateInterceptor, NamespaceDefPostUpdateContext,
		NamespaceDefPostUpdateInterceptor, NamespaceDefPreDeleteContext, NamespaceDefPreDeleteInterceptor,
		NamespaceDefPreUpdateContext, NamespaceDefPreUpdateInterceptor,
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
			F: for<'a> Fn(&mut $context_type<'a>) -> reifydb_type::Result<()> + Send + Sync,
		{
			filter: InterceptFilter,
			handler: F,
		}

		impl<F> $wrapper_name<F>
		where
			F: for<'a> Fn(&mut $context_type<'a>) -> reifydb_type::Result<()> + Send + Sync,
		{
			/// Create a new filtered interceptor.
			pub fn new(filter: InterceptFilter, handler: F) -> Self {
				Self {
					filter,
					handler,
				}
			}
		}

		impl<F> Clone for $wrapper_name<F>
		where
			F: for<'a> Fn(&mut $context_type<'a>) -> reifydb_type::Result<()> + Send + Sync + Clone,
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
			F: for<'a> Fn(&mut $context_type<'a>) -> reifydb_type::Result<()> + Send + Sync,
		{
			fn intercept<'a>(&self, ctx: &mut $context_type<'a>) -> reifydb_type::Result<()> {
				// TODO: Add namespace matching once we have namespace name resolution.
				// For now, we only match by entity name if namespace is not specified in filter,
				// or skip namespace check entirely.
				let entity_name = &ctx.$entity_field.name;

				// Check if name matches (or filter allows all names)
				let name_matches = self.filter.name.as_ref().map_or(true, |n| n == entity_name);

				// TODO: Namespace matching - for now we skip namespace check if filter has namespace
				// This means "ns.table" currently only matches by table name
				let ns_matches = true; // Placeholder until we add namespace resolution

				if name_matches && ns_matches {
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

// Namespace definition filtered interceptors
define_filtered_interceptor!(
	FilteredNamespaceDefPostCreateInterceptor,
	NamespaceDefPostCreateInterceptor,
	NamespaceDefPostCreateContext,
	post
);

define_filtered_interceptor!(
	FilteredNamespaceDefPreUpdateInterceptor,
	NamespaceDefPreUpdateInterceptor,
	NamespaceDefPreUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredNamespaceDefPostUpdateInterceptor,
	NamespaceDefPostUpdateInterceptor,
	NamespaceDefPostUpdateContext,
	pre
);

define_filtered_interceptor!(
	FilteredNamespaceDefPreDeleteInterceptor,
	NamespaceDefPreDeleteInterceptor,
	NamespaceDefPreDeleteContext,
	pre
);
