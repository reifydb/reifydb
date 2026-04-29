// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Host callback implementations for FFI operators
//!
//! This module provides the host-side implementations of callbacks that FFI operators
//! can invoke. The callbacks are organized into five categories:
//! - `memory`: Arena-based memory allocation
//! - `state`: Operator state management
//! - `store`: Read-only store access
//! - `logging`: Logging from FFI operators
//! - `catalog`: Read-only catalog access (namespaces, tables)

use reifydb_abi::{
	callbacks::{
		builder::BuilderCallbacks, catalog::CatalogCallbacks, host::HostCallbacks, log::LogCallbacks,
		memory::MemoryCallbacks, rql::RqlCallbacks, state::StateCallbacks, store::StoreCallbacks,
	},
	constants::FFI_ERROR_INTERNAL,
	context::context::ContextFFI,
	data::buffer::BufferFFI,
};
use reifydb_extension::procedure::ffi_callbacks::{logging, memory};

pub mod builder;
pub mod catalog;
pub mod state;
pub mod state_iterator;
pub mod store;
pub mod store_iterator;

/// Create the complete host callbacks structure
///
/// This aggregates all callback function pointers from the memory, state,
/// store, logging, and catalog modules into a single HostCallbacks structure that
/// can be passed to FFI operators.
pub fn create_host_callbacks() -> HostCallbacks {
	HostCallbacks {
		memory: MemoryCallbacks {
			alloc: memory::host_alloc,
			free: memory::host_free,
			realloc: memory::host_realloc,
		},
		state: StateCallbacks {
			get: state::host_state_get,
			set: state::host_state_set,
			remove: state::host_state_remove,
			clear: state::host_state_clear,
			prefix: state::host_state_prefix,
			range: state::host_state_range,
			iterator_next: state::host_state_iterator_next,
			iterator_free: state::host_state_iterator_free,
		},
		log: LogCallbacks {
			message: logging::host_log_message,
		},
		store: StoreCallbacks {
			get: store::host_store_get,
			contains_key: store::host_store_contains_key,
			prefix: store::host_store_prefix,
			range: store::host_store_range,
			iterator_next: store::host_store_iterator_next,
			iterator_free: store::host_store_iterator_free,
		},
		catalog: CatalogCallbacks {
			find_namespace: catalog::host_catalog_find_namespace,
			find_namespace_by_name: catalog::host_catalog_find_namespace_by_name,
			find_table: catalog::host_catalog_find_table,
			find_table_by_name: catalog::host_catalog_find_table_by_name,
			free_namespace: catalog::host_catalog_free_namespace,
			free_table: catalog::host_catalog_free_table,
		},
		rql: RqlCallbacks {
			rql: host_rql_unsupported,
		},
		builder: BuilderCallbacks {
			acquire: builder::host_builder_acquire,
			data_ptr: builder::host_builder_data_ptr,
			offsets_ptr: builder::host_builder_offsets_ptr,
			bitvec_ptr: builder::host_builder_bitvec_ptr,
			grow: builder::host_builder_grow,
			commit: builder::host_builder_commit,
			release: builder::host_builder_release,
			emit_diff: builder::host_builder_emit_diff,
		},
	}
}

/// Stub: RQL execution is not supported from sub-flow FFI operators.
unsafe extern "C" fn host_rql_unsupported(
	_ctx: *mut ContextFFI,
	_rql_ptr: *const u8,
	_rql_len: usize,
	_params_ptr: *const u8,
	_params_len: usize,
	_result_out: *mut BufferFFI,
) -> i32 {
	FFI_ERROR_INTERNAL
}
