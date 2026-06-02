// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb_abi::callbacks::{
	builder::BuilderCallbacks, catalog::CatalogCallbacks, host::HostCallbacks, log::LogCallbacks,
	memory::MemoryCallbacks, rql::RqlCallbacks, state::StateCallbacks, store::StoreCallbacks,
};
use reifydb_extension::{
	ffi_callbacks::builder,
	procedure::ffi_callbacks::{logging, memory},
};

pub mod catalog;
mod marshal;
pub mod rql;
pub mod state;
pub mod state_iterator;
pub mod store;
pub mod store_iterator;

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
			internal_get: state::host_internal_state_get,
			internal_set: state::host_internal_state_set,
			internal_remove: state::host_internal_state_remove,
			get_many: state::host_state_get_many,
			internal_get_many: state::host_state_internal_get_many,
			allocate_row_numbers: state::host_allocate_row_numbers,
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
			find_row_shape: catalog::host_catalog_find_row_shape,
			free_namespace: catalog::host_catalog_free_namespace,
			free_table: catalog::host_catalog_free_table,
			free_row_shape: catalog::host_catalog_free_row_shape,
		},
		rql: RqlCallbacks {
			rql: rql::host_rql,
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
