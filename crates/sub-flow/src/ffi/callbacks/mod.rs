//! Host callback implementations for FFI operators
//!
//! This module provides the host-side implementations of callbacks that FFI operators
//! can invoke. The callbacks are organized into five categories:
//! - `memory`: Arena-based memory allocation
//! - `state`: Operator state management
//! - `store`: Read-only store access
//! - `logging`: Logging from FFI operators
//! - `catalog`: Read-only catalog access (namespaces, tables)

pub mod catalog;
pub mod logging;
pub mod memory;
pub mod state;
pub mod state_iterator;
pub mod store;
pub mod store_iterator;

// Re-export commonly used functions for arena management
pub use memory::{clear_current_arena, set_current_arena};
use reifydb_flow_operator_abi::{
	CatalogCallbacks, HostCallbacks, LogCallbacks, MemoryCallbacks, StateCallbacks, StoreCallbacks,
};

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
	}
}
