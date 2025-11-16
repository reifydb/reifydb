//! Host callback implementations for FFI operators
//!
//! This module provides the host-side implementations of callbacks that FFI operators
//! can invoke. The callbacks are organized into three categories:
//! - `memory`: Arena-based memory allocation
//! - `state`: Operator state management
//! - `logging`: Logging from FFI operators

pub mod logging;
pub mod memory;
pub mod state;
pub mod state_iterator;

// Re-export commonly used functions for arena management
pub use memory::{clear_current_arena, set_current_arena};
use reifydb_flow_operator_abi::{HostCallbacks, LogCallbacks, MemoryCallbacks, StateCallbacks};

/// Create the complete host callbacks structure
///
/// This aggregates all callback function pointers from the memory, state,
/// and logging modules into a single HostCallbacks structure that can be
/// passed to FFI operators.
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
	}
}
