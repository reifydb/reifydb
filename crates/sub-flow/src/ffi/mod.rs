//! Host runtime for FFI operators
//!
//! This module provides the host-side implementation for FFI operator integration,
//! including type marshalling, memory management, and callback implementations.

pub mod callbacks;
pub mod conversion;
pub mod error;
pub mod loader;
pub mod registry;
pub mod transaction;

// Re-export main types
pub use callbacks::{clear_current_arena, create_host_callbacks, set_current_arena};
pub use conversion::{from_operator_sdk_change, to_operator_sdk_change};
pub use error::{FFIError, FFIResult};
pub use loader::FFIOperatorLoader;
pub use registry::FFIOperatorRegistry;
// Re-export Arena and FFIMarshaller from flow-operator-sdk for backwards compatibility
pub use reifydb_flow_operator_sdk::ffi::{Arena, FFIMarshaller};
pub use transaction::TransactionHandle;
