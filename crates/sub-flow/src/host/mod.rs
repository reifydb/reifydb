//! Host runtime for FFI operators
//!
//! This module provides the host-side implementation for FFI operator integration,
//! including type marshalling, memory management, and callback implementations.

pub mod arena;
pub mod callbacks;
pub mod error;
pub mod loader;
pub mod marshalling;
pub mod registry;
pub mod transaction;

// Re-export main types
pub use arena::Arena;
pub use callbacks::{create_host_callbacks, HostCallbackContext};
pub use error::{FFIError, FFIResult};
pub use loader::FFIOperatorLoader;
pub use marshalling::FFIMarshaller;
pub use registry::FFIOperatorRegistry;
pub use transaction::TransactionHandle;