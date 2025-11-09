//! C ABI definitions for ReifyDB FFI operators
//!
//! This crate provides the stable C ABI interface that FFI operators must implement.
//! It defines FFI-safe types and function signatures for operators to interact with
//! the ReifyDB host system.

pub mod types;
pub mod vtable;
pub mod callbacks;
pub mod constants;

// Re-export main types
pub use types::*;
pub use vtable::*;
pub use callbacks::*;
pub use constants::*;