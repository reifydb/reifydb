//! C ABI definitions for ReifyDB FFI operators
//!
//! This crate provides the stable C ABI interface that FFI operators must implement.
//! It defines FFI-safe types and function signatures for operators to interact with
//! the ReifyDB host system.

#![cfg_attr(not(debug_assertions), deny(warnings))]

pub mod callbacks;
pub mod constants;
pub mod ffi;
pub mod types;
pub mod vtable;

// Re-export main types
pub use callbacks::*;
pub use constants::*;
pub use ffi::*;
pub use types::*;
pub use vtable::*;
