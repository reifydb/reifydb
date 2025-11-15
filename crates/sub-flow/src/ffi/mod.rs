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
