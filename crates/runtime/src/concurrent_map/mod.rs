//! Concurrent map abstraction that provides a unified API across native and WASM targets.
//!
//! On native platforms, this wraps `DashMap` for high-performance concurrent access.
//! On WASM platforms, this wraps `Arc<RwLock<HashMap>>` to provide similar semantics.

#[cfg(feature = "native")]
pub mod native;

#[cfg(feature = "wasm")]
pub mod wasm;
