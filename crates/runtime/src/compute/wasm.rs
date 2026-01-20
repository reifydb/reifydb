// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM compute pool implementation using sequential execution.

use std::sync::Arc;

/// WASM-compatible compute pool.
///
/// Uses sequential execution since WASM doesn't support threads.
/// All operations execute immediately on the current thread.
#[derive(Clone)]
pub struct WasmComputePool {
	// Marker to maintain similar API to native implementation
	_marker: Arc<()>,
}

impl WasmComputePool {
	/// Creates a new WASM compute pool.
	pub(crate) fn new() -> Self {
		Self {
			_marker: Arc::new(()),
		}
	}

	/// Executes a closure immediately (sequential execution).
	///
	/// In WASM, there's no thread pool, so this executes synchronously.
	pub fn install<R, F>(&self, f: F) -> R
	where
		R: Send,
		F: FnOnce() -> R + Send,
	{
		// Execute immediately (sequential)
		f()
	}

	/// Runs a CPU-bound function immediately (sequential execution).
	///
	/// In WASM, there's no thread pool or admission control, so this
	/// executes synchronously and returns immediately.
	pub async fn compute<R, F>(&self, f: F) -> Result<R, crate::runtime::WasmJoinError>
	where
		R: Send + 'static,
		F: FnOnce() -> R + Send + 'static,
	{
		// Execute immediately (sequential)
		Ok(f())
	}
}
