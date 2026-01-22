// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM runtime implementation using single-threaded execution.

use std::{future::Future, pin::Pin, task::Poll};

use futures_util::future::LocalBoxFuture;

use crate::compute::wasm::WasmComputePool;

/// WASM-compatible runtime implementation.
///
/// Uses single-threaded execution since WASM doesn't support OS threads.
pub(crate) struct WasmRuntime {
	compute_pool: WasmComputePool,
}

impl WasmRuntime {
	/// Create a new WASM runtime.
	///
	/// Thread configuration parameters are ignored in WASM builds.
	pub(crate) fn new(_async_threads: usize, _compute_threads: usize, _compute_max_in_flight: usize) -> Self {
		Self {
			compute_pool: WasmComputePool::new(),
		}
	}

	/// Get a handle to the runtime (WASM placeholder).
	pub(crate) fn handle(&self) -> WasmHandle {
		WasmHandle
	}

	/// Get the compute pool for CPU-bound work.
	pub(crate) fn compute_pool(&self) -> WasmComputePool {
		self.compute_pool.clone()
	}

	/// Spawn a future onto the runtime.
	///
	/// In WASM, this creates a join handle that can be awaited.
	pub(crate) fn spawn<F>(&self, future: F) -> WasmJoinHandle<F::Output>
	where
		F: Future + 'static,
		F::Output: 'static,
	{
		WasmJoinHandle {
			future: Box::pin(future),
		}
	}

	/// Block the current thread until the future completes.
	///
	/// **Note:** Not supported in WASM builds - will panic.
	pub(crate) fn block_on<F>(&self, _future: F) -> F::Output
	where
		F: Future,
	{
		unimplemented!("block_on not supported in WASM - use async execution instead")
	}
}

impl Clone for WasmRuntime {
	fn clone(&self) -> Self {
		Self {
			compute_pool: self.compute_pool.clone(),
		}
	}
}

/// WASM-compatible handle (placeholder).
#[derive(Clone, Copy, Debug)]
pub struct WasmHandle;

/// WASM-compatible join handle.
///
/// Implements Future to be compatible with async/await.
pub struct WasmJoinHandle<T> {
	future: LocalBoxFuture<'static, T>,
}

impl<T> Future for WasmJoinHandle<T> {
	type Output = Result<T, WasmJoinError>;

	fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
		match self.future.as_mut().poll(cx) {
			Poll::Ready(v) => Poll::Ready(Ok(v)),
			Poll::Pending => Poll::Pending,
		}
	}
}

/// WASM join error (compatible with tokio::task::JoinError API).
#[derive(Debug)]
pub struct WasmJoinError;

impl std::fmt::Display for WasmJoinError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "WASM task failed")
	}
}

impl std::error::Error for WasmJoinError {}
