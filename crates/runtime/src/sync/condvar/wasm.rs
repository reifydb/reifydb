// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM Condvar implementation (no-op).
//!
//! Since WASM is single-threaded, this is a no-op implementation.

use std::time::Duration;

use crate::sync::mutex::wasm::MutexGuard;

/// Result of a timed wait on a condition variable.
pub struct WaitTimeoutResult {
	timed_out: bool,
}

impl WaitTimeoutResult {
	/// Returns whether the wait timed out.
	pub fn timed_out(&self) -> bool {
		self.timed_out
	}
}

/// A condition variable for coordinating threads.
///
/// WASM implementation is a no-op (single-threaded).
#[derive(Debug)]
pub struct Condvar;

impl Condvar {
	/// Creates a new condition variable.
	pub fn new() -> Self {
		Self
	}

	/// No-op in WASM (never blocks).
	pub fn wait<'a, T>(&self, _guard: &mut MutexGuard<'a, T>) {
		// No-op: can't block in single-threaded WASM
	}

	/// No-op in WASM (returns immediately as if timed out).
	pub fn wait_for<'a, T>(&self, _guard: &mut MutexGuard<'a, T>, _timeout: Duration) -> WaitTimeoutResult {
		// Return timed_out=true to indicate timeout (can't actually wait)
		WaitTimeoutResult { timed_out: true }
	}

	/// No-op in WASM (no threads to wake).
	pub fn notify_one(&self) {
		// No-op
	}

	/// No-op in WASM (no threads to wake).
	pub fn notify_all(&self) {
		// No-op
	}
}

impl Default for Condvar {
	fn default() -> Self {
		Self::new()
	}
}
