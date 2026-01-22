// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! WASM Condvar implementation (no-op).
//!
//! Since WASM is single-threaded, this is a no-op implementation.

use std::time::Duration;

use crate::sync::mutex::MutexGuard;

/// WASM condition variable implementation (no-op since single-threaded).
#[derive(Debug)]
pub struct CondvarInner;

impl CondvarInner {
	/// Creates a new condition variable.
	pub fn new() -> Self {
		Self
	}

	/// No-op in WASM (never blocks).
	pub fn wait<'a, T>(&self, _guard: &mut MutexGuard<'a, T>) {
		// No-op: can't block in single-threaded WASM
	}

	/// No-op in WASM (returns immediately as if timed out).
	/// Returns true to indicate timeout (can't actually wait).
	pub fn wait_for<'a, T>(&self, _guard: &mut MutexGuard<'a, T>, _timeout: Duration) -> bool {
		true
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

impl Default for CondvarInner {
	fn default() -> Self {
		Self::new()
	}
}
