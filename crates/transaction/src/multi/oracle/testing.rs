// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

//! Testing utilities for the oracle module.
//!
//! This module provides hooks for injecting behavior into the oracle's
//! commit path, allowing tests to reliably trigger race conditions.

use std::sync::Arc;

use reifydb_core::CommitVersion;

use super::get_oracle_test_hook;

/// RAII guard that clears the oracle test hook when dropped.
///
/// This ensures the hook is always cleaned up, even if a test panics.
pub struct OracleTestHookGuard;

impl Drop for OracleTestHookGuard {
	fn drop(&mut self) {
		// Clear the hook synchronously
		*get_oracle_test_hook().lock() = None;
	}
}

/// Set a test hook that runs between version allocation and begin().
///
/// The hook receives the allocated version and can perform synchronous operations
/// like yielding to other threads. This is useful for testing race conditions.
///
/// Returns an RAII guard that clears the hook when dropped.
///
/// # Example
/// ```ignore
/// let _guard = set_oracle_test_hook(|_version| {
///     std::thread::yield_now()
/// });
/// ```
pub fn set_oracle_test_hook<F>(hook: F) -> OracleTestHookGuard
where
	F: Fn(CommitVersion) + Send + Sync + 'static,
{
	*get_oracle_test_hook().lock() = Some(Arc::new(hook));
	OracleTestHookGuard
}

/// Clear the oracle test hook.
#[allow(dead_code)]
pub fn clear_oracle_test_hook() {
	*get_oracle_test_hook().lock() = None;
}

/// Create a hook that yields to other threads.
///
/// This is useful for maximizing thread interleaving in concurrent tests.
pub fn yield_hook() -> impl Fn(CommitVersion) + Send + Sync {
	|_version| std::thread::yield_now()
}
