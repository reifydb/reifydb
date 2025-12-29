// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Testing utilities for the oracle module.
//!
//! This module provides hooks for injecting behavior into the oracle's
//! commit path, allowing tests to reliably trigger race conditions.

use std::{future::Future, pin::Pin};

use reifydb_core::CommitVersion;

use super::get_oracle_test_hook;

/// RAII guard that clears the oracle test hook when dropped.
///
/// This ensures the hook is always cleaned up, even if a test panics.
pub struct OracleTestHookGuard;

impl Drop for OracleTestHookGuard {
	fn drop(&mut self) {
		// Spawn a thread to clear the hook asynchronously
		// This avoids the "cannot start runtime from within runtime" issue
		std::thread::spawn(|| {
			if let Ok(rt) = tokio::runtime::Builder::new_current_thread().enable_all().build() {
				rt.block_on(async {
					*get_oracle_test_hook().lock().await = None;
				});
			}
		})
		.join()
		.ok();
	}
}

/// Set a test hook that runs between version allocation and begin().
///
/// The hook receives the allocated version and can perform async operations
/// like yielding to other tasks. This is useful for testing race conditions.
///
/// Returns an RAII guard that clears the hook when dropped.
///
/// # Example
/// ```ignore
/// let _guard = set_oracle_test_hook(|_version| {
///     Box::pin(async { tokio::task::yield_now().await })
/// }).await;
/// ```
pub async fn set_oracle_test_hook<F, Fut>(hook: F) -> OracleTestHookGuard
where
	F: Fn(CommitVersion) -> Fut + Send + Sync + 'static,
	Fut: Future<Output = ()> + Send + 'static,
{
	*get_oracle_test_hook().lock().await = Some(Box::new(move |v| Box::pin(hook(v))));
	OracleTestHookGuard
}

/// Clear the oracle test hook.
#[allow(dead_code)]
pub async fn clear_oracle_test_hook() {
	*get_oracle_test_hook().lock().await = None;
}

/// Create a hook that yields to other tasks.
///
/// This is useful for maximizing task interleaving in concurrent tests.
pub fn yield_hook() -> impl Fn(CommitVersion) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync {
	|_version| Box::pin(async { tokio::task::yield_now().await })
}
