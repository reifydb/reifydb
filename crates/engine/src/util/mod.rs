// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

mod encode;

use std::future::Future;

pub use encode::encode_value;

/// Run an async future from a sync context.
/// This handles multiple scenarios:
/// 1. No runtime exists: create a temporary one
/// 2. Inside a runtime: spawn a thread to run the async work
///
/// The thread-spawning approach is safe for both multi-thread and current-thread runtimes.
pub fn block_on<F>(future: F) -> F::Output
where
	F: Future + Send,
	F::Output: Send,
{
	match tokio::runtime::Handle::try_current() {
		Ok(handle) => {
			// We're inside a tokio runtime context.
			// Spawn a new thread to run the async work to avoid blocking issues.
			std::thread::scope(|s| s.spawn(|| handle.block_on(future)).join().expect("Thread panicked"))
		}
		Err(_) => {
			// No runtime exists, create a temporary one
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("Failed to create runtime")
				.block_on(future)
		}
	}
}

/// Run an async future from a sync context without requiring Send.
/// This is used for futures created by #[async_trait(?Send)] methods.
///
/// IMPORTANT: This should only be called from within spawn_blocking or similar contexts
/// where we're already on a dedicated thread.
pub fn block_on_local<F>(future: F) -> F::Output
where
	F: Future,
{
	match tokio::runtime::Handle::try_current() {
		Ok(handle) => {
			// We're inside a tokio runtime context.
			// Use block_in_place to run the future on the current thread.
			tokio::task::block_in_place(|| handle.block_on(future))
		}
		Err(_) => {
			// No runtime exists, create a temporary one
			tokio::runtime::Builder::new_current_thread()
				.enable_all()
				.build()
				.expect("Failed to create runtime")
				.block_on(future)
		}
	}
}
