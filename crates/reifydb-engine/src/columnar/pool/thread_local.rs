// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

//! Thread-local pool storage for avoiding explicit pool passing
//!
//! This module provides thread-local storage for pools, allowing ColumnData
//! operations to access pools without needing to pass them through the entire
//! call stack.

use std::cell::RefCell;

use super::Pools;

thread_local! {
    /// Thread-local storage for pools instance
    static THREAD_POOLS: RefCell<Option<Pools>> = RefCell::new(None);
}

/// Set the thread-local pools instance for the current thread
pub fn set_thread_pools(pools: Pools) {
	THREAD_POOLS.with(|p| {
		*p.borrow_mut() = Some(pools);
	});
}

/// Get a clone of the thread-local pools instance
/// Returns None if not initialized
pub fn get_thread_pools() -> Option<Pools> {
	THREAD_POOLS.with(|p| p.borrow().clone())
}

/// Get the thread-local pools instance or panic with helpful message
pub fn thread_pools() -> Pools {
	get_thread_pools().expect(
        "Thread-local pools not initialized. Call init_thread_pools() or set_thread_pools() first."
    )
}

/// Execute a function with access to thread-local pools
pub fn with_thread_pools<F, R>(f: F) -> R
where
	F: FnOnce(&Pools) -> R,
{
	THREAD_POOLS.with(|p| {
        let pools = p.borrow();
        let pools = pools.as_ref().expect(
            "Thread-local pools not initialized. Call init_thread_pools() or set_thread_pools() first."
        );
        f(pools)
    })
}

/// Clear thread-local pools for the current thread
pub fn clear_thread_pools() {
	THREAD_POOLS.with(|p| {
		*p.borrow_mut() = None;
	});
}

/// Check if thread-local pools are initialized
pub fn has_thread_pools() -> bool {
	THREAD_POOLS.with(|p| p.borrow().is_some())
}

/// Execute a closure with temporary thread-local pools
/// The previous pools state is restored after the closure completes
pub fn with_temporary_pools<F, R>(pools: Pools, f: F) -> R
where
	F: FnOnce() -> R,
{
	let previous = get_thread_pools();
	set_thread_pools(pools);

	// Use defer pattern to ensure cleanup even on panic
	struct Cleanup(Option<Pools>);
	impl Drop for Cleanup {
		fn drop(&mut self) {
			match self.0.take() {
				Some(p) => set_thread_pools(p),
				None => clear_thread_pools(),
			}
		}
	}
	let _cleanup = Cleanup(previous);

	f()
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::columnar::pool::allocator::PoolAllocator;

	#[test]
	fn test_thread_local_basic() {
		// Initially not set
		assert!(!has_thread_pools());
		assert!(get_thread_pools().is_none());

		// Set pools
		let pools = Pools::default();
		set_thread_pools(pools.clone());

		// Now should be available
		assert!(has_thread_pools());
		assert!(get_thread_pools().is_some());

		// Can access via thread_pools()
		let retrieved = thread_pools();
		// Can't compare Pools directly, but we got one

		// Clear pools
		clear_thread_pools();
		assert!(!has_thread_pools());
	}

	#[test]
	fn test_with_thread_pools() {
		let pools = Pools::default();
		set_thread_pools(pools);

		let result = with_thread_pools(|p| {
			// Access pools inside closure
			p.bool_pool().stats().available
		});

		// Should get some stats
		assert_eq!(result, 0); // Empty pool initially

		clear_thread_pools();
	}

	#[test]
	fn test_temporary_pools() {
		// Set initial pools
		let pools1 = Pools::new(16);
		set_thread_pools(pools1.clone());
		assert!(has_thread_pools());

		// Use temporary pools
		let pools2 = Pools::new(32);
		let result = with_temporary_pools(pools2, || {
			// Should have temporary pools here
			thread_pools().bool_pool().stats().available
		});

		// Original pools should be restored
		assert!(has_thread_pools());
		// Can't easily verify it's the same pools instance, but it's
		// restored

		clear_thread_pools();
	}

	#[test]
	fn test_temporary_pools_with_none() {
		// Start with no pools
		assert!(!has_thread_pools());

		// Use temporary pools
		let pools = Pools::default();
		with_temporary_pools(pools, || {
			assert!(has_thread_pools());
		});

		// Should be back to no pools
		assert!(!has_thread_pools());
	}

	#[test]
	#[should_panic(expected = "Thread-local pools not initialized")]
	fn test_thread_pools_panics_when_not_set() {
		clear_thread_pools();
		thread_pools(); // Should panic
	}
}
