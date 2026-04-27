// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Reuse pool of `Arc<T>` slabs.
//!
//! A `Slab<T>` lets a producer hand out `Arc<T>` instances, send them
//! through downstream consumers (which clone the `Arc` to share state),
//! and reclaim the original allocation once consumers drop their clones.
//! On the next acquire, the producer pulls a slab whose `strong_count`
//! has returned to 1 and overwrites it in place via `Arc::make_mut`,
//! preserving the inner allocation's capacity instead of reallocating.
//!
//! Designed for hot paths that build many short-lived `Arc<T>` values of
//! similar shape (for example the CDC producer's per-row column buffers).
//!
//! Slabs are bounded by a configurable cap so a transient burst does not
//! retain unlimited capacity. Slabs that would push the pool past its cap
//! are dropped and reclaimed by the allocator instead.

use std::sync::Arc;

use reifydb_runtime::sync::mutex::Mutex;

/// Reusable pool of `Arc<T>` slabs.
///
/// `T` must be `Default` so the pool can mint fresh empty slabs when its
/// reserve is exhausted.
pub struct Slab<T> {
	pool: Mutex<Vec<Arc<T>>>,
	cap: usize,
}

impl<T: Default> Slab<T> {
	/// Create a new `Slab` that retains at most `cap` reusable slabs.
	pub fn new(cap: usize) -> Self {
		Self {
			pool: Mutex::new(Vec::new()),
			cap,
		}
	}

	/// Pull a slab from the pool, or allocate a fresh one if the pool
	/// is empty (or every retained slab is still referenced elsewhere).
	///
	/// Returned slabs always have `strong_count == 1`, so the caller
	/// can mutate them in place via `Arc::make_mut` without the COW
	/// fork penalty.
	pub fn acquire(&self) -> Arc<T> {
		let mut pool = self.pool.lock();
		while let Some(slab) = pool.pop() {
			if Arc::strong_count(&slab) == 1 {
				return slab;
			}
			// Slab is still referenced (e.g. an in-flight consumer
			// has not dropped its clone yet). Drop our local
			// reference and try the next pool entry; eventually we
			// either find a unique one or fall through to allocating
			// fresh.
		}
		drop(pool);
		Arc::new(T::default())
	}

	/// Return a slab to the pool. If the pool is at its cap the slab is
	/// dropped instead so memory pressure does not grow without bound.
	pub fn release(&self, slab: Arc<T>) {
		let mut pool = self.pool.lock();
		if pool.len() < self.cap {
			pool.push(slab);
		}
	}
}

impl<T> Slab<T> {
	/// Number of slabs currently held in the pool. Primarily for tests.
	pub fn len(&self) -> usize {
		self.pool.lock().len()
	}

	/// `true` if the pool is empty. Primarily for tests.
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

#[cfg(test)]
mod tests {
	use std::sync::Arc;

	use super::Slab;

	#[derive(Clone, Default, Debug)]
	struct Buf {
		bytes: Vec<u8>,
	}

	#[test]
	fn acquire_from_empty_pool_allocates_fresh() {
		let slab: Slab<Buf> = Slab::new(8);
		let a = slab.acquire();
		assert_eq!(Arc::strong_count(&a), 1);
		assert_eq!(slab.len(), 0);
	}

	#[test]
	fn release_then_acquire_returns_same_allocation() {
		let slab: Slab<Buf> = Slab::new(8);
		let mut a = slab.acquire();
		Arc::make_mut(&mut a).bytes.extend_from_slice(b"hello");
		let ptr_before = a.bytes.as_ptr();
		slab.release(a);
		assert_eq!(slab.len(), 1);

		let b = slab.acquire();
		// Same allocation reused (capacity preserved). The pool keeps
		// the data as-is - callers are expected to overwrite via
		// Arc::make_mut on the next use.
		assert_eq!(b.bytes.as_ptr(), ptr_before);
		assert_eq!(slab.len(), 0);
	}

	#[test]
	fn shared_slabs_in_pool_are_skipped_on_acquire() {
		let slab: Slab<Buf> = Slab::new(8);
		let a = slab.acquire();
		let _shadow = a.clone(); // bumps strong_count to 2
		slab.release(a);
		assert_eq!(slab.len(), 1);

		// acquire skips the shared slab and allocates fresh.
		let b = slab.acquire();
		assert_eq!(Arc::strong_count(&b), 1);
		assert!(slab.is_empty());
	}

	#[test]
	fn release_at_cap_drops_overflow() {
		let slab: Slab<Buf> = Slab::new(2);
		let a = slab.acquire();
		let b = slab.acquire();
		let c = slab.acquire();
		slab.release(a);
		slab.release(b);
		assert_eq!(slab.len(), 2);
		// Third release exceeds cap; slab is dropped.
		slab.release(c);
		assert_eq!(slab.len(), 2);
	}

	#[test]
	fn acquire_handles_pool_with_only_shared_slabs() {
		let slab: Slab<Buf> = Slab::new(8);
		let a = slab.acquire();
		let b = slab.acquire();
		let _shadow_a = a.clone();
		let _shadow_b = b.clone();
		slab.release(a);
		slab.release(b);
		assert_eq!(slab.len(), 2);

		let c = slab.acquire();
		assert_eq!(Arc::strong_count(&c), 1);
		// Pool drained as the iterator skipped shared entries.
		assert!(slab.is_empty());
	}
}
