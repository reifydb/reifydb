// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::sync::Arc;

use reifydb_runtime::sync::mutex::Mutex;

pub struct Slab<T> {
	pool: Mutex<Vec<Arc<T>>>,
	cap: usize,
}

impl<T: Default> Slab<T> {
	pub fn new(cap: usize) -> Self {
		Self {
			pool: Mutex::new(Vec::new()),
			cap,
		}
	}

	pub fn acquire(&self) -> Arc<T> {
		let mut pool = self.pool.lock();
		while let Some(slab) = pool.pop() {
			if Arc::strong_count(&slab) == 1 {
				return slab;
			}
		}
		drop(pool);
		Arc::new(T::default())
	}

	pub fn release(&self, slab: Arc<T>) {
		let mut pool = self.pool.lock();
		if pool.len() < self.cap {
			pool.push(slab);
		}
	}
}

impl<T> Slab<T> {
	pub fn len(&self) -> usize {
		self.pool.lock().len()
	}

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
