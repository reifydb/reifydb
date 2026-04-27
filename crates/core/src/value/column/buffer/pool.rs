// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

//! Per-type reuse pool for `ColumnBuffer`.
//!
//! Buckets buffers by their concrete `Type` discriminant. `acquire(target,
//! min_capacity)` returns the smallest pooled buffer whose capacity is at
//! least `min_capacity` (best-fit), falling back to `ColumnBuffer::with_capacity`
//! when the bucket is empty or holds nothing big enough. `release(buf)` clears
//! the buffer (preserving capacity) and indexes it by its `get_type()`.
//!
//! Polymorphic and variable-shape types (`Option(_)`, `Any`, `List(_)`,
//! `Record(_)`, `Tuple(_)`) are not pooled - `acquire` for them always
//! allocates fresh and `release` drops them. The wrapper allocations for
//! these types are rare in CDC hot paths and would either bloat the pool
//! with seldom-reused shapes or churn with cache misses.

use std::collections::HashMap;

use reifydb_runtime::sync::mutex::Mutex;
use reifydb_type::value::r#type::Type;

use crate::value::column::buffer::ColumnBuffer;

/// Fixed per-type retention cap. Not configurable for now; bounds the
/// worst-case pool size at 26 (concrete types) * 64 = 1664 buffers.
const CAP_PER_TYPE: usize = 64;

pub struct ColumnBufferPool {
	inner: Mutex<HashMap<Type, Vec<ColumnBuffer>>>,
}

impl Default for ColumnBufferPool {
	fn default() -> Self {
		Self::new()
	}
}

impl ColumnBufferPool {
	pub fn new() -> Self {
		Self {
			inner: Mutex::new(HashMap::new()),
		}
	}

	/// Pop a buffer for `target` whose `capacity() >= min_capacity`, preferring
	/// the smallest qualifying buffer (best-fit). Falls back to allocating
	/// fresh via `ColumnBuffer::with_capacity` if `target` is not poolable
	/// or no qualifying buffer is in the bucket.
	pub fn acquire(&self, target: &Type, min_capacity: usize) -> ColumnBuffer {
		if is_poolable(target) {
			let mut pool = self.inner.lock();
			if let Some(bucket) = pool.get_mut(target) {
				let mut best_idx: Option<usize> = None;
				let mut best_cap: usize = usize::MAX;
				for (i, buf) in bucket.iter().enumerate() {
					let cap = buf.capacity();
					if cap >= min_capacity && cap < best_cap {
						best_cap = cap;
						best_idx = Some(i);
					}
				}
				if let Some(i) = best_idx {
					return bucket.swap_remove(i);
				}
			}
		}
		ColumnBuffer::with_capacity(target.clone(), min_capacity)
	}

	/// Clear `buffer` and return it to the pool indexed by its type. Drops
	/// the buffer if its type is not poolable, or if the bucket is at
	/// `CAP_PER_TYPE`.
	pub fn release(&self, mut buffer: ColumnBuffer) {
		let buffer_type = buffer.get_type();
		if !is_poolable(&buffer_type) {
			return;
		}
		buffer.clear();
		let mut pool = self.inner.lock();
		let bucket = pool.entry(buffer_type).or_default();
		if bucket.len() < CAP_PER_TYPE {
			bucket.push(buffer);
		}
	}

	/// Total number of buffers currently held across all type buckets.
	/// Primarily for tests.
	pub fn len(&self) -> usize {
		self.inner.lock().values().map(|v| v.len()).sum()
	}

	/// `true` if every bucket is empty. Primarily for tests.
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

/// True for the 26 concrete `Type` variants the pool buckets. Polymorphic
/// or variable-shape types (`Option(_)`, `Any`, `List(_)`, `Record(_)`,
/// `Tuple(_)`) bypass the pool - `acquire` allocates fresh, `release` drops.
fn is_poolable(t: &Type) -> bool {
	matches!(
		t,
		Type::Boolean
			| Type::Float4 | Type::Float8
			| Type::Int1 | Type::Int2
			| Type::Int4 | Type::Int8
			| Type::Int16 | Type::Uint1
			| Type::Uint2 | Type::Uint4
			| Type::Uint8 | Type::Uint16
			| Type::Utf8 | Type::Date
			| Type::DateTime | Type::Time
			| Type::Duration | Type::IdentityId
			| Type::Uuid4 | Type::Uuid7
			| Type::Blob | Type::Int
			| Type::Uint | Type::Decimal
			| Type::DictionaryId
	)
}

#[cfg(test)]
mod tests {
	use reifydb_type::value::r#type::Type;

	use super::{ColumnBufferPool, is_poolable};
	use crate::value::column::buffer::ColumnBuffer;

	#[test]
	fn acquire_from_empty_pool_allocates_fresh() {
		let pool = ColumnBufferPool::new();
		let buf = pool.acquire(&Type::Int8, 4);
		assert_eq!(buf.get_type(), Type::Int8);
		assert!(buf.capacity() >= 4);
		assert!(pool.is_empty());
	}

	#[test]
	fn release_then_acquire_reuses_same_allocation() {
		let pool = ColumnBufferPool::new();
		let mut buf = ColumnBuffer::with_capacity(Type::Int8, 16);
		// Push then clear to mark the buffer non-empty before release
		// (so the capacity assertion below checks reuse, not freshness).
		for i in 0..8i64 {
			buf.push_value(reifydb_type::value::Value::Int8(i));
		}
		let original_capacity = buf.capacity();
		pool.release(buf);
		assert_eq!(pool.len(), 1);

		let reused = pool.acquire(&Type::Int8, 1);
		assert_eq!(reused.get_type(), Type::Int8);
		assert_eq!(reused.capacity(), original_capacity);
		assert_eq!(reused.len(), 0);
		assert!(pool.is_empty());
	}

	#[test]
	fn best_fit_prefers_smallest_qualifying_buffer() {
		let pool = ColumnBufferPool::new();
		pool.release(ColumnBuffer::with_capacity(Type::Int8, 4));
		pool.release(ColumnBuffer::with_capacity(Type::Int8, 32));
		pool.release(ColumnBuffer::with_capacity(Type::Int8, 16));
		pool.release(ColumnBuffer::with_capacity(Type::Int8, 64));
		assert_eq!(pool.len(), 4);

		// Need >= 10. The smallest qualifying buffer in the pool
		// is the 16-capacity one. Aligned-capacity quirks may round
		// up a bit but it should pick the buffer closest to 10.
		let pick = pool.acquire(&Type::Int8, 10);
		assert!(pick.capacity() >= 10);
		assert!(pick.capacity() < 32);
		assert_eq!(pool.len(), 3);
	}

	#[test]
	fn release_at_cap_drops_overflow() {
		let pool = ColumnBufferPool::new();
		// CAP_PER_TYPE is 64; release 65 to overflow by one.
		for _ in 0..65 {
			pool.release(ColumnBuffer::with_capacity(Type::Int8, 1));
		}
		assert_eq!(pool.len(), 64);
	}

	#[test]
	fn buffers_do_not_cross_pollute_across_types() {
		let pool = ColumnBufferPool::new();
		pool.release(ColumnBuffer::with_capacity(Type::Int8, 16));
		pool.release(ColumnBuffer::with_capacity(Type::Utf8, 16));
		assert_eq!(pool.len(), 2);

		let int8 = pool.acquire(&Type::Int8, 1);
		assert_eq!(int8.get_type(), Type::Int8);
		assert_eq!(pool.len(), 1);

		let utf8 = pool.acquire(&Type::Utf8, 1);
		assert_eq!(utf8.get_type(), Type::Utf8);
		assert!(pool.is_empty());
	}

	#[test]
	fn non_poolable_types_bypass_pool() {
		let pool = ColumnBufferPool::new();
		// Option(Int8) is not poolable.
		let opt_ty = Type::Option(Box::new(Type::Int8));
		let opt_buf = ColumnBuffer::with_capacity(opt_ty.clone(), 8);
		pool.release(opt_buf);
		assert!(pool.is_empty(), "Option-wrapped buffers must not enter the pool");

		// Subsequent acquire allocates fresh; the pool stays empty.
		let acquired = pool.acquire(&opt_ty, 4);
		assert!(acquired.capacity() >= 4);
		assert!(pool.is_empty());
	}

	#[test]
	fn is_poolable_matrix() {
		assert!(is_poolable(&Type::Boolean));
		assert!(is_poolable(&Type::Float4));
		assert!(is_poolable(&Type::Float8));
		assert!(is_poolable(&Type::Int1));
		assert!(is_poolable(&Type::Int2));
		assert!(is_poolable(&Type::Int4));
		assert!(is_poolable(&Type::Int8));
		assert!(is_poolable(&Type::Int16));
		assert!(is_poolable(&Type::Uint1));
		assert!(is_poolable(&Type::Uint2));
		assert!(is_poolable(&Type::Uint4));
		assert!(is_poolable(&Type::Uint8));
		assert!(is_poolable(&Type::Uint16));
		assert!(is_poolable(&Type::Utf8));
		assert!(is_poolable(&Type::Date));
		assert!(is_poolable(&Type::DateTime));
		assert!(is_poolable(&Type::Time));
		assert!(is_poolable(&Type::Duration));
		assert!(is_poolable(&Type::IdentityId));
		assert!(is_poolable(&Type::Uuid4));
		assert!(is_poolable(&Type::Uuid7));
		assert!(is_poolable(&Type::Blob));
		assert!(is_poolable(&Type::Int));
		assert!(is_poolable(&Type::Uint));
		assert!(is_poolable(&Type::Decimal));
		assert!(is_poolable(&Type::DictionaryId));

		assert!(!is_poolable(&Type::Option(Box::new(Type::Int8))));
		assert!(!is_poolable(&Type::Any));
		assert!(!is_poolable(&Type::List(Box::new(Type::Int8))));
		assert!(!is_poolable(&Type::Record(vec![])));
		assert!(!is_poolable(&Type::Tuple(vec![])));
	}
}
