// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_runtime::sync::mutex::Mutex;
use reifydb_value::value::value_type::ValueType;

use crate::value::column::buffer::ColumnBuffer;

const CAP_PER_TYPE: usize = 64;

pub struct ColumnBufferPool {
	inner: Mutex<HashMap<ValueType, Vec<ColumnBuffer>>>,
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

	pub fn acquire(&self, target: &ValueType, min_capacity: usize) -> ColumnBuffer {
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

	pub fn len(&self) -> usize {
		self.inner.lock().values().map(|v| v.len()).sum()
	}

	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

fn is_poolable(t: &ValueType) -> bool {
	matches!(
		t,
		ValueType::Boolean
			| ValueType::Float4 | ValueType::Float8
			| ValueType::Int1 | ValueType::Int2
			| ValueType::Int4 | ValueType::Int8
			| ValueType::Int16 | ValueType::Uint1
			| ValueType::Uint2 | ValueType::Uint4
			| ValueType::Uint8 | ValueType::Uint16
			| ValueType::Utf8 | ValueType::Date
			| ValueType::DateTime | ValueType::Time
			| ValueType::Duration | ValueType::IdentityId
			| ValueType::Uuid4 | ValueType::Uuid7
			| ValueType::Blob | ValueType::Int
			| ValueType::Uint | ValueType::Decimal
			| ValueType::DictionaryId
	)
}

#[cfg(test)]
mod tests {
	use reifydb_value::value::{Value, value_type::ValueType};

	use super::{ColumnBufferPool, is_poolable};
	use crate::value::column::buffer::ColumnBuffer;

	#[test]
	fn acquire_from_empty_pool_allocates_fresh() {
		let pool = ColumnBufferPool::new();
		let buf = pool.acquire(&ValueType::Int8, 4);
		assert_eq!(buf.get_type(), ValueType::Int8);
		assert!(buf.capacity() >= 4);
		assert!(pool.is_empty());
	}

	#[test]
	fn release_then_acquire_reuses_same_allocation() {
		let pool = ColumnBufferPool::new();
		let mut buf = ColumnBuffer::with_capacity(ValueType::Int8, 16);
		// Push then clear to mark the buffer non-empty before release
		// (so the capacity assertion below checks reuse, not freshness).
		for i in 0..8i64 {
			buf.push_value(Value::Int8(i));
		}
		let original_capacity = buf.capacity();
		pool.release(buf);
		assert_eq!(pool.len(), 1);

		let reused = pool.acquire(&ValueType::Int8, 1);
		assert_eq!(reused.get_type(), ValueType::Int8);
		assert_eq!(reused.capacity(), original_capacity);
		assert_eq!(reused.len(), 0);
		assert!(pool.is_empty());
	}

	#[test]
	fn best_fit_prefers_smallest_qualifying_buffer() {
		let pool = ColumnBufferPool::new();
		pool.release(ColumnBuffer::with_capacity(ValueType::Int8, 4));
		pool.release(ColumnBuffer::with_capacity(ValueType::Int8, 32));
		pool.release(ColumnBuffer::with_capacity(ValueType::Int8, 16));
		pool.release(ColumnBuffer::with_capacity(ValueType::Int8, 64));
		assert_eq!(pool.len(), 4);

		// Need >= 10. The smallest qualifying buffer in the pool
		// is the 16-capacity one. Aligned-capacity quirks may round
		// up a bit but it should pick the buffer closest to 10.
		let pick = pool.acquire(&ValueType::Int8, 10);
		assert!(pick.capacity() >= 10);
		assert!(pick.capacity() < 32);
		assert_eq!(pool.len(), 3);
	}

	#[test]
	fn release_at_cap_drops_overflow() {
		let pool = ColumnBufferPool::new();
		// CAP_PER_TYPE is 64; release 65 to overflow by one.
		for _ in 0..65 {
			pool.release(ColumnBuffer::with_capacity(ValueType::Int8, 1));
		}
		assert_eq!(pool.len(), 64);
	}

	#[test]
	fn buffers_do_not_cross_pollute_across_types() {
		let pool = ColumnBufferPool::new();
		pool.release(ColumnBuffer::with_capacity(ValueType::Int8, 16));
		pool.release(ColumnBuffer::with_capacity(ValueType::Utf8, 16));
		assert_eq!(pool.len(), 2);

		let int8 = pool.acquire(&ValueType::Int8, 1);
		assert_eq!(int8.get_type(), ValueType::Int8);
		assert_eq!(pool.len(), 1);

		let utf8 = pool.acquire(&ValueType::Utf8, 1);
		assert_eq!(utf8.get_type(), ValueType::Utf8);
		assert!(pool.is_empty());
	}

	#[test]
	fn non_poolable_types_bypass_pool() {
		let pool = ColumnBufferPool::new();
		// Option(Int8) is not poolable.
		let opt_ty = ValueType::Option(Box::new(ValueType::Int8));
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
		assert!(is_poolable(&ValueType::Boolean));
		assert!(is_poolable(&ValueType::Float4));
		assert!(is_poolable(&ValueType::Float8));
		assert!(is_poolable(&ValueType::Int1));
		assert!(is_poolable(&ValueType::Int2));
		assert!(is_poolable(&ValueType::Int4));
		assert!(is_poolable(&ValueType::Int8));
		assert!(is_poolable(&ValueType::Int16));
		assert!(is_poolable(&ValueType::Uint1));
		assert!(is_poolable(&ValueType::Uint2));
		assert!(is_poolable(&ValueType::Uint4));
		assert!(is_poolable(&ValueType::Uint8));
		assert!(is_poolable(&ValueType::Uint16));
		assert!(is_poolable(&ValueType::Utf8));
		assert!(is_poolable(&ValueType::Date));
		assert!(is_poolable(&ValueType::DateTime));
		assert!(is_poolable(&ValueType::Time));
		assert!(is_poolable(&ValueType::Duration));
		assert!(is_poolable(&ValueType::IdentityId));
		assert!(is_poolable(&ValueType::Uuid4));
		assert!(is_poolable(&ValueType::Uuid7));
		assert!(is_poolable(&ValueType::Blob));
		assert!(is_poolable(&ValueType::Int));
		assert!(is_poolable(&ValueType::Uint));
		assert!(is_poolable(&ValueType::Decimal));
		assert!(is_poolable(&ValueType::DictionaryId));

		assert!(!is_poolable(&ValueType::Option(Box::new(ValueType::Int8))));
		assert!(!is_poolable(&ValueType::Any));
		assert!(!is_poolable(&ValueType::List(Box::new(ValueType::Int8))));
		assert!(!is_poolable(&ValueType::Record(vec![])));
		assert!(!is_poolable(&ValueType::Tuple(vec![])));
	}
}
