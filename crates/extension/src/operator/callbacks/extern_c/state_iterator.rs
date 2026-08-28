// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{cell::RefCell, collections::HashMap};

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::key::operator_state::GroupStateKey;

pub type StateIteratorHandle = u64;
type StateIteratorBatch = (*const (Vec<u8>, Vec<u8>), usize);

thread_local! {
	static ITERATOR_REGISTRY: RefCell<IteratorRegistry> = RefCell::new(IteratorRegistry::new());
}

struct BatchIterator {
	items: Vec<(Vec<u8>, Vec<u8>)>,
	position: usize,
}

impl BatchIterator {
	fn new(entries: Vec<(GroupStateKey, EncodedBytes)>) -> Self {
		let items = entries.into_iter().map(|(key, bytes)| (key.as_slice().to_vec(), bytes.to_vec())).collect();

		Self {
			items,
			position: 0,
		}
	}
}

struct IteratorRegistry {
	next_handle: StateIteratorHandle,
	iterators: HashMap<StateIteratorHandle, BatchIterator>,
}

impl IteratorRegistry {
	fn new() -> Self {
		Self {
			next_handle: 1,
			iterators: HashMap::new(),
		}
	}

	fn insert(&mut self, iter: BatchIterator) -> StateIteratorHandle {
		let handle = self.next_handle;
		self.next_handle = self.next_handle.wrapping_add(1);
		self.iterators.insert(handle, iter);
		handle
	}

	fn get_mut(&mut self, handle: StateIteratorHandle) -> Option<&mut BatchIterator> {
		self.iterators.get_mut(&handle)
	}

	fn remove(&mut self, handle: StateIteratorHandle) -> Option<BatchIterator> {
		self.iterators.remove(&handle)
	}
}

pub(crate) fn create_iterator(entries: Vec<(GroupStateKey, EncodedBytes)>) -> StateIteratorHandle {
	let iter = BatchIterator::new(entries);
	ITERATOR_REGISTRY.with(|r| r.borrow_mut().insert(iter))
}

pub(crate) fn next_iterator_batch(handle: StateIteratorHandle, cap: usize) -> Option<StateIteratorBatch> {
	ITERATOR_REGISTRY.with(|r| {
		let mut registry = r.borrow_mut();
		let iter = registry.get_mut(handle)?;
		let remaining = iter.items.len().saturating_sub(iter.position);
		let take = remaining.min(cap);
		let start = iter.items[iter.position..].as_ptr();
		iter.position += take;
		Some((start, take))
	})
}

pub(crate) fn free_iterator(handle: StateIteratorHandle) -> bool {
	ITERATOR_REGISTRY.with(|r| r.borrow_mut().remove(handle).is_some())
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::key::operator_state::{GroupId, KeyspaceId, OperatorStateKey};
	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	fn make_state_key(_operator_id: u64, key: &[u8]) -> GroupStateKey {
		OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::CUSTOM_NOT_CACHED, key.to_vec())
	}

	fn decoded_suffix(framed: &[u8]) -> Vec<u8> {
		OperatorStateKey::decode_inner(framed).expect("iterator must hand back a framed group state key").2
	}

	fn make_value(data: &[u8]) -> EncodedBytes {
		EncodedBytes(CowVec::new(data.to_vec()))
	}

	#[test]
	fn test_create_and_free_iterator() {
		let items = vec![(make_state_key(1, b"key1"), make_value(b"value1"))];

		let handle = create_iterator(items);
		assert!(handle > 0);

		let freed = free_iterator(handle);
		assert!(freed);

		let freed_again = free_iterator(handle);
		assert!(!freed_again);
	}

	fn next_one(handle: StateIteratorHandle) -> Option<(Vec<u8>, Vec<u8>)> {
		// Pulls one entry and copies it out before any further registry call can invalidate it.
		let (ptr, len) = next_iterator_batch(handle, 1)?;
		if len == 0 {
			return None;
		}
		// SAFETY: len > 0 means the registry entry `ptr` points at is still alive and no
		// intervening registry call has been made, so the reference is valid for this read.
		let (key, value) = unsafe { &*ptr };
		Some((key.clone(), value.clone()))
	}

	#[test]
	fn test_iterator_next() {
		let items = vec![
			(make_state_key(1, b"key1"), make_value(b"value1")),
			(make_state_key(1, b"key2"), make_value(b"value2")),
		];

		let handle = create_iterator(items);

		let (key1, val1) = next_one(handle).unwrap();
		assert_eq!(decoded_suffix(&key1), b"key1");
		assert_eq!(val1, b"value1");

		let (key2, val2) = next_one(handle).unwrap();
		assert_eq!(decoded_suffix(&key2), b"key2");
		assert_eq!(val2, b"value2");

		assert!(next_one(handle).is_none());

		free_iterator(handle);
	}

	#[test]
	fn test_iterator_batch_respects_cap_then_exhausts() {
		let items = (0u8..5).map(|n| (make_state_key(1, &[b'k', n]), make_value(&[b'v', n]))).collect();
		let handle = create_iterator(items);

		let (_, first) = next_iterator_batch(handle, 3).unwrap();
		assert_eq!(first, 3, "a batch call must fill at most cap entries");
		let (_, second) = next_iterator_batch(handle, 3).unwrap();
		assert_eq!(second, 2, "the final partial batch must return the remainder");
		let (_, third) = next_iterator_batch(handle, 3).unwrap();
		assert_eq!(third, 0, "an exhausted iterator must report an empty batch");

		free_iterator(handle);
	}

	#[test]
	fn test_iterator_invalid_handle() {
		let result = next_iterator_batch(999999, 1);
		assert!(result.is_none());

		let freed = free_iterator(999999);
		assert!(!freed);
	}

	#[test]
	fn test_multiple_iterators() {
		let items1 = vec![(make_state_key(1, b"iter1"), make_value(b"value1"))];

		let items2 = vec![(make_state_key(2, b"iter2"), make_value(b"value2"))];

		let handle1 = create_iterator(items1);
		let handle2 = create_iterator(items2);

		assert_ne!(handle1, handle2);

		let (key1, _) = next_one(handle1).unwrap();
		let (key2, _) = next_one(handle2).unwrap();

		assert_eq!(decoded_suffix(&key1), b"iter1");
		assert_eq!(decoded_suffix(&key2), b"iter2");

		free_iterator(handle1);
		free_iterator(handle2);
	}
}
