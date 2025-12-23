//! Store iterator management for FFI operators
//!
//! Manages store iterators across the FFI boundary using a handle-based approach.
//! Unlike state iterators, store iterators return raw keys without any namespace decoding.
//! Each thread maintains its own registry to eliminate contention.

use std::{cell::RefCell, collections::HashMap};

use reifydb_core::interface::MultiVersionBatch;

/// Handle to a store iterator
pub type StoreIteratorHandle = u64;

// Thread-local registry of active store iterators
thread_local! {
	static ITERATOR_REGISTRY: RefCell<StoreIteratorRegistry> = RefCell::new(StoreIteratorRegistry::new());
}

/// Batch-based iterator for FFI boundary
///
/// Pre-decodes all items from a MultiVersionBatch for efficient iteration.
/// Returns raw keys without any namespace decoding.
struct BatchIterator {
	items: Vec<(Vec<u8>, Vec<u8>)>, // (raw_key, value) pairs
	position: usize,
}

impl BatchIterator {
	/// Create a new batch iterator from a MultiVersionBatch
	fn new(batch: MultiVersionBatch) -> Self {
		let items = batch
			.items
			.into_iter()
			.map(|multi| (multi.key.as_ref().to_vec(), multi.values.as_ref().to_vec()))
			.collect();

		Self {
			items,
			position: 0,
		}
	}

	/// Get the next key-value pair
	fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
		if self.position < self.items.len() {
			let item = self.items[self.position].clone();
			self.position += 1;
			Some(item)
		} else {
			None
		}
	}
}

/// Registry for managing store iterators
struct StoreIteratorRegistry {
	next_handle: StoreIteratorHandle,
	iterators: HashMap<StoreIteratorHandle, BatchIterator>,
}

impl StoreIteratorRegistry {
	fn new() -> Self {
		Self {
			next_handle: 1,
			iterators: HashMap::new(),
		}
	}

	fn insert(&mut self, iter: BatchIterator) -> StoreIteratorHandle {
		let handle = self.next_handle;
		self.next_handle = self.next_handle.wrapping_add(1);
		self.iterators.insert(handle, iter);
		handle
	}

	fn get_mut(&mut self, handle: StoreIteratorHandle) -> Option<&mut BatchIterator> {
		self.iterators.get_mut(&handle)
	}

	fn remove(&mut self, handle: StoreIteratorHandle) -> Option<BatchIterator> {
		self.iterators.remove(&handle)
	}
}

/// Create a new iterator from a batch and return its handle
pub(crate) fn create_iterator(batch: MultiVersionBatch) -> StoreIteratorHandle {
	let iter = BatchIterator::new(batch);
	ITERATOR_REGISTRY.with(|r| r.borrow_mut().insert(iter))
}

/// Get the next key-value pair from an iterator
///
/// Returns:
/// - Some((key, value)) if there's a next item
/// - None if iterator is exhausted or handle is invalid
pub(crate) fn next_iterator(handle: StoreIteratorHandle) -> Option<(Vec<u8>, Vec<u8>)> {
	ITERATOR_REGISTRY.with(|r| {
		let mut registry = r.borrow_mut();
		registry.get_mut(handle)?.next()
	})
}

/// Free an iterator by its handle
pub(crate) fn free_iterator(handle: StoreIteratorHandle) -> bool {
	ITERATOR_REGISTRY.with(|r| r.borrow_mut().remove(handle).is_some())
}
