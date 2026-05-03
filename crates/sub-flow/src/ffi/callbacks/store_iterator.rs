// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{cell::RefCell, collections::HashMap};

use reifydb_core::interface::store::MultiVersionBatch;

pub type StoreIteratorHandle = u64;

thread_local! {
	static ITERATOR_REGISTRY: RefCell<StoreIteratorRegistry> = RefCell::new(StoreIteratorRegistry::new());
}

struct BatchIterator {
	items: Vec<(Vec<u8>, Vec<u8>)>,
	position: usize,
}

impl BatchIterator {
	fn new(batch: MultiVersionBatch) -> Self {
		let items = batch.items.into_iter().map(|multi| (multi.key.to_vec(), multi.row.to_vec())).collect();

		Self {
			items,
			position: 0,
		}
	}

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

pub(crate) fn create_iterator(batch: MultiVersionBatch) -> StoreIteratorHandle {
	let iter = BatchIterator::new(batch);
	ITERATOR_REGISTRY.with(|r| r.borrow_mut().insert(iter))
}

pub(crate) fn next_iterator(handle: StoreIteratorHandle) -> Option<(Vec<u8>, Vec<u8>)> {
	ITERATOR_REGISTRY.with(|r| {
		let mut registry = r.borrow_mut();
		registry.get_mut(handle)?.next()
	})
}

pub(crate) fn free_iterator(handle: StoreIteratorHandle) -> bool {
	ITERATOR_REGISTRY.with(|r| r.borrow_mut().remove(handle).is_some())
}
