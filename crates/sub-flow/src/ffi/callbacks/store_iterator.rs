//! Store iterator management for FFI operators
//!
//! Manages store iterators across the FFI boundary using a handle-based approach.
//! Unlike state iterators, store iterators return raw keys without any namespace decoding.

use std::{collections::HashMap, sync::Mutex};

use reifydb_core::interface::BoxedMultiVersionIter;

/// Handle to a store iterator
pub type StoreIteratorHandle = u64;

/// Global registry of active store iterators
static ITERATOR_REGISTRY: Mutex<Option<StoreIteratorRegistry>> = Mutex::new(None);

/// Registry for managing store iterators
struct StoreIteratorRegistry {
	next_handle: StoreIteratorHandle,
	iterators: HashMap<StoreIteratorHandle, BoxedMultiVersionIter<'static>>,
}

impl StoreIteratorRegistry {
	fn new() -> Self {
		Self {
			next_handle: 1,
			iterators: HashMap::new(),
		}
	}

	fn insert(&mut self, iter: BoxedMultiVersionIter<'static>) -> StoreIteratorHandle {
		let handle = self.next_handle;
		self.next_handle = self.next_handle.wrapping_add(1);
		self.iterators.insert(handle, iter);
		handle
	}

	fn get_mut(&mut self, handle: StoreIteratorHandle) -> Option<&mut BoxedMultiVersionIter<'static>> {
		self.iterators.get_mut(&handle)
	}

	fn remove(&mut self, handle: StoreIteratorHandle) -> Option<BoxedMultiVersionIter<'static>> {
		self.iterators.remove(&handle)
	}
}

/// Initialize the iterator registry
fn get_registry() -> &'static Mutex<Option<StoreIteratorRegistry>> {
	&ITERATOR_REGISTRY
}

/// Create a new iterator and return its handle
pub(crate) fn create_iterator(iter: BoxedMultiVersionIter<'static>) -> StoreIteratorHandle {
	let mut guard = get_registry().lock().unwrap();
	if guard.is_none() {
		*guard = Some(StoreIteratorRegistry::new());
	}
	guard.as_mut().unwrap().insert(iter)
}

/// Get the next key-value pair from an iterator
///
/// Returns:
/// - Some((key, value)) if there's a next item
/// - None if iterator is exhausted or handle is invalid
pub(crate) fn next_iterator(handle: StoreIteratorHandle) -> Option<(Vec<u8>, Vec<u8>)> {
	let mut guard = get_registry().lock().unwrap();
	let registry = guard.as_mut()?;
	let iter = registry.get_mut(handle)?;

	// Get next item from iterator - return raw key without decoding
	iter.next().map(|multi| (multi.key.as_ref().to_vec(), multi.values.as_ref().to_vec()))
}

/// Free an iterator by its handle
pub(crate) fn free_iterator(handle: StoreIteratorHandle) -> bool {
	let mut guard = get_registry().lock().unwrap();
	if let Some(registry) = guard.as_mut() {
		registry.remove(handle).is_some()
	} else {
		false
	}
}
