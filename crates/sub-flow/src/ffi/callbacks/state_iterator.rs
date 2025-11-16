//! State iterator management for FFI operators
//!
//! Manages iterators across the FFI boundary using a handle-based approach.

use std::{collections::HashMap, sync::Mutex};

use reifydb_core::{
	interface::BoxedMultiVersionIter,
	key::{EncodableKey, FlowNodeStateKey},
};

/// Handle to a state iterator
pub type StateIteratorHandle = u64;

/// Global registry of active state iterators
static ITERATOR_REGISTRY: Mutex<Option<IteratorRegistry>> = Mutex::new(None);

/// Registry for managing state iterators
struct IteratorRegistry {
	next_handle: StateIteratorHandle,
	iterators: HashMap<StateIteratorHandle, BoxedMultiVersionIter<'static>>,
}

impl IteratorRegistry {
	fn new() -> Self {
		Self {
			next_handle: 1,
			iterators: HashMap::new(),
		}
	}

	fn insert(&mut self, iter: BoxedMultiVersionIter<'static>) -> StateIteratorHandle {
		let handle = self.next_handle;
		self.next_handle = self.next_handle.wrapping_add(1);
		self.iterators.insert(handle, iter);
		handle
	}

	fn get_mut(&mut self, handle: StateIteratorHandle) -> Option<&mut BoxedMultiVersionIter<'static>> {
		self.iterators.get_mut(&handle)
	}

	fn remove(&mut self, handle: StateIteratorHandle) -> Option<BoxedMultiVersionIter<'static>> {
		self.iterators.remove(&handle)
	}
}

/// Initialize the iterator registry
fn get_registry() -> &'static Mutex<Option<IteratorRegistry>> {
	&ITERATOR_REGISTRY
}

/// Create a new iterator and return its handle
pub(crate) fn create_iterator(iter: BoxedMultiVersionIter<'static>) -> StateIteratorHandle {
	let mut guard = get_registry().lock().unwrap();
	if guard.is_none() {
		*guard = Some(IteratorRegistry::new());
	}
	guard.as_mut().unwrap().insert(iter)
}

/// Get the next key-value pair from an iterator
///
/// Returns:
/// - Some((user_key, value)) if there's a next item
/// - None if iterator is exhausted or handle is invalid
pub(crate) fn next_iterator(handle: StateIteratorHandle) -> Option<(Vec<u8>, Vec<u8>)> {
	let mut guard = get_registry().lock().unwrap();
	let registry = guard.as_mut()?;
	let iter = registry.get_mut(handle)?;

	// Get next item from iterator
	iter.next().and_then(|multi| {
		// Decode the FlowNodeStateKey to extract the user key
		let state_key = FlowNodeStateKey::decode(&multi.key)?;
		Some((state_key.key, multi.values.as_ref().to_vec()))
	})
}

/// Free an iterator by its handle
pub(crate) fn free_iterator(handle: StateIteratorHandle) -> bool {
	let mut guard = get_registry().lock().unwrap();
	if let Some(registry) = guard.as_mut() {
		registry.remove(handle).is_some()
	} else {
		false
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		CommitVersion, CowVec, EncodedKey,
		interface::{FlowNodeId, MultiVersionValues},
		value::encoded::EncodedValues,
	};

	use super::*;

	fn make_state_key(node_id: u64, key: &[u8]) -> EncodedKey {
		FlowNodeStateKey::new(FlowNodeId(node_id), key.to_vec()).encode()
	}

	fn make_value(data: &[u8]) -> EncodedValues {
		EncodedValues(CowVec::new(data.to_vec()))
	}

	#[test]
	fn test_create_and_free_iterator() {
		let items = vec![MultiVersionValues {
			key: make_state_key(1, b"key1"),
			values: make_value(b"value1"),
			version: CommitVersion(1),
		}];

		let iter: BoxedMultiVersionIter<'static> =
			Box::new(items.into_iter()) as BoxedMultiVersionIter<'static>;

		let handle = create_iterator(iter);
		assert!(handle > 0);

		let freed = free_iterator(handle);
		assert!(freed);

		// Freeing again should return false
		let freed_again = free_iterator(handle);
		assert!(!freed_again);
	}

	#[test]
	fn test_iterator_next() {
		let items = vec![
			MultiVersionValues {
				key: make_state_key(1, b"key1"),
				values: make_value(b"value1"),
				version: CommitVersion(1),
			},
			MultiVersionValues {
				key: make_state_key(1, b"key2"),
				values: make_value(b"value2"),
				version: CommitVersion(1),
			},
		];

		let iter: BoxedMultiVersionIter<'static> =
			Box::new(items.into_iter()) as BoxedMultiVersionIter<'static>;

		let handle = create_iterator(iter);

		// Read first item
		let (key1, val1) = next_iterator(handle).unwrap();
		assert_eq!(key1, b"key1");
		assert_eq!(val1, b"value1");

		// Read second item
		let (key2, val2) = next_iterator(handle).unwrap();
		assert_eq!(key2, b"key2");
		assert_eq!(val2, b"value2");

		// Iterator exhausted
		assert!(next_iterator(handle).is_none());

		free_iterator(handle);
	}

	#[test]
	fn test_iterator_invalid_handle() {
		let result = next_iterator(999999);
		assert!(result.is_none());

		let freed = free_iterator(999999);
		assert!(!freed);
	}

	#[test]
	fn test_multiple_iterators() {
		let items1 = vec![MultiVersionValues {
			key: make_state_key(1, b"iter1"),
			values: make_value(b"value1"),
			version: CommitVersion(1),
		}];

		let items2 = vec![MultiVersionValues {
			key: make_state_key(2, b"iter2"),
			values: make_value(b"value2"),
			version: CommitVersion(1),
		}];

		let handle1 = create_iterator(Box::new(items1.into_iter()) as BoxedMultiVersionIter<'static>);
		let handle2 = create_iterator(Box::new(items2.into_iter()) as BoxedMultiVersionIter<'static>);

		assert_ne!(handle1, handle2);

		let (key1, _) = next_iterator(handle1).unwrap();
		let (key2, _) = next_iterator(handle2).unwrap();

		assert_eq!(key1, b"iter1");
		assert_eq!(key2, b"iter2");

		free_iterator(handle1);
		free_iterator(handle2);
	}
}
