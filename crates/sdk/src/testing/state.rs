// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::encoded::{
	encoded::EncodedValues, key::EncodedKey, layout::EncodedValuesLayout, named::EncodedValuesNamedLayout,
};
use reifydb_type::value::Value;

/// Mock state store for testing operators
#[derive(Debug, Clone, Default)]
pub struct TestStateStore {
	data: HashMap<EncodedKey, EncodedValues>,
}

impl TestStateStore {
	/// Create a new empty mock state store
	pub fn new() -> Self {
		Self {
			data: HashMap::new(),
		}
	}

	/// Get a value from the store
	pub fn get(&self, key: &EncodedKey) -> Option<&EncodedValues> {
		self.data.get(key)
	}

	/// Set a value in the store
	pub fn set(&mut self, key: EncodedKey, value: EncodedValues) {
		self.data.insert(key, value);
	}

	/// Remove a value from the store
	pub fn remove(&mut self, key: &EncodedKey) -> Option<EncodedValues> {
		self.data.remove(key)
	}

	/// Check if a key exists
	pub fn contains(&self, key: &EncodedKey) -> bool {
		self.data.contains_key(key)
	}

	/// Get the number of entries
	pub fn len(&self) -> usize {
		self.data.len()
	}

	/// Check if the store is empty
	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	/// Clear all entries
	pub fn clear(&mut self) {
		self.data.clear();
	}

	/// Get all keys
	pub fn keys(&self) -> Vec<&EncodedKey> {
		self.data.keys().collect()
	}

	/// Get all key-value pairs
	pub fn entries(&self) -> Vec<(&EncodedKey, &EncodedValues)> {
		self.data.iter().map(|(k, v)| (k, v)).collect()
	}

	/// Decode a value using a layout
	pub fn decode_value(&self, key: &EncodedKey, layout: &EncodedValuesLayout) -> Option<Vec<Value>> {
		self.get(key).map(|encoded| super::helpers::get_values(layout, encoded))
	}

	/// Decode a value using a named layout
	pub fn decode_named_value(
		&self,
		key: &EncodedKey,
		layout: &EncodedValuesNamedLayout,
	) -> Option<HashMap<String, Value>> {
		self.get(key).map(|encoded| {
			let values = super::helpers::get_values(layout.layout(), encoded);
			layout.names().iter().map(|n| n.as_str().to_string()).zip(values).collect()
		})
	}

	/// Set a value using a layout
	pub fn set_value(&mut self, key: EncodedKey, values: &[Value], layout: &EncodedValuesLayout) {
		let mut encoded = layout.allocate();
		layout.set_values(&mut encoded, values);
		self.set(key, encoded);
	}

	/// Set a value using a named layout
	pub fn set_named_value(
		&mut self,
		key: EncodedKey,
		values: &HashMap<String, Value>,
		layout: &EncodedValuesNamedLayout,
	) {
		let mut encoded = layout.layout().allocate();

		// Convert HashMap to ordered values based on layout names
		let ordered_values: Vec<Value> = layout
			.names()
			.iter()
			.map(|name| values.get(name.as_str()).cloned().unwrap_or(Value::Undefined))
			.collect();

		layout.layout().set_values(&mut encoded, &ordered_values);
		self.set(key, encoded);
	}

	/// Create a snapshot of the current state
	pub fn snapshot(&self) -> HashMap<EncodedKey, EncodedValues> {
		self.data.clone()
	}

	/// Restore from a snapshot
	pub fn restore(&mut self, snapshot: HashMap<EncodedKey, EncodedValues>) {
		self.data = snapshot;
	}

	/// Assert that a key has a specific value
	pub fn assert_value(&self, key: &EncodedKey, expected: &[Value], layout: &EncodedValuesLayout) {
		let actual = self.decode_value(key, layout).expect(&format!("Key {:?} not found in state", key));
		assert_eq!(actual, expected, "State value mismatch for key {:?}", key);
	}

	/// Assert that a key exists
	pub fn assert_exists(&self, key: &EncodedKey) {
		assert!(self.contains(key), "Expected key {:?} to exist in state", key);
	}

	/// Assert that a key does not exist
	pub fn assert_not_exists(&self, key: &EncodedKey) {
		assert!(!self.contains(key), "Expected key {:?} to not exist in state", key);
	}

	/// Assert the state has a specific number of entries
	pub fn assert_count(&self, expected: usize) {
		assert_eq!(self.len(), expected, "Expected {} entries in state, found {}", expected, self.len());
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::encoded::{
		encoded::EncodedValues, layout::EncodedValuesLayout, named::EncodedValuesNamedLayout,
	};
	use reifydb_type::value::r#type::Type;

	use super::*;
	use crate::testing::helpers::encode_key;

	#[test]
	fn test_state_store_basic_operations() {
		use reifydb_type::util::cowvec::CowVec;
		let mut store = TestStateStore::new();
		let key = encode_key("test_key");
		let value = EncodedValues(CowVec::new(vec![1, 2, 3, 4]));

		assert!(store.is_empty());

		store.set(key.clone(), value.clone());
		assert_eq!(store.get(&key), Some(&value));
		assert!(store.contains(&key));
		assert_eq!(store.len(), 1);

		let removed = store.remove(&key);
		assert_eq!(removed, Some(value));
		assert!(store.is_empty());
	}

	#[test]
	fn test_state_store_with_layout() {
		let mut store = TestStateStore::new();
		let layout = EncodedValuesLayout::testing(&[Type::Int8, Type::Utf8]);
		let key = encode_key("test_key");
		let values = vec![Value::Int8(42i64), Value::Utf8("hello".into())];

		store.set_value(key.clone(), &values, &layout);

		let decoded = store.decode_value(&key, &layout).unwrap();
		assert_eq!(decoded, values);
	}

	#[test]
	fn test_state_store_with_named_layout() {
		let mut store = TestStateStore::new();
		let layout = EncodedValuesNamedLayout::new(vec![
			("count".to_string(), Type::Int8),
			("name".to_string(), Type::Utf8),
		]);
		let key = encode_key("test_key");

		let mut values = HashMap::new();
		values.insert("count".to_string(), Value::Int8(10i64));
		values.insert("name".to_string(), Value::Utf8("test".into()));

		store.set_named_value(key.clone(), &values, &layout);

		let decoded = store.decode_named_value(&key, &layout).unwrap();
		assert_eq!(decoded, values);
	}

	#[test]
	fn test_state_store_snapshot_and_restore() {
		use reifydb_type::util::cowvec::CowVec;
		let mut store = TestStateStore::new();
		let key1 = encode_key("key1");
		let key2 = encode_key("key2");

		store.set(key1.clone(), EncodedValues(CowVec::new(vec![1])));
		store.set(key2.clone(), EncodedValues(CowVec::new(vec![2])));

		let snapshot = store.snapshot();
		assert_eq!(snapshot.len(), 2);

		store.clear();
		assert!(store.is_empty());

		store.restore(snapshot);
		assert_eq!(store.len(), 2);
		assert_eq!(store.get(&key1), Some(&EncodedValues(CowVec::new(vec![1]))));
		assert_eq!(store.get(&key2), Some(&EncodedValues(CowVec::new(vec![2]))));
	}

	#[test]
	fn test_state_store_assertions() {
		let mut store = TestStateStore::new();
		let layout = EncodedValuesLayout::testing(&[Type::Int8]);
		let key = encode_key("test_key");
		let values = vec![Value::Int8(100i64)];

		store.set_value(key.clone(), &values, &layout);

		store.assert_exists(&key);
		store.assert_value(&key, &values, &layout);
		store.assert_count(1);

		let missing_key = encode_key("missing");
		store.assert_not_exists(&missing_key);
	}
}
