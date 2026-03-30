// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::encoded::{key::EncodedKey, row::EncodedRow, shape::RowShape};
use reifydb_type::value::Value;

use super::helpers::get_values;

/// Mock state store for testing operators
#[derive(Debug, Clone, Default)]
pub struct TestStateStore {
	data: HashMap<EncodedKey, EncodedRow>,
}

impl TestStateStore {
	/// Create a new empty mock state store
	pub fn new() -> Self {
		Self {
			data: HashMap::new(),
		}
	}

	/// Get a value from the store
	pub fn get(&self, key: &EncodedKey) -> Option<&EncodedRow> {
		self.data.get(key)
	}

	/// Set a value in the store
	pub fn set(&mut self, key: EncodedKey, value: EncodedRow) {
		self.data.insert(key, value);
	}

	/// Remove a value from the store
	pub fn remove(&mut self, key: &EncodedKey) -> Option<EncodedRow> {
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
	pub fn entries(&self) -> Vec<(&EncodedKey, &EncodedRow)> {
		self.data.iter().collect()
	}

	/// Decode a value using a shape
	pub fn decode_value(&self, key: &EncodedKey, shape: &RowShape) -> Option<Vec<Value>> {
		self.get(key).map(|encoded| get_values(shape, encoded))
	}

	/// Decode a value using a shape with field names
	pub fn decode_named_value(&self, key: &EncodedKey, shape: &RowShape) -> Option<HashMap<String, Value>> {
		self.get(key).map(|encoded| {
			let values = get_values(shape, encoded);
			shape.field_names().map(|n| n.to_string()).zip(values).collect()
		})
	}

	/// Set a value using a shape
	pub fn set_value(&mut self, key: EncodedKey, values: &[Value], shape: &RowShape) {
		let mut encoded = shape.allocate();
		shape.set_values(&mut encoded, values);
		self.set(key, encoded);
	}

	/// Set a value using a shape with field names
	pub fn set_named_value(&mut self, key: EncodedKey, values: &HashMap<String, Value>, shape: &RowShape) {
		let mut encoded = shape.allocate();

		// Convert HashMap to ordered values based on shape field names
		let ordered_values: Vec<Value> =
			shape.field_names().map(|name| values.get(name).cloned().unwrap_or(Value::none())).collect();

		shape.set_values(&mut encoded, &ordered_values);
		self.set(key, encoded);
	}

	/// Create a snapshot of the current state
	pub fn snapshot(&self) -> HashMap<EncodedKey, EncodedRow> {
		self.data.clone()
	}

	/// Restore from a snapshot
	pub fn restore(&mut self, snapshot: HashMap<EncodedKey, EncodedRow>) {
		self.data = snapshot;
	}

	/// Assert that a key has a specific value
	pub fn assert_value(&self, key: &EncodedKey, expected: &[Value], shape: &RowShape) {
		let actual = self.decode_value(key, shape).expect(&format!("Key {:?} not found in state", key));
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
		row::EncodedRow,
		shape::{RowShape, RowShapeField},
	};
	use reifydb_type::{util::cowvec::CowVec, value::r#type::Type};

	use super::*;
	use crate::testing::helpers::encode_key;

	#[test]
	fn test_state_store_basic_operations() {
		let mut store = TestStateStore::new();
		let key = encode_key("test_key");
		let value = EncodedRow(CowVec::new(vec![1, 2, 3, 4]));

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
	fn test_state_store_with_shape() {
		let mut store = TestStateStore::new();
		let shape = RowShape::testing(&[Type::Int8, Type::Utf8]);
		let key = encode_key("test_key");
		let values = vec![Value::Int8(42i64), Value::Utf8("hello".into())];

		store.set_value(key.clone(), &values, &shape);

		let decoded = store.decode_value(&key, &shape).unwrap();
		assert_eq!(decoded, values);
	}

	#[test]
	fn test_state_store_with_named_shape() {
		let mut store = TestStateStore::new();
		let shape = RowShape::new(vec![
			RowShapeField::unconstrained("count", Type::Int8),
			RowShapeField::unconstrained("name", Type::Utf8),
		]);
		let key = encode_key("test_key");

		let mut values = HashMap::new();
		values.insert("count".to_string(), Value::Int8(10i64));
		values.insert("name".to_string(), Value::Utf8("test".into()));

		store.set_named_value(key.clone(), &values, &shape);

		let decoded = store.decode_named_value(&key, &shape).unwrap();
		assert_eq!(decoded, values);
	}

	#[test]
	fn test_state_store_snapshot_and_restore() {
		let mut store = TestStateStore::new();
		let key1 = encode_key("key1");
		let key2 = encode_key("key2");

		store.set(key1.clone(), EncodedRow(CowVec::new(vec![1])));
		store.set(key2.clone(), EncodedRow(CowVec::new(vec![2])));

		let snapshot = store.snapshot();
		assert_eq!(snapshot.len(), 2);

		store.clear();
		assert!(store.is_empty());

		store.restore(snapshot);
		assert_eq!(store.len(), 2);
		assert_eq!(store.get(&key1), Some(&EncodedRow(CowVec::new(vec![1]))));
		assert_eq!(store.get(&key2), Some(&EncodedRow(CowVec::new(vec![2]))));
	}

	#[test]
	fn test_state_store_assertions() {
		let mut store = TestStateStore::new();
		let shape = RowShape::testing(&[Type::Int8]);
		let key = encode_key("test_key");
		let values = vec![Value::Int8(100i64)];

		store.set_value(key.clone(), &values, &shape);

		store.assert_exists(&key);
		store.assert_value(&key, &values, &shape);
		store.assert_count(1);

		let missing_key = encode_key("missing");
		store.assert_not_exists(&missing_key);
	}
}
