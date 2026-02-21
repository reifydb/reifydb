// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use std::collections::HashMap;

use reifydb_core::encoded::{encoded::EncodedValues, key::EncodedKey, schema::Schema};
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

	/// Decode a value using a schema
	pub fn decode_value(&self, key: &EncodedKey, schema: &Schema) -> Option<Vec<Value>> {
		self.get(key).map(|encoded| super::helpers::get_values(schema, encoded))
	}

	/// Decode a value using a schema with field names
	pub fn decode_named_value(&self, key: &EncodedKey, schema: &Schema) -> Option<HashMap<String, Value>> {
		self.get(key).map(|encoded| {
			let values = super::helpers::get_values(schema, encoded);
			schema.field_names().map(|n| n.to_string()).zip(values).collect()
		})
	}

	/// Set a value using a schema
	pub fn set_value(&mut self, key: EncodedKey, values: &[Value], schema: &Schema) {
		let mut encoded = schema.allocate();
		schema.set_values(&mut encoded, values);
		self.set(key, encoded);
	}

	/// Set a value using a schema with field names
	pub fn set_named_value(&mut self, key: EncodedKey, values: &HashMap<String, Value>, schema: &Schema) {
		let mut encoded = schema.allocate();

		// Convert HashMap to ordered values based on schema field names
		let ordered_values: Vec<Value> =
			schema.field_names().map(|name| values.get(name).cloned().unwrap_or(Value::none())).collect();

		schema.set_values(&mut encoded, &ordered_values);
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
	pub fn assert_value(&self, key: &EncodedKey, expected: &[Value], schema: &Schema) {
		let actual = self.decode_value(key, schema).expect(&format!("Key {:?} not found in state", key));
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
	use reifydb_core::encoded::{encoded::EncodedValues, schema::Schema};
	use reifydb_type::{util::cowvec::CowVec, value::r#type::Type};

	use super::*;
	use crate::testing::helpers::encode_key;

	#[test]
	fn test_state_store_basic_operations() {
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
	fn test_state_store_with_schema() {
		let mut store = TestStateStore::new();
		let schema = Schema::testing(&[Type::Int8, Type::Utf8]);
		let key = encode_key("test_key");
		let values = vec![Value::Int8(42i64), Value::Utf8("hello".into())];

		store.set_value(key.clone(), &values, &schema);

		let decoded = store.decode_value(&key, &schema).unwrap();
		assert_eq!(decoded, values);
	}

	#[test]
	fn test_state_store_with_named_schema() {
		let mut store = TestStateStore::new();
		let schema = Schema::new(vec![
			reifydb_core::encoded::schema::SchemaField::unconstrained("count", Type::Int8),
			reifydb_core::encoded::schema::SchemaField::unconstrained("name", Type::Utf8),
		]);
		let key = encode_key("test_key");

		let mut values = HashMap::new();
		values.insert("count".to_string(), Value::Int8(10i64));
		values.insert("name".to_string(), Value::Utf8("test".into()));

		store.set_named_value(key.clone(), &values, &schema);

		let decoded = store.decode_named_value(&key, &schema).unwrap();
		assert_eq!(decoded, values);
	}

	#[test]
	fn test_state_store_snapshot_and_restore() {
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
		let schema = Schema::testing(&[Type::Int8]);
		let key = encode_key("test_key");
		let values = vec![Value::Int8(100i64)];

		store.set_value(key.clone(), &values, &schema);

		store.assert_exists(&key);
		store.assert_value(&key, &values, &schema);
		store.assert_count(1);

		let missing_key = encode_key("missing");
		store.assert_not_exists(&missing_key);
	}
}
