// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, fmt::Debug};

use reifydb_codec::{
	encoded::{bytes::EncodedBytes, shape::RowShape},
	key::encoded::EncodedKey,
	operator::{EncodedOperatorRow, OperatorState, decode},
};
use reifydb_value::value::Value;

use super::helpers::get_values;

#[derive(Debug, Clone, Default)]
pub struct TestStateStore {
	data: HashMap<EncodedKey, EncodedBytes>,
}

impl TestStateStore {
	pub fn new() -> Self {
		Self {
			data: HashMap::new(),
		}
	}

	pub fn get(&self, key: &EncodedKey) -> Option<&EncodedBytes> {
		self.data.get(key)
	}

	pub fn set(&mut self, key: EncodedKey, value: EncodedBytes) {
		self.data.insert(key, value);
	}

	pub fn remove(&mut self, key: &EncodedKey) -> Option<EncodedBytes> {
		self.data.remove(key)
	}

	pub fn contains(&self, key: &EncodedKey) -> bool {
		self.data.contains_key(key)
	}

	pub fn len(&self) -> usize {
		self.data.len()
	}

	pub fn is_empty(&self) -> bool {
		self.data.is_empty()
	}

	pub fn clear(&mut self) {
		self.data.clear();
	}

	pub fn keys(&self) -> Vec<&EncodedKey> {
		self.data.keys().collect()
	}

	pub fn entries(&self) -> Vec<(&EncodedKey, &EncodedBytes)> {
		self.data.iter().collect()
	}

	pub fn decode_value(&self, key: &EncodedKey, shape: &RowShape) -> Option<Vec<Value>> {
		self.get(key).map(|encoded| get_values(shape, encoded))
	}

	pub fn set_value(&mut self, key: EncodedKey, values: &[Value], shape: &RowShape) {
		let mut encoded = shape.allocate();
		shape.set_values(&mut encoded, values);
		self.set(key, encoded.freeze());
	}

	pub fn snapshot(&self) -> HashMap<EncodedKey, EncodedBytes> {
		self.data.clone()
	}

	pub fn restore(&mut self, snapshot: HashMap<EncodedKey, EncodedBytes>) {
		self.data = snapshot;
	}

	pub fn assert_value(&self, key: &EncodedKey, expected: &[Value], shape: &RowShape) {
		let actual =
			self.decode_value(key, shape).unwrap_or_else(|| panic!("Key {:?} not found in state", key));
		assert_eq!(actual, expected, "State value mismatch for key {:?}", key);
	}

	pub fn decode_typed<T: OperatorState>(&self, key: &EncodedKey) -> Option<T> {
		let row = self.get(key)?;
		let bytes = EncodedOperatorRow::try_from(row.clone()).ok()?;
		decode(&bytes).ok()
	}

	pub fn assert_typed_value<T: OperatorState + PartialEq + Debug>(&self, key: &EncodedKey, expected: &T) {
		let actual = self.decode_typed::<T>(key).unwrap_or_else(|| panic!("Key {:?} not found in state", key));
		assert_eq!(&actual, expected, "Typed state value mismatch for key {:?}", key);
	}

	pub fn assert_exists(&self, key: &EncodedKey) {
		assert!(self.contains(key), "Expected key {:?} to exist in state", key);
	}

	pub fn assert_not_exists(&self, key: &EncodedKey) {
		assert!(!self.contains(key), "Expected key {:?} to not exist in state", key);
	}

	pub fn assert_count(&self, expected: usize) {
		assert_eq!(self.len(), expected, "Expected {} entries in state, found {}", expected, self.len());
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::encoded::{bytes::EncodedBytes, shape::RowShape};
	use reifydb_value::{util::cowvec::CowVec, value::value_type::ValueType};

	use super::*;
	use crate::helpers::encode_key;

	#[test]
	fn test_state_store_basic_operations() {
		let mut store = TestStateStore::new();
		let key = encode_key("test_key");
		let value = EncodedBytes(CowVec::new(vec![1, 2, 3, 4]));

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
		let shape = RowShape::testing(&[ValueType::Int8, ValueType::Utf8]);
		let key = encode_key("test_key");
		let values = vec![Value::Int8(42i64), Value::Utf8("hello".into())];

		store.set_value(key.clone(), &values, &shape);

		let decoded = store.decode_value(&key, &shape).unwrap();
		assert_eq!(decoded, values);
	}

	#[test]
	fn test_state_store_snapshot_and_restore() {
		let mut store = TestStateStore::new();
		let key1 = encode_key("key1");
		let key2 = encode_key("key2");

		store.set(key1.clone(), EncodedBytes(CowVec::new(vec![1])));
		store.set(key2.clone(), EncodedBytes(CowVec::new(vec![2])));

		let snapshot = store.snapshot();
		assert_eq!(snapshot.len(), 2);

		store.clear();
		assert!(store.is_empty());

		store.restore(snapshot);
		assert_eq!(store.len(), 2);
		assert_eq!(store.get(&key1), Some(&EncodedBytes(CowVec::new(vec![1]))));
		assert_eq!(store.get(&key2), Some(&EncodedBytes(CowVec::new(vec![2]))));
	}

	#[test]
	fn test_state_store_assertions() {
		let mut store = TestStateStore::new();
		let shape = RowShape::testing(&[ValueType::Int8]);
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
