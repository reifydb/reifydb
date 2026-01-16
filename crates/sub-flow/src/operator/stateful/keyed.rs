// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::{encoded::EncodedValues, key::EncodedKey, layout::EncodedValuesLayout},
	util::encoding::keycode::serializer::KeySerializer,
};
use reifydb_type::value::{Value, r#type::Type};

use super::utils;
use crate::{operator::stateful::raw::RawStatefulOperator, transaction::FlowTransaction};

/// Operator with multiple keyed state values (for aggregations, grouping, etc.)
/// Extends TransformOperator directly and uses utility functions for state management
pub trait KeyedStateful: RawStatefulOperator {
	/// Get or create the layout for state rows
	fn layout(&self) -> EncodedValuesLayout;

	/// Schema for keys - defines the types of the key components
	fn key_types(&self) -> &[Type];

	/// Create EncodedKey from Values
	fn encode_key(&self, key_values: &[Value]) -> EncodedKey {
		// Use keycode encoding for order-preserving keys
		let mut serializer = KeySerializer::new();

		for value in key_values.iter() {
			serializer.extend_value(value);
		}

		EncodedKey::new(serializer.finish())
	}

	/// Create a new state encoded with default values
	fn create_state(&self) -> EncodedValues {
		let layout = self.layout();
		layout.allocate()
	}

	/// Load state for a specific key
	fn load_state(&self, txn: &mut FlowTransaction, key_values: &[Value]) -> reifydb_type::Result<EncodedValues> {
		let key = self.encode_key(key_values);
		utils::load_or_create_row(self.id(), txn, &key, &self.layout())
	}

	/// Save state for a specific key
	fn save_state(
		&self,
		txn: &mut FlowTransaction,
		key_values: &[Value],
		row: EncodedValues,
	) -> reifydb_type::Result<()> {
		let key = self.encode_key(key_values);
		utils::save_row(self.id(), txn, &key, row)
	}

	/// Update state for a key with a function
	fn update_state<F>(
		&self,
		txn: &mut FlowTransaction,
		key_values: &[Value],
		f: F,
	) -> reifydb_type::Result<EncodedValues>
	where
		F: FnOnce(&EncodedValuesLayout, &mut EncodedValues) -> reifydb_type::Result<()>,
	{
		let layout = self.layout();
		let mut row = self.load_state(txn, key_values)?;
		f(&layout, &mut row)?;
		self.save_state(txn, key_values, row.clone())?;
		Ok(row)
	}

	/// Remove state for a key
	fn remove_state(&self, txn: &mut FlowTransaction, key_values: &[Value]) -> reifydb_type::Result<()> {
		let key = self.encode_key(key_values);
		utils::state_remove(self.id(), txn, &key)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::{common::CommitVersion, interface::catalog::flow::FlowNodeId};
	use reifydb_type::value::{Value, r#type::Type};

	use super::*;
	#[cfg(test)]
	use crate::operator::stateful::test_utils::test::*;
	use crate::transaction::FlowTransaction;

	// Extend TestOperator to implement KeyedStateful
	impl KeyedStateful for TestOperator {
		fn layout(&self) -> EncodedValuesLayout {
			self.layout.clone()
		}

		fn key_types(&self) -> &[Type] {
			&self.key_types
		}
	}

	#[test]
	fn test_encode_key() {
		let operator = TestOperator::with_key_types(FlowNodeId(1), vec![Type::Int4, Type::Utf8]);

		// Test encoding with different key values
		let key1 = vec![Value::Int4(42), Value::Utf8("test".to_string())];
		let encoded1 = operator.encode_key(&key1);

		let key2 = vec![Value::Int4(42), Value::Utf8("test2".to_string())];
		let encoded2 = operator.encode_key(&key2);

		// Different keys should produce different encodings
		assert_ne!(encoded1.as_ref(), encoded2.as_ref());

		// Same key should produce same encoding
		let encoded1_again = operator.encode_key(&key1);
		assert_eq!(encoded1.as_ref(), encoded1_again.as_ref());
	}

	#[test]
	fn test_create_state() {
		let operator = TestOperator::new(FlowNodeId(1));
		let state = operator.create_state();

		// State should have the correct size for layout
		assert!(state.len() > 0);
	}

	#[test]
	fn test_load_save_state() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let operator = TestOperator::with_key_types(FlowNodeId(1), vec![Type::Int4, Type::Utf8]);
		let key = vec![Value::Int4(100), Value::Utf8("key1".to_string())];

		// Initially should create new state
		let state1 = operator.load_state(&mut txn, &key).unwrap();

		// Modify and save
		let mut modified = state1.clone();
		modified.make_mut()[0] = 0x42; // Modify first byte
		operator.save_state(&mut txn, &key, modified.clone()).unwrap();

		// Load should return modified state
		let state2 = operator.load_state(&mut txn, &key).unwrap();
		assert_eq!(state2.as_ref()[0], 0x42);
	}

	#[test]
	fn test_update_state() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let operator = TestOperator::with_key_types(FlowNodeId(1), vec![Type::Int4, Type::Utf8]);
		let key = vec![Value::Int4(200), Value::Utf8("update_key".to_string())];

		// Update with a function
		let result = operator
			.update_state(&mut txn, &key, |_layout, row| {
				// Set first byte to a specific value
				row.make_mut()[0] = 0x55;
				Ok(())
			})
			.unwrap();

		assert_eq!(result.as_ref()[0], 0x55);

		// Verify it was persisted
		let loaded = operator.load_state(&mut txn, &key).unwrap();
		assert_eq!(loaded.as_ref()[0], 0x55);
	}

	#[test]
	fn test_remove_state() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let operator = TestOperator::with_key_types(FlowNodeId(1), vec![Type::Int4, Type::Utf8]);
		let key = vec![Value::Int4(300), Value::Utf8("remove_key".to_string())];

		// Create and save state
		let state = operator.create_state();
		operator.save_state(&mut txn, &key, state).unwrap();

		// Remove state
		operator.remove_state(&mut txn, &key).unwrap();

		// Loading should create new state (not find existing)
		let new_state = operator.load_state(&mut txn, &key).unwrap();
		assert_eq!(new_state.as_ref()[0], 0); // Should be default initialized
	}

	#[test]
	fn test_multiple_keys() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let operator = TestOperator::with_key_types(FlowNodeId(1), vec![Type::Int4, Type::Utf8]);

		// Create multiple keys with different states
		for i in 0..5 {
			let key = vec![Value::Int4(i), Value::Utf8(format!("key_{}", i))];
			operator.update_state(&mut txn, &key, |_layout, row| {
				row.make_mut()[0] = i as u8;
				Ok(())
			})
			.unwrap();
		}

		// Verify each key has its own state
		for i in 0..5 {
			let key = vec![Value::Int4(i), Value::Utf8(format!("key_{}", i))];
			let state = operator.load_state(&mut txn, &key).unwrap();
			assert_eq!(state.as_ref()[0], i as u8);
		}
	}

	#[test]
	fn test_key_ordering() {
		let operator = TestOperator::with_key_types(FlowNodeId(1), vec![Type::Int4, Type::Utf8]);

		// Test that keys maintain order
		let key1 = vec![Value::Int4(1), Value::Utf8("a".to_string())];
		let key2 = vec![Value::Int4(1), Value::Utf8("b".to_string())];
		let key3 = vec![Value::Int4(2), Value::Utf8("a".to_string())];

		let encoded1 = operator.encode_key(&key1);
		let encoded2 = operator.encode_key(&key2);
		let encoded3 = operator.encode_key(&key3);

		// Due to inverted encoding for integers, smaller values produce larger encoded values
		// But strings should maintain normal ordering
		assert!(encoded1 < encoded2); // Same int, "a" < "b"
		assert!(encoded3 < encoded1); // 2 > 1 in inverted encoding
	}
}
