// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file
use reifydb_core::{
	EncodedKey,
	interface::FlowNodeId,
	key::{EncodableKey, FlowNodeStateKey},
	util::{CowVec, encoding::keycode::KeySerializer},
	value::encoded::{EncodedKeyRange, EncodedValues},
};
use reifydb_type::RowNumber;

use crate::{operator::stateful::RawStatefulOperator, transaction::FlowTransaction};

/// Provides stable encoded numbers for keys with automatic Insert/Update detection
///
/// This component maintains:
/// - A sequential counter for generating new encoded numbers
/// - A mapping from keys to their assigned encoded numbers
///
/// When a key is seen for the first time, it gets a new encoded number and returns
/// true. When a key is seen again, it returns the existing encoded number and
/// false.
pub struct RowNumberProvider {
	node: FlowNodeId,
}

impl RowNumberProvider {
	/// Create a new RowNumberProvider for the given operator
	pub fn new(node: FlowNodeId) -> Self {
		Self {
			node,
		}
	}

	/// Get or create a RowNumber for a given key
	/// Returns (RowNumber, is_new) where is_new indicates if it was newly
	/// created
	pub fn get_or_create_row_number<O: RawStatefulOperator>(
		&self,
		txn: &mut FlowTransaction,
		operator: &O,
		key: &EncodedKey,
	) -> crate::Result<(RowNumber, bool)> {
		// Check if we already have a encoded number for this key
		let map_key = self.make_map_key(key);
		let encoded_map_key = EncodedKey::new(map_key.clone());

		if let Some(existing_row) = operator.state_get(txn, &encoded_map_key)? {
			// Key exists, return existing encoded number
			let bytes = existing_row.as_ref();
			if bytes.len() >= 8 {
				let row_num = u64::from_be_bytes([
					bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
				]);
				return Ok((RowNumber(row_num), false));
			}
		}

		// Key doesn't exist, generate a new encoded number
		let counter = self.load_counter::<O>(txn, operator)?;
		let new_row_number = RowNumber(counter);

		// Save the new counter value
		self.save_counter::<O>(txn, operator, counter + 1)?;

		// Save the mapping from key to encoded number
		let row_num_bytes = counter.to_be_bytes().to_vec();
		operator.state_set(txn, &encoded_map_key, EncodedValues(CowVec::new(row_num_bytes)))?;

		Ok((new_row_number, true))
	}

	/// Load the current counter value
	fn load_counter<O: RawStatefulOperator>(&self, txn: &mut FlowTransaction, operator: &O) -> crate::Result<u64> {
		let key = self.make_counter_key();
		let encoded_key = EncodedKey::new(key);
		match operator.state_get(txn, &encoded_key)? {
			None => Ok(1), // First time, start at 1
			Some(state_row) => {
				// Parse the stored counter
				let bytes = state_row.as_ref();
				if bytes.len() >= 8 {
					Ok(u64::from_be_bytes([
						bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
						bytes[7],
					]))
				} else {
					Ok(1)
				}
			}
		}
	}

	/// Save the counter value
	fn save_counter<O: RawStatefulOperator>(
		&self,
		txn: &mut FlowTransaction,
		operator: &O,
		counter: u64,
	) -> crate::Result<()> {
		let key = self.make_counter_key();
		let encoded_key = EncodedKey::new(key);
		let value = EncodedValues(CowVec::new(counter.to_be_bytes().to_vec()));
		operator.state_set(txn, &encoded_key, value)?;
		Ok(())
	}

	/// Create a key for the counter, including node_id
	fn make_counter_key(&self) -> Vec<u8> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u64(self.node.0);
		serializer.extend_u8(b'C'); // 'C' for counter
		serializer.finish()
	}

	/// Create a mapping key for a given encoded key, including node_id
	fn make_map_key(&self, key: &EncodedKey) -> Vec<u8> {
		let mut serializer = KeySerializer::new();
		serializer.extend_u64(self.node.0);
		serializer.extend_u8(b'M'); // 'M' for mapping
		serializer.extend_bytes(key.as_ref());
		serializer.finish()
	}

	/// Remove all encoded number mappings with the given prefix
	/// This is useful for cleaning up all join results from a specific left encoded
	pub fn remove_by_prefix<O: RawStatefulOperator>(
		&self,
		txn: &mut FlowTransaction,
		operator: &O,
		key_prefix: &[u8],
	) -> crate::Result<()> {
		// Create the prefix for scanning
		let mut prefix = Vec::new();
		let mut serializer = KeySerializer::new();
		serializer.extend_u64(self.node.0);
		serializer.extend_u8(b'M'); // 'M' for mapping
		prefix.extend_from_slice(&serializer.finish());
		prefix.extend_from_slice(key_prefix);

		// Create range for prefix scan with the operator state prefix
		let state_prefix = FlowNodeStateKey::new(operator.id(), prefix.clone());
		let full_range = EncodedKeyRange::prefix(&state_prefix.encode());

		// Collect keys to remove (similar pattern to state_clear in utils.rs)
		let keys_to_remove: Vec<_> = txn.range(full_range)?.map(|multi| multi.key).collect();

		for key in keys_to_remove {
			txn.remove(&key)?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::CommitVersion;

	use super::*;
	use crate::operator::stateful::test_utils::test::*;

	// TestOperator already implements SimpleStatefulOperator

	#[test]
	fn test_first_row_number() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1));
		let operator = TestOperator::simple(FlowNodeId(1));
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("first");
		let (row_num, is_new) = provider.get_or_create_row_number(&mut txn, &operator, &key).unwrap();

		assert_eq!(row_num.0, 1);
		assert!(is_new);
	}

	#[test]
	fn test_duplicate_key_same_row_number() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1));
		let operator = TestOperator::simple(FlowNodeId(1));
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("duplicate");

		// First call - should create new
		let (row_num1, is_new1) = provider.get_or_create_row_number(&mut txn, &operator, &key).unwrap();
		assert_eq!(row_num1.0, 1);
		assert!(is_new1);

		// Second call with same key - should return existing
		let (row_num2, is_new2) = provider.get_or_create_row_number(&mut txn, &operator, &key).unwrap();
		assert_eq!(row_num2.0, 1);
		assert!(!is_new2);

		// Row numbers should be the same
		assert_eq!(row_num1, row_num2);
	}

	#[test]
	fn test_sequential_row_numbers() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1));
		let operator = TestOperator::simple(FlowNodeId(1));
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create multiple unique keys
		for i in 1..=5 {
			let key = test_key(&format!("key_{}", i));
			let (row_num, is_new) = provider.get_or_create_row_number(&mut txn, &operator, &key).unwrap();

			assert_eq!(row_num.0, i as u64);
			assert!(is_new);
		}
	}

	#[test]
	fn test_mixed_new_and_existing() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1));
		let operator = TestOperator::simple(FlowNodeId(1));
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create some keys
		let key1 = test_key("mixed_1");
		let key2 = test_key("mixed_2");
		let key3 = test_key("mixed_3");

		// First round - all new
		let (rn1, new1) = provider.get_or_create_row_number(&mut txn, &operator, &key1).unwrap();
		let (rn2, new2) = provider.get_or_create_row_number(&mut txn, &operator, &key2).unwrap();
		let (rn3, new3) = provider.get_or_create_row_number(&mut txn, &operator, &key3).unwrap();

		assert_eq!(rn1.0, 1);
		assert!(new1);
		assert_eq!(rn2.0, 2);
		assert!(new2);
		assert_eq!(rn3.0, 3);
		assert!(new3);

		// Second round - mixed
		let key4 = test_key("mixed_4");
		let (rn2_again, new2_again) = provider.get_or_create_row_number(&mut txn, &operator, &key2).unwrap();
		let (rn4, new4) = provider.get_or_create_row_number(&mut txn, &operator, &key4).unwrap();
		let (rn1_again, new1_again) = provider.get_or_create_row_number(&mut txn, &operator, &key1).unwrap();

		assert_eq!(rn2_again.0, 2);
		assert!(!new2_again);
		assert_eq!(rn4.0, 4); // Next sequential number
		assert!(new4);
		assert_eq!(rn1_again.0, 1);
		assert!(!new1_again);
	}

	#[test]
	fn test_multiple_providers_isolated() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1));
		let operator1 = TestOperator::simple(FlowNodeId(1));
		let operator2 = TestOperator::simple(FlowNodeId(2));
		let provider1 = RowNumberProvider::new(FlowNodeId(1));
		let provider2 = RowNumberProvider::new(FlowNodeId(2));

		let key = test_key("shared_key");

		// Same key in different providers should get different encoded numbers
		let (rn1, _) = provider1.get_or_create_row_number(&mut txn, &operator1, &key).unwrap();
		let (rn2, _) = provider2.get_or_create_row_number(&mut txn, &operator2, &key).unwrap();

		assert_eq!(rn1.0, 1);
		assert_eq!(rn2.0, 1);

		// Add more keys to provider1
		let key2 = test_key("key2");
		let (rn1_2, _) = provider1.get_or_create_row_number(&mut txn, &operator1, &key2).unwrap();
		assert_eq!(rn1_2.0, 2);

		// Provider2 should still be at 1 for new keys
		let (rn2_2, _) = provider2.get_or_create_row_number(&mut txn, &operator2, &key2).unwrap();
		assert_eq!(rn2_2.0, 2);
	}

	#[test]
	fn test_counter_persistence() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1));
		let operator = TestOperator::simple(FlowNodeId(1));
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create some encoded numbers
		for i in 1..=3 {
			let key = test_key(&format!("persist_{}", i));
			let (rn, _) = provider.get_or_create_row_number(&mut txn, &operator, &key).unwrap();
			assert_eq!(rn.0, i as u64);
		}

		// Simulate loading counter again (internally happens in get_or_create)
		let new_key = test_key("persist_new");
		let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &operator, &new_key).unwrap();

		// Should continue from where we left off
		assert_eq!(rn.0, 4);
		assert!(is_new);
	}

	#[test]
	fn test_large_row_numbers() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1));
		let operator = TestOperator::simple(FlowNodeId(1));
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create many encoded numbers
		for i in 1..=1000 {
			let key = test_key(&format!("large_{}", i));
			let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &operator, &key).unwrap();
			assert_eq!(rn.0, i as u64);
			assert!(is_new);
		}

		// Verify we can still retrieve early ones
		let key = test_key("large_1");
		let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &operator, &key).unwrap();
		assert_eq!(rn.0, 1);
		assert!(!is_new);

		// And continue adding new ones
		let key = test_key("large_1001");
		let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &operator, &key).unwrap();
		assert_eq!(rn.0, 1001);
		assert!(is_new);
	}
}
