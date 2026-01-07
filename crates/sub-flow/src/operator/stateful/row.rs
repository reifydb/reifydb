// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB
use std::iter::once;

use reifydb_core::{
	EncodedKey,
	interface::FlowNodeId,
	key::{EncodableKey, FlowNodeInternalStateKey},
	util::{CowVec, encoding::keycode::KeySerializer},
	value::encoded::{EncodedKeyRange, EncodedValues},
};
use reifydb_type::RowNumber;

use crate::{
	operator::stateful::utils::{internal_state_get, internal_state_set},
	transaction::FlowTransaction,
};

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

	/// Get or create RowNumbers for multiple keys
	/// Returns Vec<(RowNumber, is_new)> in the same order as input keys
	/// where is_new indicates if the row number was newly created
	pub fn get_or_create_row_numbers<'a, I>(
		&self,
		txn: &mut FlowTransaction,
		keys: I,
	) -> reifydb_type::Result<Vec<(RowNumber, bool)>>
	where
		I: IntoIterator<Item = &'a EncodedKey>,
	{
		let mut results = Vec::new();
		let mut counter = self.load_counter(txn)?;
		let initial_counter = counter;

		for key in keys {
			let map_key = self.make_map_key(key);

			if let Some(existing_row) = internal_state_get(self.node, txn, &map_key)? {
				let bytes = existing_row.as_ref();
				if bytes.len() >= 8 {
					let row_num = u64::from_be_bytes([
						bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6],
						bytes[7],
					]);
					results.push((RowNumber(row_num), false));
					continue;
				}
			}

			let new_row_number = RowNumber(counter);

			// Save the mapping from key to encoded number
			let row_num_bytes = counter.to_be_bytes().to_vec();
			internal_state_set(self.node, txn, &map_key, EncodedValues(CowVec::new(row_num_bytes)))?;

			// Save the reverse mapping from row_number to key
			let reverse_key = self.make_reverse_map_key(new_row_number);
			internal_state_set(
				self.node,
				txn,
				&reverse_key,
				EncodedValues(CowVec::new(key.as_ref().to_vec())),
			)?;

			results.push((new_row_number, true));
			counter += 1;
		}

		// Save the updated counter if we allocated any new row numbers
		if counter != initial_counter {
			self.save_counter(txn, counter)?;
		}

		Ok(results)
	}

	/// Get or create a RowNumber for a given key
	/// Returns (RowNumber, is_new) where is_new indicates if it was newly
	/// created
	pub fn get_or_create_row_number(
		&self,
		txn: &mut FlowTransaction,
		key: &EncodedKey,
	) -> reifydb_type::Result<(RowNumber, bool)> {
		Ok(self.get_or_create_row_numbers(txn, once(key))?.into_iter().next().unwrap())
	}

	/// Get the original key for a given row number (reverse lookup)
	pub fn get_key_for_row_number(
		&self,
		txn: &mut FlowTransaction,
		row_number: RowNumber,
	) -> reifydb_type::Result<Option<EncodedKey>> {
		let reverse_key = self.make_reverse_map_key(row_number);
		if let Some(key_bytes) = internal_state_get(self.node, txn, &reverse_key)? {
			Ok(Some(EncodedKey::new(key_bytes.as_ref().to_vec())))
		} else {
			Ok(None)
		}
	}

	/// Load the current counter value
	fn load_counter(&self, txn: &mut FlowTransaction) -> reifydb_type::Result<u64> {
		let key = self.make_counter_key();
		match internal_state_get(self.node, txn, &key)? {
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
	fn save_counter(&self, txn: &mut FlowTransaction, counter: u64) -> reifydb_type::Result<()> {
		let key = self.make_counter_key();
		let value = EncodedValues(CowVec::new(counter.to_be_bytes().to_vec()));
		internal_state_set(self.node, txn, &key, value)?;
		Ok(())
	}

	/// Create a key for the counter (node_id added by FlowNodeInternalStateKey wrapper)
	fn make_counter_key(&self) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'C'); // 'C' for counter
		EncodedKey::new(serializer.finish())
	}

	/// Create a mapping key for a given encoded key (node_id added by FlowNodeInternalStateKey wrapper)
	fn make_map_key(&self, key: &EncodedKey) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'M'); // 'M' for mapping
		serializer.extend_bytes(key.as_ref());
		EncodedKey::new(serializer.finish())
	}

	/// Create a reverse mapping key for a given row number (node_id added by FlowNodeInternalStateKey wrapper)
	fn make_reverse_map_key(&self, row_number: RowNumber) -> EncodedKey {
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'R'); // 'R' for reverse mapping
		serializer.extend_u64(row_number.0);
		EncodedKey::new(serializer.finish())
	}

	/// Remove all encoded number mappings with the given prefix
	/// This is useful for cleaning up all join results from a specific left encoded
	pub fn remove_by_prefix(&self, txn: &mut FlowTransaction, key_prefix: &[u8]) -> reifydb_type::Result<()> {
		// Create the prefix for scanning
		let mut prefix = Vec::new();
		let mut serializer = KeySerializer::new();
		serializer.extend_u8(b'M'); // 'M' for mapping
		prefix.extend_from_slice(&serializer.finish());
		prefix.extend_from_slice(key_prefix);

		let state_prefix = FlowNodeInternalStateKey::new(self.node, prefix.clone());
		let full_range = EncodedKeyRange::prefix(&state_prefix.encode());

		let keys_to_remove = {
			let mut stream = txn.range(full_range, 1024);
			let mut keys = Vec::new();
			while let Some(result) = stream.next() {
				let multi = result?;
				keys.push(multi.key);
			}
			keys
		};

		for key in keys_to_remove {
			txn.remove(&key)?;
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::Catalog;
	use reifydb_core::CommitVersion;

	use super::*;
	use crate::operator::stateful::test_utils::test::*;

	#[test]
	fn test_first_row_number() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("first");
		let (row_num, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();

		assert_eq!(row_num.0, 1);
		assert!(is_new);
	}

	#[test]
	fn test_duplicate_key_same_row_number() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let provider = RowNumberProvider::new(FlowNodeId(1));

		let key = test_key("duplicate");

		// First call - should create new
		let (row_num1, is_new1) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert_eq!(row_num1.0, 1);
		assert!(is_new1);

		// Second call with same key - should return existing
		let (row_num2, is_new2) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert_eq!(row_num2.0, 1);
		assert!(!is_new2);

		// Row numbers should be the same
		assert_eq!(row_num1, row_num2);
	}

	#[test]
	fn test_sequential_row_numbers() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create multiple unique keys
		for i in 1..=5 {
			let key = test_key(&format!("key_{}", i));
			let (row_num, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();

			assert_eq!(row_num.0, i as u64);
			assert!(is_new);
		}
	}

	#[test]
	fn test_mixed_new_and_existing() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create some keys
		let key1 = test_key("mixed_1");
		let key2 = test_key("mixed_2");
		let key3 = test_key("mixed_3");

		// First round - all new
		let (rn1, new1) = provider.get_or_create_row_number(&mut txn, &key1).unwrap();
		let (rn2, new2) = provider.get_or_create_row_number(&mut txn, &key2).unwrap();
		let (rn3, new3) = provider.get_or_create_row_number(&mut txn, &key3).unwrap();

		assert_eq!(rn1.0, 1);
		assert!(new1);
		assert_eq!(rn2.0, 2);
		assert!(new2);
		assert_eq!(rn3.0, 3);
		assert!(new3);

		// Second round - mixed
		let key4 = test_key("mixed_4");
		let (rn2_again, new2_again) = provider.get_or_create_row_number(&mut txn, &key2).unwrap();
		let (rn4, new4) = provider.get_or_create_row_number(&mut txn, &key4).unwrap();
		let (rn1_again, new1_again) = provider.get_or_create_row_number(&mut txn, &key1).unwrap();

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
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let provider1 = RowNumberProvider::new(FlowNodeId(1));
		let provider2 = RowNumberProvider::new(FlowNodeId(2));

		let key = test_key("shared_key");

		// Same key in different providers should get different encoded numbers
		let (rn1, _) = provider1.get_or_create_row_number(&mut txn, &key).unwrap();
		let (rn2, _) = provider2.get_or_create_row_number(&mut txn, &key).unwrap();

		assert_eq!(rn1.0, 1);
		assert_eq!(rn2.0, 1);

		// Add more keys to provider1
		let key2 = test_key("key2");
		let (rn1_2, _) = provider1.get_or_create_row_number(&mut txn, &key2).unwrap();
		assert_eq!(rn1_2.0, 2);

		// Provider2 should still be at 1 for new keys
		let (rn2_2, _) = provider2.get_or_create_row_number(&mut txn, &key2).unwrap();
		assert_eq!(rn2_2.0, 2);
	}

	#[test]
	fn test_counter_persistence() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create some encoded numbers
		for i in 1..=3 {
			let key = test_key(&format!("persist_{}", i));
			let (rn, _) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
			assert_eq!(rn.0, i as u64);
		}

		// Simulate loading counter again (internally happens in get_or_create)
		let new_key = test_key("persist_new");
		let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &new_key).unwrap();

		// Should continue from where we left off
		assert_eq!(rn.0, 4);
		assert!(is_new);
	}

	#[test]
	fn test_large_row_numbers() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create many encoded numbers
		for i in 1..=1000 {
			let key = test_key(&format!("large_{}", i));
			let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
			assert_eq!(rn.0, i as u64);
			assert!(is_new);
		}

		// Verify we can still retrieve early ones
		let key = test_key("large_1");
		let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert_eq!(rn.0, 1);
		assert!(!is_new);

		// And continue adding new ones
		let key = test_key("large_1001");
		let (rn, is_new) = provider.get_or_create_row_number(&mut txn, &key).unwrap();
		assert_eq!(rn.0, 1001);
		assert!(is_new);
	}

	#[test]
	fn test_mixed_existing_and_new_keys() {
		let mut txn = create_test_transaction();
		let mut txn = FlowTransaction::new(&mut txn, CommitVersion(1), Catalog::default());
		let provider = RowNumberProvider::new(FlowNodeId(1));

		// Create 3 initial keys to establish existing row numbers
		let key1 = test_key("key_1");
		let key2 = test_key("key_2");
		let key3 = test_key("key_3");

		let (rn1, _) = provider.get_or_create_row_number(&mut txn, &key1).unwrap();
		assert_eq!(rn1.0, 1);

		let (rn2, _) = provider.get_or_create_row_number(&mut txn, &key2).unwrap();
		assert_eq!(rn2.0, 2);

		let (rn3, _) = provider.get_or_create_row_number(&mut txn, &key3).unwrap();
		assert_eq!(rn3.0, 3);

		// Now test batch with mix of existing and new keys
		let key4 = test_key("key_4");
		let key5 = test_key("key_5");

		// Batch: [existing key2, new key4, existing key1, new key5, existing key3]
		let keys = vec![&key2, &key4, &key1, &key5, &key3];

		let results = provider.get_or_create_row_numbers(&mut txn, keys.into_iter()).unwrap();

		// Verify results are in correct order and have correct values
		assert_eq!(results.len(), 5);

		// key2 (existing) -> row number 2, not new
		assert_eq!(results[0].0.0, 2);
		assert!(!results[0].1);

		// key4 (new) -> row number 4, is new
		assert_eq!(results[1].0.0, 4);
		assert!(results[1].1);

		// key1 (existing) -> row number 1, not new
		assert_eq!(results[2].0.0, 1);
		assert!(!results[2].1);

		// key5 (new) -> row number 5, is new
		assert_eq!(results[3].0.0, 5);
		assert!(results[3].1);

		// key3 (existing) -> row number 3, not new
		assert_eq!(results[4].0.0, 3);
		assert!(!results[4].1);

		// Verify that counter was only incremented by 2 (for key4 and key5)
		// by checking that the next new key gets row number 6
		let key6 = test_key("key_6");
		let (rn6, is_new6) = provider.get_or_create_row_number(&mut txn, &key6).unwrap();
		assert_eq!(rn6.0, 6);
		assert!(is_new6);

		// Verify all mappings are still correct by retrieving them individually
		let (check_rn4, is_new4) = provider.get_or_create_row_number(&mut txn, &key4).unwrap();
		assert_eq!(check_rn4.0, 4);
		assert!(!is_new4);

		let (check_rn5, is_new5) = provider.get_or_create_row_number(&mut txn, &key5).unwrap();
		assert_eq!(check_rn5.0, 5);
		assert!(!is_new5);

		// Verify reverse mappings exist for the new keys created in batch
		let reverse_key4 = provider.get_key_for_row_number(&mut txn, RowNumber(4)).unwrap();
		assert_eq!(reverse_key4, Some(key4));

		let reverse_key5 = provider.get_key_for_row_number(&mut txn, RowNumber(5)).unwrap();
		assert_eq!(reverse_key5, Some(key5));

		// Verify reverse mappings also exist for keys created before batch
		let reverse_key1 = provider.get_key_for_row_number(&mut txn, RowNumber(1)).unwrap();
		assert_eq!(reverse_key1, Some(key1));

		let reverse_key2 = provider.get_key_for_row_number(&mut txn, RowNumber(2)).unwrap();
		assert_eq!(reverse_key2, Some(key2));
	}
}
