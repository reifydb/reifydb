// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::dictionary::DictionaryDef,
	key::{
		EncodableKey,
		dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey, DictionarySequenceKey},
	},
	value::encoded::encoded::EncodedValues,
};
use reifydb_hash::xxh::xxh3_128;
use reifydb_transaction::standard::{StandardTransaction, command::StandardCommandTransaction};
use reifydb_type::{
	internal_error,
	util::cowvec::CowVec,
	value::{Value, dictionary::DictionaryEntryId},
};

pub(crate) trait DictionaryOperations {
	/// Insert a value into the dictionary, returning its ID.
	/// If the value already exists, returns the existing ID.
	/// If the value is new, assigns a new ID and stores it.
	/// The returned ID type matches the dictionary's `id_type`.
	fn insert_into_dictionary(
		&mut self,
		dictionary: &DictionaryDef,
		value: &Value,
	) -> crate::Result<DictionaryEntryId>;

	/// Get a value from the dictionary by its ID.
	/// Returns None if the ID doesn't exist.
	fn get_from_dictionary(
		&mut self,
		dictionary: &DictionaryDef,
		id: DictionaryEntryId,
	) -> crate::Result<Option<Value>>;

	/// Find the ID of a value in the dictionary without inserting.
	/// Returns the ID if the value exists, None otherwise.
	/// The returned ID type matches the dictionary's `id_type`.
	fn find_in_dictionary(
		&mut self,
		dictionary: &DictionaryDef,
		value: &Value,
	) -> crate::Result<Option<DictionaryEntryId>>;
}

impl DictionaryOperations for StandardCommandTransaction {
	fn insert_into_dictionary(
		&mut self,
		dictionary: &DictionaryDef,
		value: &Value,
	) -> crate::Result<DictionaryEntryId> {
		// 1. Serialize value and compute hash
		let value_bytes =
			postcard::to_stdvec(value).map_err(|e| internal_error!("Failed to serialize value: {}", e))?;
		let hash = xxh3_128(&value_bytes).0.to_be_bytes();

		// 2. Check if value already exists (lookup by hash)
		let entry_key = DictionaryEntryKey::encoded(dictionary.id, hash);
		if let Some(existing) = self.get(&entry_key)? {
			// Value exists, return existing ID
			let id = u128::from_be_bytes(existing.values[..16].try_into().unwrap());
			return DictionaryEntryId::from_u128(id, dictionary.id_type);
		}

		// 3. Value doesn't exist - get next ID from sequence
		let seq_key = DictionarySequenceKey::encoded(dictionary.id);
		let next_id = match self.get(&seq_key)? {
			Some(v) => u128::from_be_bytes(v.values[..16].try_into().unwrap()) + 1,
			None => 1, // First entry
		};

		// 4. Validate the new ID fits in the dictionary's id_type (early check)
		let entry_id = DictionaryEntryId::from_u128(next_id, dictionary.id_type)?;

		// 5. Store the entry (hash -> id + value_bytes)
		let mut entry_value = Vec::with_capacity(16 + value_bytes.len());
		entry_value.extend_from_slice(&next_id.to_be_bytes());
		entry_value.extend_from_slice(&value_bytes);
		self.set(&entry_key, EncodedValues(CowVec::new(entry_value)))?;

		// 6. Store reverse index (id -> value_bytes)
		// Note: DictionaryEntryIndexKey currently uses u64, so we truncate
		// This limits practical dictionary size to u64::MAX entries
		let index_key = DictionaryEntryIndexKey::encoded(dictionary.id, next_id as u64);
		self.set(&index_key, EncodedValues(CowVec::new(value_bytes)))?;

		// 7. Update sequence
		self.set(&seq_key, EncodedValues(CowVec::new(next_id.to_be_bytes().to_vec())))?;

		Ok(entry_id)
	}

	fn get_from_dictionary(
		&mut self,
		dictionary: &DictionaryDef,
		id: DictionaryEntryId,
	) -> crate::Result<Option<Value>> {
		// Note: DictionaryEntryIndexKey currently uses u64, so we truncate
		let index_key = DictionaryEntryIndexKey::new(dictionary.id, id.to_u128() as u64).encode();
		match self.get(&index_key)? {
			Some(v) => {
				let value: Value = postcard::from_bytes(&v.values)
					.map_err(|e| internal_error!("Failed to deserialize value: {}", e))?;
				Ok(Some(value))
			}
			None => Ok(None),
		}
	}

	fn find_in_dictionary(
		&mut self,
		dictionary: &DictionaryDef,
		value: &Value,
	) -> crate::Result<Option<DictionaryEntryId>> {
		let value_bytes =
			postcard::to_stdvec(value).map_err(|e| internal_error!("Failed to serialize value: {}", e))?;
		let hash = xxh3_128(&value_bytes).0.to_be_bytes();

		let entry_key = DictionaryEntryKey::encoded(dictionary.id, hash);
		match self.get(&entry_key)? {
			Some(v) => {
				let id = u128::from_be_bytes(v.values[..16].try_into().unwrap());
				let entry_id = DictionaryEntryId::from_u128(id, dictionary.id_type)?;
				Ok(Some(entry_id))
			}
			None => Ok(None),
		}
	}
}

/// Implementation for StandardTransaction (both Command and Query)
/// This provides read-only access to dictionaries for query operations.
impl DictionaryOperations for StandardTransaction<'_> {
	fn insert_into_dictionary(
		&mut self,
		dictionary: &DictionaryDef,
		value: &Value,
	) -> crate::Result<DictionaryEntryId> {
		// Only command transactions can insert
		match self {
			StandardTransaction::Command(cmd) => cmd.insert_into_dictionary(dictionary, value),
			StandardTransaction::Query(_) => {
				Err(internal_error!("Cannot insert into dictionary during a query transaction").into())
			}
		}
	}

	fn get_from_dictionary(
		&mut self,
		dictionary: &DictionaryDef,
		id: DictionaryEntryId,
	) -> crate::Result<Option<Value>> {
		// Both command and query transactions can read
		let index_key = DictionaryEntryIndexKey::encoded(dictionary.id, id.to_u128() as u64);
		match self.get(&index_key)? {
			Some(v) => {
				let value: Value = postcard::from_bytes(&v.values)
					.map_err(|e| internal_error!("Failed to deserialize value: {}", e))?;
				Ok(Some(value))
			}
			None => Ok(None),
		}
	}

	fn find_in_dictionary(
		&mut self,
		dictionary: &DictionaryDef,
		value: &Value,
	) -> crate::Result<Option<DictionaryEntryId>> {
		// Both command and query transactions can read
		let value_bytes =
			postcard::to_stdvec(value).map_err(|e| internal_error!("Failed to serialize value: {}", e))?;
		let hash = xxh3_128(&value_bytes).0.to_be_bytes();

		let entry_key = DictionaryEntryKey::encoded(dictionary.id, hash);
		match self.get(&entry_key)? {
			Some(v) => {
				let id = u128::from_be_bytes(v.values[..16].try_into().unwrap());
				let entry_id = DictionaryEntryId::from_u128(id, dictionary.id_type)?;
				Ok(Some(entry_id))
			}
			None => Ok(None),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		dictionary::DictionaryDef,
		id::{DictionaryId, NamespaceId},
	};
	use reifydb_type::value::{Value, dictionary::DictionaryEntryId, r#type::Type};

	use super::DictionaryOperations;
	use crate::test_utils::create_test_command_transaction;

	fn test_dictionary() -> DictionaryDef {
		DictionaryDef {
			id: DictionaryId(1),
			namespace: NamespaceId(1),
			name: "test_dict".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint8,
		}
	}

	#[test]
	fn test_insert_into_dictionary() {
		let mut txn = create_test_command_transaction();
		let dict = test_dictionary();
		let value = Value::Utf8("hello".to_string());

		let id = txn.insert_into_dictionary(&dict, &value).unwrap();
		assert_eq!(id, DictionaryEntryId::U8(1)); // First entry gets ID 1
	}

	#[test]
	fn test_insert_duplicate_value() {
		let mut txn = create_test_command_transaction();
		let dict = test_dictionary();
		let value = Value::Utf8("hello".to_string());

		let id1 = txn.insert_into_dictionary(&dict, &value).unwrap();
		let id2 = txn.insert_into_dictionary(&dict, &value).unwrap();

		// Same value should return same ID
		assert_eq!(id1, id2);
		assert_eq!(id1, DictionaryEntryId::U8(1));
	}

	#[test]
	fn test_insert_multiple_values() {
		let mut txn = create_test_command_transaction();
		let dict = test_dictionary();

		let id1 = txn.insert_into_dictionary(&dict, &Value::Utf8("hello".to_string())).unwrap();
		let id2 = txn.insert_into_dictionary(&dict, &Value::Utf8("world".to_string())).unwrap();
		let id3 = txn.insert_into_dictionary(&dict, &Value::Utf8("foo".to_string())).unwrap();

		// Different values get sequential IDs
		assert_eq!(id1, DictionaryEntryId::U8(1));
		assert_eq!(id2, DictionaryEntryId::U8(2));
		assert_eq!(id3, DictionaryEntryId::U8(3));
	}

	#[test]
	fn test_get_from_dictionary() {
		let mut txn = create_test_command_transaction();
		let dict = test_dictionary();
		let value = Value::Utf8("hello".to_string());

		let id = txn.insert_into_dictionary(&dict, &value).unwrap();
		let retrieved = txn.get_from_dictionary(&dict, id).unwrap();

		assert_eq!(retrieved, Some(value));
	}

	#[test]
	fn test_get_nonexistent_id() {
		let mut txn = create_test_command_transaction();
		let dict = test_dictionary();

		// Try to get an ID that doesn't exist
		let retrieved = txn.get_from_dictionary(&dict, DictionaryEntryId::U8(999)).unwrap();
		assert_eq!(retrieved, None);
	}

	#[test]
	fn test_find_in_dictionary() {
		let mut txn = create_test_command_transaction();
		let dict = test_dictionary();
		let value = Value::Utf8("hello".to_string());

		// First insert a value
		let id = txn.insert_into_dictionary(&dict, &value).unwrap();

		// Then find should locate it
		let found = txn.find_in_dictionary(&dict, &value).unwrap();
		assert_eq!(found, Some(id));
	}

	#[test]
	fn test_find_nonexistent_value() {
		let mut txn = create_test_command_transaction();
		let dict = test_dictionary();
		let value = Value::Utf8("not_inserted".to_string());

		// Find without inserting should return None
		let found = txn.find_in_dictionary(&dict, &value).unwrap();
		assert_eq!(found, None);
	}

	#[test]
	fn test_dictionary_with_uint1_id() {
		let mut txn = create_test_command_transaction();
		let dict = DictionaryDef {
			id: DictionaryId(2),
			namespace: NamespaceId(1),
			name: "dict_u1".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint1,
		};

		let id = txn.insert_into_dictionary(&dict, &Value::Utf8("test".to_string())).unwrap();
		assert_eq!(id, DictionaryEntryId::U1(1));
		assert_eq!(id.id_type(), Type::Uint1);
	}

	#[test]
	fn test_dictionary_with_uint2_id() {
		let mut txn = create_test_command_transaction();
		let dict = DictionaryDef {
			id: DictionaryId(3),
			namespace: NamespaceId(1),
			name: "dict_u2".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint2,
		};

		let id = txn.insert_into_dictionary(&dict, &Value::Utf8("test".to_string())).unwrap();
		assert_eq!(id, DictionaryEntryId::U2(1));
		assert_eq!(id.id_type(), Type::Uint2);
	}

	#[test]
	fn test_dictionary_with_uint4_id() {
		let mut txn = create_test_command_transaction();
		let dict = DictionaryDef {
			id: DictionaryId(4),
			namespace: NamespaceId(1),
			name: "dict_u4".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint4,
		};

		let id = txn.insert_into_dictionary(&dict, &Value::Utf8("test".to_string())).unwrap();
		assert_eq!(id, DictionaryEntryId::U4(1));
		assert_eq!(id.id_type(), Type::Uint4);
	}
}
