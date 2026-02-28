// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::key::{
	dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey, DictionaryKey, DictionarySequenceKey},
	namespace_dictionary::NamespaceDictionaryKey,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::value::dictionary::DictionaryId;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_dictionary(txn: &mut AdminTransaction, dictionary: DictionaryId) -> Result<()> {
		// First, find the dictionary to get its namespace
		if let Some(dictionary_def) = Self::find_dictionary(&mut Transaction::Admin(&mut *txn), dictionary)? {
			// Remove the namespace-dictionary link (secondary index)
			txn.remove(&NamespaceDictionaryKey::encoded(dictionary_def.namespace, dictionary))?;
		}

		// Clean up dictionary entries (hash -> value)
		let entry_range = DictionaryEntryKey::full_scan(dictionary);
		let mut entry_stream = txn.range(entry_range, 1024)?;
		let mut entry_keys = Vec::new();
		while let Some(entry) = entry_stream.next() {
			entry_keys.push(entry?.key.clone());
		}
		drop(entry_stream);
		for key in entry_keys {
			txn.remove(&key)?;
		}

		// Clean up dictionary entry index (id -> value reverse lookup)
		let index_range = DictionaryEntryIndexKey::full_scan(dictionary);
		let mut index_stream = txn.range(index_range, 1024)?;
		let mut index_keys = Vec::new();
		while let Some(entry) = index_stream.next() {
			index_keys.push(entry?.key.clone());
		}
		drop(index_stream);
		for key in index_keys {
			txn.remove(&key)?;
		}

		// Clean up dictionary sequence
		txn.remove(&DictionarySequenceKey::encoded(dictionary))?;

		// Remove the dictionary definition
		txn.remove(&DictionaryKey::encoded(dictionary))?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		encoded::encoded::EncodedValues,
		key::dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey},
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::{
		fragment::Fragment,
		util::cowvec::CowVec,
		value::{dictionary::DictionaryId, r#type::Type},
	};

	use crate::{CatalogStore, store::dictionary::create::DictionaryToCreate, test_utils::ensure_test_namespace};

	#[test]
	fn test_drop_dictionary() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_dictionary(
			&mut txn,
			DictionaryToCreate {
				namespace: namespace.id,
				name: Fragment::internal("test_dict"),
				value_type: Type::Utf8,
				id_type: Type::Uint2,
			},
		)
		.unwrap();

		// Verify it exists
		let found = CatalogStore::find_dictionary(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_some());

		// Drop it
		CatalogStore::drop_dictionary(&mut txn, created.id).unwrap();

		// Verify it's gone
		let found = CatalogStore::find_dictionary(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_drop_nonexistent_dictionary() {
		let mut txn = create_test_admin_transaction();

		// Dropping a non-existent dictionary should not error
		let non_existent = DictionaryId(999999);
		let result = CatalogStore::drop_dictionary(&mut txn, non_existent);
		assert!(result.is_ok());
	}

	#[test]
	fn test_drop_dictionary_cleans_up_entries() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let dict_def = CatalogStore::create_dictionary(
			&mut txn,
			DictionaryToCreate {
				namespace: namespace.id,
				name: Fragment::internal("entry_dict"),
				value_type: Type::Utf8,
				id_type: Type::Uint2,
			},
		)
		.unwrap();

		// Insert entries using raw key operations (DictionaryOperations is pub(crate) in engine)
		let dummy_hash: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
		let dummy_value = vec![42u8, 43u8];
		let next_id: u128 = 1;
		let mut entry_value = Vec::with_capacity(16 + dummy_value.len());
		entry_value.extend_from_slice(&next_id.to_be_bytes());
		entry_value.extend_from_slice(&dummy_value);
		txn.set(&DictionaryEntryKey::encoded(dict_def.id, dummy_hash), EncodedValues(CowVec::new(entry_value)))
			.unwrap();
		txn.set(
			&DictionaryEntryIndexKey::encoded(dict_def.id, next_id as u64),
			EncodedValues(CowVec::new(dummy_value)),
		)
		.unwrap();

		// Verify entry exists before drop
		let found = txn.get(&DictionaryEntryKey::encoded(dict_def.id, dummy_hash)).unwrap();
		assert!(found.is_some());
		let found = txn.get(&DictionaryEntryIndexKey::encoded(dict_def.id, 1u64)).unwrap();
		assert!(found.is_some());

		// Drop the dictionary
		CatalogStore::drop_dictionary(&mut txn, dict_def.id).unwrap();

		// Verify entries are cleaned up
		let found = txn.get(&DictionaryEntryKey::encoded(dict_def.id, dummy_hash)).unwrap();
		assert!(found.is_none());
		let found = txn.get(&DictionaryEntryIndexKey::encoded(dict_def.id, 1u64)).unwrap();
		assert!(found.is_none());

		// Verify dictionary itself is gone
		let found = CatalogStore::find_dictionary(&mut Transaction::Admin(&mut txn), dict_def.id).unwrap();
		assert!(found.is_none());
	}
}
