// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::key::{catalog::DictionaryKey, namespace::NamespaceDictionaryKey};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::dictionary::DictionaryId;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_dictionary(txn: &mut AdminTransaction, dictionary: DictionaryId) -> Result<()> {
		if let Some(dictionary_def) = Self::find_dictionary(&mut Transaction::Admin(&mut *txn), dictionary)? {
			txn.remove(vec![(
				NamespaceDictionaryKey::new(dictionary_def.namespace, dictionary).into(),
				None,
			)])?;
		}

		txn.remove(vec![(DictionaryKey::new(dictionary).into(), None)])?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use std::slice::from_ref;

	use reifydb_codec::row::bytes::EncodedBytes;
	use reifydb_core::key::catalog::{DictionaryEntryIndexKey, DictionaryEntryKey};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{
		fragment::Fragment,
		util::cowvec::CowVec,
		value::{dictionary::DictionaryId, value_type::ValueType},
	};

	use crate::{CatalogStore, store::dictionary::create::DictionaryToCreate, test_utils::ensure_test_namespace};

	#[test]
	fn test_drop_dictionary() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let created = CatalogStore::create_dictionary(
			&mut txn,
			DictionaryToCreate {
				namespace: namespace.id(),
				name: Fragment::internal("test_dict"),
				value_type: ValueType::Utf8,
				id_type: ValueType::Uint2,
			},
		)
		.unwrap();

		let found = CatalogStore::find_dictionary(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_some());

		CatalogStore::drop_dictionary(&mut txn, created.id).unwrap();

		let found = CatalogStore::find_dictionary(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_drop_nonexistent_dictionary() {
		// Dropping a dictionary that never existed is a no-op, not an error.
		let mut txn = create_test_admin_transaction();

		let non_existent = DictionaryId(999999);
		let result = CatalogStore::drop_dictionary(&mut txn, non_existent);
		assert!(result.is_ok());
	}

	#[test]
	fn test_drop_dictionary_keeps_entries_until_commit() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let dict_def = CatalogStore::create_dictionary(
			&mut txn,
			DictionaryToCreate {
				namespace: namespace.id(),
				name: Fragment::internal("entry_dict"),
				value_type: ValueType::Utf8,
				id_type: ValueType::Uint2,
			},
		)
		.unwrap();

		// Interned entries live in the single store, not the multi store the definition uses.
		let dummy_hash: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
		let dummy_value = vec![42u8, 43u8];
		let next_id: u128 = 1;
		let mut entry_value = Vec::with_capacity(16 + dummy_value.len());
		entry_value.extend_from_slice(&next_id.to_be_bytes());
		entry_value.extend_from_slice(&dummy_value);
		let entry = DictionaryEntryKey::new(dict_def.id, dummy_hash);
		let index = DictionaryEntryIndexKey::new(dict_def.id, next_id);
		let entry_key = entry.encode();
		let index_key = index.encode();
		txn.single
			.with_command([&entry_key, &index_key], |tx| {
				tx.set(vec![(entry.clone(), EncodedBytes(CowVec::new(entry_value.clone())))])?;
				tx.set(vec![(index.clone(), EncodedBytes(CowVec::new(dummy_value.clone())))])
			})
			.unwrap();

		let found = txn.single.with_query([&entry_key], |tx| tx.get(from_ref(&entry_key))).unwrap().remove(0);
		assert!(found.is_some());
		let found = txn.single.with_query([&index_key], |tx| tx.get(from_ref(&index_key))).unwrap().remove(0);
		assert!(found.is_some());

		CatalogStore::drop_dictionary(&mut txn, dict_def.id).unwrap();

		// Entries removed before the commit are lost for good if the drop rolls back.
		let found = txn.single.with_query([&entry_key], |tx| tx.get(from_ref(&entry_key))).unwrap().remove(0);
		assert!(found.is_some());
		let found = txn.single.with_query([&index_key], |tx| tx.get(from_ref(&index_key))).unwrap().remove(0);
		assert!(found.is_some());

		let found = CatalogStore::find_dictionary(&mut Transaction::Admin(&mut txn), dict_def.id).unwrap();
		assert!(found.is_none());
	}
}
