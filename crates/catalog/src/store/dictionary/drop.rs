// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::store::SingleVersionRange,
	key::{
		dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey, DictionaryKey},
		namespace_dictionary::NamespaceDictionaryKey,
	},
};
use reifydb_transaction::{
	single::SingleTransaction,
	transaction::{Transaction, admin::AdminTransaction},
};
use reifydb_value::value::dictionary::DictionaryId;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_dictionary(txn: &mut AdminTransaction, dictionary: DictionaryId) -> Result<()> {
		if let Some(registry) = txn.dictionary_allocators() {
			registry.begin_drop(dictionary);
		}

		if let Some(dictionary_def) = Self::find_dictionary(&mut Transaction::Admin(&mut *txn), dictionary)? {
			txn.remove(&NamespaceDictionaryKey::encoded(dictionary_def.namespace, dictionary))?;
		}

		remove_dictionary_entries(&txn.single, dictionary)?;

		txn.remove(&DictionaryKey::encoded(dictionary))?;

		if let Some(registry) = txn.dictionary_allocators() {
			registry.evict(dictionary);
		}

		Ok(())
	}
}

fn remove_dictionary_entries(single: &SingleTransaction, dictionary: DictionaryId) -> Result<()> {
	let lock_key = DictionaryKey::encoded(dictionary);
	let full_scans = [DictionaryEntryKey::full_scan(dictionary), DictionaryEntryIndexKey::full_scan(dictionary)];
	for full_scan in &full_scans {
		loop {
			let store = single.read_store();
			let batch = SingleVersionRange::range_batch(&store, full_scan.clone(), 1024)?;
			if batch.items.is_empty() {
				break;
			}
			let mut tx = single.begin_command_ranged([&lock_key], full_scans.to_vec())?;
			for item in &batch.items {
				tx.remove(&item.key)?;
			}
			tx.commit()?;
		}
	}
	Ok(())
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::encoded::row::EncodedRow;
	use reifydb_core::key::dictionary::{DictionaryEntryIndexKey, DictionaryEntryKey};
	use reifydb_engine::test_harness::create_test_admin_transaction;
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
				namespace: namespace.id(),
				name: Fragment::internal("entry_dict"),
				value_type: ValueType::Utf8,
				id_type: ValueType::Uint2,
			},
		)
		.unwrap();

		// Seed entry and index rows directly in the single store, where interned entries live
		let dummy_hash: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
		let dummy_value = vec![42u8, 43u8];
		let next_id: u128 = 1;
		let mut entry_value = Vec::with_capacity(16 + dummy_value.len());
		entry_value.extend_from_slice(&next_id.to_be_bytes());
		entry_value.extend_from_slice(&dummy_value);
		let entry_key = DictionaryEntryKey::encoded(dict_def.id, dummy_hash);
		let index_key = DictionaryEntryIndexKey::encoded(dict_def.id, next_id);
		txn.single
			.with_command([&entry_key, &index_key], |tx| {
				tx.set(&entry_key, EncodedRow(CowVec::new(entry_value.clone())))?;
				tx.set(&index_key, EncodedRow(CowVec::new(dummy_value.clone())))
			})
			.unwrap();

		// Verify entries exist in the single store before drop
		let found = txn.single.with_query([&entry_key], |tx| tx.get(&entry_key)).unwrap();
		assert!(found.is_some());
		let found = txn.single.with_query([&index_key], |tx| tx.get(&index_key)).unwrap();
		assert!(found.is_some());

		// Drop the dictionary
		CatalogStore::drop_dictionary(&mut txn, dict_def.id).unwrap();

		// Verify entries are cleaned up from the single store
		let found = txn.single.with_query([&entry_key], |tx| tx.get(&entry_key)).unwrap();
		assert!(found.is_none());
		let found = txn.single.with_query([&index_key], |tx| tx.get(&index_key)).unwrap();
		assert!(found.is_none());

		// Verify dictionary itself is gone
		let found = CatalogStore::find_dictionary(&mut Transaction::Admin(&mut txn), dict_def.id).unwrap();
		assert!(found.is_none());
	}
}
