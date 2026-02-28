// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{dictionary::DictionaryDef, id::NamespaceId},
	key::{dictionary::DictionaryKey, namespace_dictionary::NamespaceDictionaryKey},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::{dictionary::DictionaryId, r#type::Type};

use crate::{
	CatalogStore, Result,
	store::dictionary::schema::{dictionary, dictionary_namespace},
};

impl CatalogStore {
	/// List all dictionaries in a namespace
	pub(crate) fn list_dictionaries(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
	) -> Result<Vec<DictionaryDef>> {
		// Collect dictionary IDs first to avoid borrow conflict
		let mut dictionary_ids = Vec::new();
		{
			let mut stream = rx.range(NamespaceDictionaryKey::full_scan(namespace), 1024)?;
			while let Some(entry) = stream.next() {
				let multi = entry?;
				let row = &multi.values;
				dictionary_ids.push(DictionaryId(
					dictionary_namespace::SCHEMA.get_u64(row, dictionary_namespace::ID),
				));
			}
		}

		let mut dictionaries = Vec::new();
		for dictionary_id in dictionary_ids {
			if let Some(dictionary) = Self::find_dictionary(rx, dictionary_id)? {
				dictionaries.push(dictionary);
			}
		}

		Ok(dictionaries)
	}

	/// List all dictionaries in the database
	pub(crate) fn list_all_dictionaries(rx: &mut Transaction<'_>) -> Result<Vec<DictionaryDef>> {
		let mut dictionaries = Vec::new();

		let mut stream = rx.range(DictionaryKey::full_scan(), 1024)?;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let id = DictionaryId(dictionary::SCHEMA.get_u64(&row, dictionary::ID));
			let namespace = NamespaceId(dictionary::SCHEMA.get_u64(&row, dictionary::NAMESPACE));
			let name = dictionary::SCHEMA.get_utf8(&row, dictionary::NAME).to_string();
			let value_type_ordinal = dictionary::SCHEMA.get_u8(&row, dictionary::VALUE_TYPE);
			let id_type_ordinal = dictionary::SCHEMA.get_u8(&row, dictionary::ID_TYPE);

			dictionaries.push(DictionaryDef {
				id,
				namespace,
				name,
				value_type: Type::from_u8(value_type_ordinal),
				id_type: Type::from_u8(id_type_ordinal),
			});
		}

		Ok(dictionaries)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::NamespaceId;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::{fragment::Fragment, value::r#type::Type};

	use crate::{
		CatalogStore,
		store::{dictionary::create::DictionaryToCreate, namespace::create::NamespaceToCreate},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_list_dictionaries_empty() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let result = CatalogStore::list_dictionaries(&mut Transaction::Admin(&mut txn), namespace.id).unwrap();

		assert!(result.is_empty());
	}

	#[test]
	fn test_list_dictionaries_multiple() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		// Create multiple dictionaries
		for i in 0..3 {
			let to_create = DictionaryToCreate {
				namespace: namespace.id,
				name: Fragment::internal(format!("dict_{}", i)),
				value_type: Type::Utf8,
				id_type: Type::Uint2,
			};
			CatalogStore::create_dictionary(&mut txn, to_create).unwrap();
		}

		let result = CatalogStore::list_dictionaries(&mut Transaction::Admin(&mut txn), namespace.id).unwrap();

		assert_eq!(result.len(), 3);
	}

	#[test]
	fn test_list_dictionaries_different_namespaces() {
		let mut txn = create_test_admin_transaction();
		let namespace1 = ensure_test_namespace(&mut txn);

		let namespace2 = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "namespace2".to_string(),
				parent_id: NamespaceId::ROOT,
			},
		)
		.unwrap();

		// Create 2 dictionaries in namespace1
		for i in 0..2 {
			let to_create = DictionaryToCreate {
				namespace: namespace1.id,
				name: Fragment::internal(format!("ns1_dict_{}", i)),
				value_type: Type::Utf8,
				id_type: Type::Uint2,
			};
			CatalogStore::create_dictionary(&mut txn, to_create).unwrap();
		}

		// Create 3 dictionaries in namespace2
		for i in 0..3 {
			let to_create = DictionaryToCreate {
				namespace: namespace2.id,
				name: Fragment::internal(format!("ns2_dict_{}", i)),
				value_type: Type::Uint8,
				id_type: Type::Uint4,
			};
			CatalogStore::create_dictionary(&mut txn, to_create).unwrap();
		}

		// Verify namespace1 has 2 dictionaries
		let ns1_dicts =
			CatalogStore::list_dictionaries(&mut Transaction::Admin(&mut txn), namespace1.id).unwrap();
		assert_eq!(ns1_dicts.len(), 2);

		// Verify namespace2 has 3 dictionaries
		let ns2_dicts =
			CatalogStore::list_dictionaries(&mut Transaction::Admin(&mut txn), namespace2.id).unwrap();
		assert_eq!(ns2_dicts.len(), 3);
	}

	#[test]
	fn test_list_all_dictionaries() {
		let mut txn = create_test_admin_transaction();
		let namespace1 = ensure_test_namespace(&mut txn);

		let namespace2 = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "namespace2".to_string(),
				parent_id: NamespaceId::ROOT,
			},
		)
		.unwrap();

		// Create dictionaries in both namespaces
		for i in 0..2 {
			let to_create = DictionaryToCreate {
				namespace: namespace1.id,
				name: Fragment::internal(format!("ns1_dict_{}", i)),
				value_type: Type::Utf8,
				id_type: Type::Uint2,
			};
			CatalogStore::create_dictionary(&mut txn, to_create).unwrap();
		}

		for i in 0..3 {
			let to_create = DictionaryToCreate {
				namespace: namespace2.id,
				name: Fragment::internal(format!("ns2_dict_{}", i)),
				value_type: Type::Uint8,
				id_type: Type::Uint4,
			};
			CatalogStore::create_dictionary(&mut txn, to_create).unwrap();
		}

		// List all dictionaries
		let all_dicts = CatalogStore::list_all_dictionaries(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(all_dicts.len(), 5);
	}
}
