// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{DictionaryDef, DictionaryId, NamespaceId, QueryTransaction},
	key::{DictionaryKey, NamespaceDictionaryKey},
};
use reifydb_type::Type;

use crate::{
	CatalogStore,
	store::dictionary::layout::{dictionary, dictionary_namespace},
};

impl CatalogStore {
	/// List all dictionaries in a namespace
	pub async fn list_dictionaries(
		rx: &mut impl QueryTransaction,
		namespace: NamespaceId,
	) -> crate::Result<Vec<DictionaryDef>> {
		// Collect dictionary IDs first to avoid borrow conflict
		let batch = rx.range(NamespaceDictionaryKey::full_scan(namespace)).await?;
		let dictionary_ids: Vec<DictionaryId> = batch
			.items
			.iter()
			.map(|multi| {
				let row = &multi.values;
				DictionaryId(dictionary_namespace::LAYOUT.get_u64(row, dictionary_namespace::ID))
			})
			.collect();

		let mut dictionaries = Vec::new();
		for dictionary_id in dictionary_ids {
			if let Some(dictionary) = Self::find_dictionary(rx, dictionary_id).await? {
				dictionaries.push(dictionary);
			}
		}

		Ok(dictionaries)
	}

	/// List all dictionaries in the database
	pub async fn list_all_dictionaries(rx: &mut impl QueryTransaction) -> crate::Result<Vec<DictionaryDef>> {
		let mut dictionaries = Vec::new();

		let batch = rx.range(DictionaryKey::full_scan()).await?;
		for multi in batch.items {
			let row = &multi.values;
			let id = DictionaryId(dictionary::LAYOUT.get_u64(&row, dictionary::ID));
			let namespace = NamespaceId(dictionary::LAYOUT.get_u64(&row, dictionary::NAMESPACE));
			let name = dictionary::LAYOUT.get_utf8(&row, dictionary::NAME).to_string();
			let value_type_ordinal = dictionary::LAYOUT.get_u8(&row, dictionary::VALUE_TYPE);
			let id_type_ordinal = dictionary::LAYOUT.get_u8(&row, dictionary::ID_TYPE);

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
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::Type;

	use crate::{
		CatalogStore, namespace::NamespaceToCreate, store::dictionary::create::DictionaryToCreate,
		test_utils::ensure_test_namespace,
	};

	#[tokio::test]
	async fn test_list_dictionaries_empty() {
		let mut txn = create_test_command_transaction().await;
		let namespace = ensure_test_namespace(&mut txn).await;

		let result = CatalogStore::list_dictionaries(&mut txn, namespace.id).await.unwrap();

		assert!(result.is_empty());
	}

	#[tokio::test]
	async fn test_list_dictionaries_multiple() {
		let mut txn = create_test_command_transaction().await;
		let namespace = ensure_test_namespace(&mut txn).await;

		// Create multiple dictionaries
		for i in 0..3 {
			let to_create = DictionaryToCreate {
				namespace: namespace.id,
				dictionary: format!("dict_{}", i),
				value_type: Type::Utf8,
				id_type: Type::Uint2,
				fragment: None,
			};
			CatalogStore::create_dictionary(&mut txn, to_create).await.unwrap();
		}

		let result = CatalogStore::list_dictionaries(&mut txn, namespace.id).await.unwrap();

		assert_eq!(result.len(), 3);
	}

	#[tokio::test]
	async fn test_list_dictionaries_different_namespaces() {
		let mut txn = create_test_command_transaction().await;
		let namespace1 = ensure_test_namespace(&mut txn).await;

		let namespace2 = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "namespace2".to_string(),
			},
		)
		.await
		.unwrap();

		// Create 2 dictionaries in namespace1
		for i in 0..2 {
			let to_create = DictionaryToCreate {
				namespace: namespace1.id,
				dictionary: format!("ns1_dict_{}", i),
				value_type: Type::Utf8,
				id_type: Type::Uint2,
				fragment: None,
			};
			CatalogStore::create_dictionary(&mut txn, to_create).await.unwrap();
		}

		// Create 3 dictionaries in namespace2
		for i in 0..3 {
			let to_create = DictionaryToCreate {
				namespace: namespace2.id,
				dictionary: format!("ns2_dict_{}", i),
				value_type: Type::Uint8,
				id_type: Type::Uint4,
				fragment: None,
			};
			CatalogStore::create_dictionary(&mut txn, to_create).await.unwrap();
		}

		// Verify namespace1 has 2 dictionaries
		let ns1_dicts = CatalogStore::list_dictionaries(&mut txn, namespace1.id).await.unwrap();
		assert_eq!(ns1_dicts.len(), 2);

		// Verify namespace2 has 3 dictionaries
		let ns2_dicts = CatalogStore::list_dictionaries(&mut txn, namespace2.id).await.unwrap();
		assert_eq!(ns2_dicts.len(), 3);
	}

	#[tokio::test]
	async fn test_list_all_dictionaries() {
		let mut txn = create_test_command_transaction().await;
		let namespace1 = ensure_test_namespace(&mut txn).await;

		let namespace2 = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "namespace2".to_string(),
			},
		)
		.await
		.unwrap();

		// Create dictionaries in both namespaces
		for i in 0..2 {
			let to_create = DictionaryToCreate {
				namespace: namespace1.id,
				dictionary: format!("ns1_dict_{}", i),
				value_type: Type::Utf8,
				id_type: Type::Uint2,
				fragment: None,
			};
			CatalogStore::create_dictionary(&mut txn, to_create).await.unwrap();
		}

		for i in 0..3 {
			let to_create = DictionaryToCreate {
				namespace: namespace2.id,
				dictionary: format!("ns2_dict_{}", i),
				value_type: Type::Uint8,
				id_type: Type::Uint4,
				fragment: None,
			};
			CatalogStore::create_dictionary(&mut txn, to_create).await.unwrap();
		}

		// List all dictionaries
		let all_dicts = CatalogStore::list_all_dictionaries(&mut txn).await.unwrap();
		assert_eq!(all_dicts.len(), 5);
	}
}
