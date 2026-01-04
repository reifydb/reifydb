// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use futures_util::StreamExt;
use reifydb_core::{
	interface::{DictionaryDef, DictionaryId, NamespaceId},
	key::{DictionaryKey, NamespaceDictionaryKey},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::Type;

use crate::{
	CatalogStore,
	store::dictionary::layout::{dictionary, dictionary_namespace},
};

impl CatalogStore {
	pub async fn find_dictionary(
		rx: &mut impl IntoStandardTransaction,
		dictionary_id: DictionaryId,
	) -> crate::Result<Option<DictionaryDef>> {
		let mut txn = rx.into_standard_transaction();
		let Some(multi) = txn.get(&DictionaryKey::encoded(dictionary_id)).await? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = DictionaryId(dictionary::LAYOUT.get_u64(&row, dictionary::ID));
		let namespace = NamespaceId(dictionary::LAYOUT.get_u64(&row, dictionary::NAMESPACE));
		let name = dictionary::LAYOUT.get_utf8(&row, dictionary::NAME).to_string();
		let value_type_ordinal = dictionary::LAYOUT.get_u8(&row, dictionary::VALUE_TYPE);
		let id_type_ordinal = dictionary::LAYOUT.get_u8(&row, dictionary::ID_TYPE);

		Ok(Some(DictionaryDef {
			id,
			namespace,
			name,
			value_type: Type::from_u8(value_type_ordinal),
			id_type: Type::from_u8(id_type_ordinal),
		}))
	}

	pub async fn find_dictionary_by_name(
		rx: &mut impl IntoStandardTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<DictionaryDef>> {
		let name = name.as_ref();
		let mut txn = rx.into_standard_transaction();
		let mut stream = txn.range(NamespaceDictionaryKey::full_scan(namespace), 1024)?;

		let mut found_dictionary_id = None;
		while let Some(entry) = stream.next().await {
			let multi = entry?;
			let row = &multi.values;
			let dictionary_name = dictionary_namespace::LAYOUT.get_utf8(row, dictionary_namespace::NAME);
			if name == dictionary_name {
				found_dictionary_id = Some(DictionaryId(
					dictionary_namespace::LAYOUT.get_u64(row, dictionary_namespace::ID),
				));
				break;
			}
		}

		drop(stream);

		let Some(dictionary_id) = found_dictionary_id else {
			return Ok(None);
		};

		Ok(Some(Self::get_dictionary(&mut txn, dictionary_id).await?))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::DictionaryId;
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::Type;

	use crate::{
		CatalogStore, namespace::NamespaceToCreate, store::dictionary::create::DictionaryToCreate,
		test_utils::ensure_test_namespace,
	};

	#[tokio::test]
	async fn test_find_dictionary_exists() {
		let mut txn = create_test_command_transaction().await;
		let test_namespace = ensure_test_namespace(&mut txn).await;

		let to_create = DictionaryToCreate {
			namespace: test_namespace.id,
			dictionary: "test_dict".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint2,
			fragment: None,
		};

		let created = CatalogStore::create_dictionary(&mut txn, to_create).await.unwrap();

		let found = CatalogStore::find_dictionary(&mut txn, created.id)
			.await
			.unwrap()
			.expect("Dictionary should exist");

		assert_eq!(found.id, created.id);
		assert_eq!(found.name, created.name);
		assert_eq!(found.namespace, created.namespace);
		assert_eq!(found.value_type, Type::Utf8);
		assert_eq!(found.id_type, Type::Uint2);
	}

	#[tokio::test]
	async fn test_find_dictionary_not_exists() {
		let mut txn = create_test_command_transaction().await;

		let result = CatalogStore::find_dictionary(&mut txn, DictionaryId(999)).await.unwrap();

		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_find_dictionary_by_name_exists() {
		let mut txn = create_test_command_transaction().await;
		let namespace = ensure_test_namespace(&mut txn).await;

		let to_create = DictionaryToCreate {
			namespace: namespace.id,
			dictionary: "token_mints".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint4,
			fragment: None,
		};

		let created = CatalogStore::create_dictionary(&mut txn, to_create).await.unwrap();

		let found = CatalogStore::find_dictionary_by_name(&mut txn, namespace.id, "token_mints")
			.await
			.unwrap()
			.expect("Should find dictionary by name");

		assert_eq!(found.id, created.id);
		assert_eq!(found.name, "token_mints");
		assert_eq!(found.value_type, Type::Utf8);
		assert_eq!(found.id_type, Type::Uint4);
	}

	#[tokio::test]
	async fn test_find_dictionary_by_name_not_exists() {
		let mut txn = create_test_command_transaction().await;
		let namespace = ensure_test_namespace(&mut txn).await;

		let result = CatalogStore::find_dictionary_by_name(&mut txn, namespace.id, "nonexistent_dict")
			.await
			.unwrap();

		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_find_dictionary_by_name_different_namespace() {
		let mut txn = create_test_command_transaction().await;
		let namespace1 = ensure_test_namespace(&mut txn).await;

		// Create namespace2
		let namespace2 = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "namespace2".to_string(),
			},
		)
		.await
		.unwrap();

		// Create dictionary in namespace1
		let to_create = DictionaryToCreate {
			namespace: namespace1.id,
			dictionary: "shared_name".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint2,
			fragment: None,
		};

		CatalogStore::create_dictionary(&mut txn, to_create).await.unwrap();

		// Try to find in namespace2 - should not exist
		let result =
			CatalogStore::find_dictionary_by_name(&mut txn, namespace2.id, "shared_name").await.unwrap();

		assert!(result.is_none());

		// Find in namespace1 - should exist
		let found =
			CatalogStore::find_dictionary_by_name(&mut txn, namespace1.id, "shared_name").await.unwrap();

		assert!(found.is_some());
	}
}
