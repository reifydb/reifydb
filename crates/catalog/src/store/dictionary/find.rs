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
	pub(crate) fn find_dictionary(
		rx: &mut Transaction<'_>,
		dictionary_id: DictionaryId,
	) -> Result<Option<DictionaryDef>> {
		let Some(multi) = rx.get(&DictionaryKey::encoded(dictionary_id))? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = DictionaryId(dictionary::SCHEMA.get_u64(&row, dictionary::ID));
		let namespace = NamespaceId(dictionary::SCHEMA.get_u64(&row, dictionary::NAMESPACE));
		let name = dictionary::SCHEMA.get_utf8(&row, dictionary::NAME).to_string();
		let value_type_ordinal = dictionary::SCHEMA.get_u8(&row, dictionary::VALUE_TYPE);
		let id_type_ordinal = dictionary::SCHEMA.get_u8(&row, dictionary::ID_TYPE);

		Ok(Some(DictionaryDef {
			id,
			namespace,
			name,
			value_type: Type::from_u8(value_type_ordinal),
			id_type: Type::from_u8(id_type_ordinal),
		}))
	}

	pub(crate) fn find_dictionary_by_name(
		rx: &mut Transaction<'_>,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> Result<Option<DictionaryDef>> {
		let name = name.as_ref();
		let mut stream = rx.range(NamespaceDictionaryKey::full_scan(namespace), 1024)?;

		let mut found_dictionary_id = None;
		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row = &multi.values;
			let dictionary_name = dictionary_namespace::SCHEMA.get_utf8(row, dictionary_namespace::NAME);
			if name == dictionary_name {
				found_dictionary_id = Some(DictionaryId(
					dictionary_namespace::SCHEMA.get_u64(row, dictionary_namespace::ID),
				));
				break;
			}
		}

		drop(stream);

		let Some(dictionary_id) = found_dictionary_id else {
			return Ok(None);
		};

		Ok(Some(Self::get_dictionary(rx, dictionary_id)?))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::NamespaceId;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::{
		fragment::Fragment,
		value::{dictionary::DictionaryId, r#type::Type},
	};

	use crate::{
		CatalogStore,
		store::{dictionary::create::DictionaryToCreate, namespace::create::NamespaceToCreate},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_find_dictionary_exists() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = DictionaryToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("test_dict"),
			value_type: Type::Utf8,
			id_type: Type::Uint2,
		};

		let created = CatalogStore::create_dictionary(&mut txn, to_create).unwrap();

		let found = CatalogStore::find_dictionary(&mut Transaction::Admin(&mut txn), created.id)
			.unwrap()
			.expect("Dictionary should exist");

		assert_eq!(found.id, created.id);
		assert_eq!(found.name, created.name);
		assert_eq!(found.namespace, created.namespace);
		assert_eq!(found.value_type, Type::Utf8);
		assert_eq!(found.id_type, Type::Uint2);
	}

	#[test]
	fn test_find_dictionary_not_exists() {
		let mut txn = create_test_admin_transaction();

		let result =
			CatalogStore::find_dictionary(&mut Transaction::Admin(&mut txn), DictionaryId(999)).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_dictionary_by_name_exists() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let to_create = DictionaryToCreate {
			namespace: namespace.id,
			name: Fragment::internal("token_mints"),
			value_type: Type::Utf8,
			id_type: Type::Uint4,
		};

		let created = CatalogStore::create_dictionary(&mut txn, to_create).unwrap();

		let found = CatalogStore::find_dictionary_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace.id,
			"token_mints",
		)
		.unwrap()
		.expect("Should find dictionary by name");

		assert_eq!(found.id, created.id);
		assert_eq!(found.name, "token_mints");
		assert_eq!(found.value_type, Type::Utf8);
		assert_eq!(found.id_type, Type::Uint4);
	}

	#[test]
	fn test_find_dictionary_by_name_not_exists() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let result = CatalogStore::find_dictionary_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace.id,
			"nonexistent_dict",
		)
		.unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_dictionary_by_name_different_namespace() {
		let mut txn = create_test_admin_transaction();
		let namespace1 = ensure_test_namespace(&mut txn);

		// Create namespace2
		let namespace2 = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "namespace2".to_string(),
				parent_id: NamespaceId::ROOT,
			},
		)
		.unwrap();

		// Create dictionary in namespace1
		let to_create = DictionaryToCreate {
			namespace: namespace1.id,
			name: Fragment::internal("shared_name"),
			value_type: Type::Utf8,
			id_type: Type::Uint2,
		};

		CatalogStore::create_dictionary(&mut txn, to_create).unwrap();

		// Try to find in namespace2 - should not exist
		let result = CatalogStore::find_dictionary_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace2.id,
			"shared_name",
		)
		.unwrap();

		assert!(result.is_none());

		// Find in namespace1 - should exist
		let found = CatalogStore::find_dictionary_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace1.id,
			"shared_name",
		)
		.unwrap();

		assert!(found.is_some());
	}
}
