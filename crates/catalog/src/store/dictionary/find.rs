// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{DictionaryDef, DictionaryId, MultiVersionValues, NamespaceId, QueryTransaction},
	key::{DictionaryKey, NamespaceDictionaryKey},
};
use reifydb_type::Type;

use crate::{
	CatalogStore,
	store::dictionary::layout::{dictionary, dictionary_namespace},
};

impl CatalogStore {
	pub fn find_dictionary(
		rx: &mut impl QueryTransaction,
		dictionary_id: DictionaryId,
	) -> crate::Result<Option<DictionaryDef>> {
		let Some(multi) = rx.get(&DictionaryKey::encoded(dictionary_id))? else {
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

	pub fn find_dictionary_by_name(
		rx: &mut impl QueryTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<DictionaryDef>> {
		let name = name.as_ref();
		let Some(dictionary_id) = rx.range(NamespaceDictionaryKey::full_scan(namespace))?.find_map(
			|multi: MultiVersionValues| {
				let row = &multi.values;
				let dictionary_name =
					dictionary_namespace::LAYOUT.get_utf8(row, dictionary_namespace::NAME);
				if name == dictionary_name {
					Some(DictionaryId(
						dictionary_namespace::LAYOUT.get_u64(row, dictionary_namespace::ID),
					))
				} else {
					None
				}
			},
		) else {
			return Ok(None);
		};

		Ok(Some(Self::get_dictionary(rx, dictionary_id)?))
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

	#[test]
	fn test_find_dictionary_exists() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = DictionaryToCreate {
			namespace: test_namespace.id,
			dictionary: "test_dict".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint2,
			fragment: None,
		};

		let created = CatalogStore::create_dictionary(&mut txn, to_create).unwrap();

		let found =
			CatalogStore::find_dictionary(&mut txn, created.id).unwrap().expect("Dictionary should exist");

		assert_eq!(found.id, created.id);
		assert_eq!(found.name, created.name);
		assert_eq!(found.namespace, created.namespace);
		assert_eq!(found.value_type, Type::Utf8);
		assert_eq!(found.id_type, Type::Uint2);
	}

	#[test]
	fn test_find_dictionary_not_exists() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::find_dictionary(&mut txn, DictionaryId(999)).unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_dictionary_by_name_exists() {
		let mut txn = create_test_command_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let to_create = DictionaryToCreate {
			namespace: namespace.id,
			dictionary: "token_mints".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint4,
			fragment: None,
		};

		let created = CatalogStore::create_dictionary(&mut txn, to_create).unwrap();

		let found = CatalogStore::find_dictionary_by_name(&mut txn, namespace.id, "token_mints")
			.unwrap()
			.expect("Should find dictionary by name");

		assert_eq!(found.id, created.id);
		assert_eq!(found.name, "token_mints");
		assert_eq!(found.value_type, Type::Utf8);
		assert_eq!(found.id_type, Type::Uint4);
	}

	#[test]
	fn test_find_dictionary_by_name_not_exists() {
		let mut txn = create_test_command_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let result = CatalogStore::find_dictionary_by_name(&mut txn, namespace.id, "nonexistent_dict").unwrap();

		assert!(result.is_none());
	}

	#[test]
	fn test_find_dictionary_by_name_different_namespace() {
		let mut txn = create_test_command_transaction();
		let namespace1 = ensure_test_namespace(&mut txn);

		// Create namespace2
		let namespace2 = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: None,
				name: "namespace2".to_string(),
			},
		)
		.unwrap();

		// Create dictionary in namespace1
		let to_create = DictionaryToCreate {
			namespace: namespace1.id,
			dictionary: "shared_name".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint2,
			fragment: None,
		};

		CatalogStore::create_dictionary(&mut txn, to_create).unwrap();

		// Try to find in namespace2 - should not exist
		let result = CatalogStore::find_dictionary_by_name(&mut txn, namespace2.id, "shared_name").unwrap();

		assert!(result.is_none());

		// Find in namespace1 - should exist
		let found = CatalogStore::find_dictionary_by_name(&mut txn, namespace1.id, "shared_name").unwrap();

		assert!(found.is_some());
	}
}
