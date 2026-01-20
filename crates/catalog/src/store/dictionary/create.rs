// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::encoded::EncodedValues,
	interface::catalog::{
		dictionary::DictionaryDef,
		id::{DictionaryId, NamespaceId},
	},
	key::{
		dictionary::{DictionaryKey, DictionarySequenceKey},
		namespace_dictionary::NamespaceDictionaryKey,
	},
};
use reifydb_transaction::standard::command::StandardCommandTransaction;
use reifydb_core::error::diagnostic::catalog::dictionary_already_exists;
use reifydb_type::{
	fragment::Fragment, return_error, util::cowvec::CowVec,
	value::r#type::Type,
};

use crate::{CatalogStore, store::sequence::system::SystemSequence};

#[derive(Debug, Clone)]
pub struct DictionaryToCreate {
	pub fragment: Option<Fragment>,
	pub dictionary: String,
	pub namespace: NamespaceId,
	pub value_type: Type,
	pub id_type: Type,
}

impl CatalogStore {
	pub(crate) fn create_dictionary(
		txn: &mut StandardCommandTransaction,
		to_create: DictionaryToCreate,
	) -> crate::Result<DictionaryDef> {
		let namespace_id = to_create.namespace;

		// Check if dictionary already exists
		if let Some(dictionary) =
			CatalogStore::find_dictionary_by_name(txn, namespace_id, &to_create.dictionary)?
		{
			let namespace = CatalogStore::get_namespace(txn, namespace_id)?;
			return_error!(dictionary_already_exists(
				to_create.fragment.unwrap_or_else(|| Fragment::None),
				&namespace.name,
				&dictionary.name
			));
		}

		// Allocate new dictionary ID
		let dictionary_id = SystemSequence::next_dictionary_id(txn)?;

		// Store the dictionary
		Self::store_dictionary(txn, dictionary_id, namespace_id, &to_create)?;

		// Link dictionary to namespace
		Self::link_dictionary_to_namespace(txn, namespace_id, dictionary_id, &to_create.dictionary)?;

		// Initialize dictionary sequence counter to 0
		Self::initialize_dictionary_sequence(txn, dictionary_id)?;

		Ok(Self::get_dictionary(txn, dictionary_id)?)
	}

	fn store_dictionary(
		txn: &mut StandardCommandTransaction,
		dictionary: DictionaryId,
		namespace: NamespaceId,
		to_create: &DictionaryToCreate,
	) -> crate::Result<()> {
		use crate::store::dictionary::schema::dictionary;

		let mut row = dictionary::SCHEMA.allocate();
		dictionary::SCHEMA.set_u64(&mut row, dictionary::ID, dictionary);
		dictionary::SCHEMA.set_u64(&mut row, dictionary::NAMESPACE, namespace);
		dictionary::SCHEMA.set_utf8(&mut row, dictionary::NAME, &to_create.dictionary);
		dictionary::SCHEMA.set_u8(&mut row, dictionary::VALUE_TYPE, to_create.value_type.to_u8());
		dictionary::SCHEMA.set_u8(&mut row, dictionary::ID_TYPE, to_create.id_type.to_u8());

		txn.set(&DictionaryKey::encoded(dictionary), row)?;

		Ok(())
	}

	fn link_dictionary_to_namespace(
		txn: &mut StandardCommandTransaction,
		namespace: NamespaceId,
		dictionary: DictionaryId,
		name: &str,
	) -> crate::Result<()> {
		use crate::store::dictionary::schema::dictionary_namespace;

		let mut row = dictionary_namespace::SCHEMA.allocate();
		dictionary_namespace::SCHEMA.set_u64(&mut row, dictionary_namespace::ID, dictionary);
		dictionary_namespace::SCHEMA.set_utf8(&mut row, dictionary_namespace::NAME, name);

		txn.set(&NamespaceDictionaryKey::encoded(namespace, dictionary), row)?;

		Ok(())
	}

	fn initialize_dictionary_sequence(
		txn: &mut StandardCommandTransaction,
		dictionary: DictionaryId,
	) -> crate::Result<()> {
		// Initialize sequence counter to 0
		// This ensures StorageTracker begins tracking the dictionary immediately
		let seq_key = DictionarySequenceKey::encoded(dictionary);
		let initial_value = 0u128.to_be_bytes().to_vec();

		txn.set(&seq_key, EncodedValues(CowVec::new(initial_value)))?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::value::r#type::Type;

	use super::*;
	use crate::{store::dictionary::schema::dictionary_namespace, test_utils::ensure_test_namespace};

	#[test]
	fn test_create_simple_dictionary() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = DictionaryToCreate {
			namespace: test_namespace.id,
			dictionary: "token_mints".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint2,
			fragment: None,
		};

		let result = CatalogStore::create_dictionary(&mut txn, to_create).unwrap();

		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id);
		assert_eq!(result.name, "token_mints");
		assert_eq!(result.value_type, Type::Utf8);
		assert_eq!(result.id_type, Type::Uint2);
	}

	#[test]
	fn test_create_duplicate_dictionary() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = DictionaryToCreate {
			namespace: test_namespace.id,
			dictionary: "test_dict".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint4,
			fragment: None,
		};

		// First creation should succeed
		let result = CatalogStore::create_dictionary(&mut txn, to_create.clone()).unwrap();
		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id);
		assert_eq!(result.name, "test_dict");

		// Second creation should fail with duplicate error
		let err = CatalogStore::create_dictionary(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_006");
	}

	#[test]
	fn test_dictionary_linked_to_namespace() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create1 = DictionaryToCreate {
			namespace: test_namespace.id,
			dictionary: "dict1".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint1,
			fragment: None,
		};

		CatalogStore::create_dictionary(&mut txn, to_create1).unwrap();

		let to_create2 = DictionaryToCreate {
			namespace: test_namespace.id,
			dictionary: "dict2".to_string(),
			value_type: Type::Uint8,
			id_type: Type::Uint2,
			fragment: None,
		};

		CatalogStore::create_dictionary(&mut txn, to_create2).unwrap();

		// Check namespace links
		let links: Vec<_> = txn
			.range(NamespaceDictionaryKey::full_scan(test_namespace.id), 1024)
			.unwrap()
			.collect::<Result<Vec<_>, _>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		// Check first link (descending order, so dict2 comes first)
		let link = &links[0];
		let row = &link.values;
		let id2 = dictionary_namespace::SCHEMA.get_u64(row, dictionary_namespace::ID);
		assert!(id2 > 0);
		assert_eq!(dictionary_namespace::SCHEMA.get_utf8(row, dictionary_namespace::NAME), "dict2");

		// Check second link (dict1 comes second)
		let link = &links[1];
		let row = &link.values;
		let id1 = dictionary_namespace::SCHEMA.get_u64(row, dictionary_namespace::ID);
		assert!(id2 > id1);
		assert_eq!(dictionary_namespace::SCHEMA.get_utf8(row, dictionary_namespace::NAME), "dict1");
	}

	#[test]
	fn test_create_dictionary_with_various_types() {
		let mut txn = create_test_command_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		// Test with Uint1 ID type
		let to_create = DictionaryToCreate {
			namespace: test_namespace.id,
			dictionary: "small_dict".to_string(),
			value_type: Type::Utf8,
			id_type: Type::Uint1,
			fragment: None,
		};
		let result = CatalogStore::create_dictionary(&mut txn, to_create).unwrap();
		assert_eq!(result.id_type, Type::Uint1);

		// Test with Uint8 ID type
		let to_create = DictionaryToCreate {
			namespace: test_namespace.id,
			dictionary: "large_dict".to_string(),
			value_type: Type::Blob,
			id_type: Type::Uint8,
			fragment: None,
		};
		let result = CatalogStore::create_dictionary(&mut txn, to_create).unwrap();
		assert_eq!(result.id_type, Type::Uint8);
		assert_eq!(result.value_type, Type::Blob);
	}
}
