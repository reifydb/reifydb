// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::tag::type_tag_byte;
use reifydb_core::{
	interface::catalog::{dictionary::Dictionary, id::NamespaceId},
	key::{dictionary::DictionaryKey, namespace_dictionary::NamespaceDictionaryKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::{
	fragment::Fragment,
	value::{dictionary::DictionaryId, value_type::ValueType},
};

use crate::{
	CatalogStore, Result,
	error::{CatalogError, CatalogObjectKind},
	store::{
		dictionary::shape::{dictionary, dictionary_namespace},
		sequence::system::SystemSequence,
	},
};

#[derive(Debug, Clone)]
pub struct DictionaryToCreate {
	pub name: Fragment,
	pub namespace: NamespaceId,
	pub value_type: ValueType,
	pub id_type: ValueType,
}

impl CatalogStore {
	pub(crate) fn create_dictionary(
		txn: &mut AdminTransaction,
		to_create: DictionaryToCreate,
	) -> Result<Dictionary> {
		let namespace_id = to_create.namespace;

		if let Some(dictionary) = CatalogStore::find_dictionary_by_name(
			&mut Transaction::Admin(&mut *txn),
			namespace_id,
			to_create.name.text(),
		)? {
			let namespace = CatalogStore::get_namespace(&mut Transaction::Admin(&mut *txn), namespace_id)?;
			return Err(CatalogError::AlreadyExists {
				kind: CatalogObjectKind::Dictionary,
				namespace: namespace.name().to_string(),
				name: dictionary.name,
				fragment: to_create.name.clone(),
			}
			.into());
		}

		let dictionary_id = SystemSequence::next_dictionary_id(txn)?;

		Self::store_dictionary(txn, dictionary_id, namespace_id, &to_create)?;

		Self::link_dictionary_to_namespace(txn, namespace_id, dictionary_id, to_create.name.text())?;

		Self::get_dictionary(&mut Transaction::Admin(&mut *txn), dictionary_id)
	}

	fn store_dictionary(
		txn: &mut AdminTransaction,
		dictionary: DictionaryId,
		namespace: NamespaceId,
		to_create: &DictionaryToCreate,
	) -> Result<()> {
		let mut row = dictionary::SHAPE.allocate();
		dictionary::SHAPE.set_u64(&mut row, dictionary::ID, dictionary);
		dictionary::SHAPE.set_u64(&mut row, dictionary::NAMESPACE, namespace);
		dictionary::SHAPE.set_utf8(&mut row, dictionary::NAME, to_create.name.text());
		dictionary::SHAPE.set_u8(&mut row, dictionary::VALUE_TYPE, type_tag_byte(&to_create.value_type));
		dictionary::SHAPE.set_u8(&mut row, dictionary::ID_TYPE, type_tag_byte(&to_create.id_type));

		txn.set(&DictionaryKey::encoded(dictionary), row)?;

		Ok(())
	}

	fn link_dictionary_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		dictionary: DictionaryId,
		name: &str,
	) -> Result<()> {
		let mut row = dictionary_namespace::SHAPE.allocate();
		dictionary_namespace::SHAPE.set_u64(&mut row, dictionary_namespace::ID, dictionary);
		dictionary_namespace::SHAPE.set_utf8(&mut row, dictionary_namespace::NAME, name);

		txn.set(&NamespaceDictionaryKey::encoded(namespace, dictionary), row)?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::multi::RangeScope;
	use reifydb_value::{fragment::Fragment, value::value_type::ValueType};

	use super::*;
	use crate::{store::dictionary::shape::dictionary_namespace, test_utils::ensure_test_namespace};

	#[test]
	fn test_create_simple_dictionary() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = DictionaryToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("token_mints"),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint2,
		};

		let result = CatalogStore::create_dictionary(&mut txn, to_create).unwrap();

		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id());
		assert_eq!(result.name, "token_mints");
		assert_eq!(result.value_type, ValueType::Utf8);
		assert_eq!(result.id_type, ValueType::Uint2);
	}

	#[test]
	fn test_create_duplicate_dictionary() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = DictionaryToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("test_dict"),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint4,
		};

		// First creation should succeed
		let result = CatalogStore::create_dictionary(&mut txn, to_create.clone()).unwrap();
		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id());
		assert_eq!(result.name, "test_dict");

		// Second creation should fail with duplicate error
		let err = CatalogStore::create_dictionary(&mut txn, to_create).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_006");
	}

	#[test]
	fn test_dictionary_linked_to_namespace() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create1 = DictionaryToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("dict1"),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint1,
		};

		CatalogStore::create_dictionary(&mut txn, to_create1).unwrap();

		let to_create2 = DictionaryToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("dict2"),
			value_type: ValueType::Uint8,
			id_type: ValueType::Uint2,
		};

		CatalogStore::create_dictionary(&mut txn, to_create2).unwrap();

		// Check namespace links
		let links: Vec<_> = txn
			.range(NamespaceDictionaryKey::full_scan(test_namespace.id()), RangeScope::All, 1024)
			.unwrap()
			.collect::<Result<Vec<_>>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		// Check first link (descending order, so dict2 comes first)
		let link = &links[0];
		let row = &link.row;
		let id2 = dictionary_namespace::SHAPE.get_u64(row, dictionary_namespace::ID);
		assert!(id2 > 0);
		assert_eq!(dictionary_namespace::SHAPE.get_utf8(row, dictionary_namespace::NAME), "dict2");

		// Check second link (dict1 comes second)
		let link = &links[1];
		let row = &link.row;
		let id1 = dictionary_namespace::SHAPE.get_u64(row, dictionary_namespace::ID);
		assert!(id2 > id1);
		assert_eq!(dictionary_namespace::SHAPE.get_utf8(row, dictionary_namespace::NAME), "dict1");
	}

	#[test]
	fn test_create_dictionary_with_various_types() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		// Test with Uint1 ID type
		let to_create = DictionaryToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("small_dict"),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint1,
		};
		let result = CatalogStore::create_dictionary(&mut txn, to_create).unwrap();
		assert_eq!(result.id_type, ValueType::Uint1);

		// Test with Uint8 ID type
		let to_create = DictionaryToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("large_dict"),
			value_type: ValueType::Blob,
			id_type: ValueType::Uint8,
		};
		let result = CatalogStore::create_dictionary(&mut txn, to_create).unwrap();
		assert_eq!(result.id_type, ValueType::Uint8);
		assert_eq!(result.value_type, ValueType::Blob);
	}
}
