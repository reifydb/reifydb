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
		let mut row = dictionary::allocate();
		dictionary::set_id(&mut row, u64::from(dictionary));
		dictionary::set_namespace(&mut row, u64::from(namespace));
		dictionary::set_name(&mut row, to_create.name.text());
		dictionary::set_value_type(&mut row, type_tag_byte(&to_create.value_type));
		dictionary::set_id_type(&mut row, type_tag_byte(&to_create.id_type));

		txn.set(&DictionaryKey::encoded(dictionary), row.freeze())?;

		Ok(())
	}

	fn link_dictionary_to_namespace(
		txn: &mut AdminTransaction,
		namespace: NamespaceId,
		dictionary: DictionaryId,
		name: &str,
	) -> Result<()> {
		let mut row = dictionary_namespace::allocate();
		dictionary_namespace::set_id(&mut row, u64::from(dictionary));
		dictionary_namespace::set_name(&mut row, name);

		txn.set(&NamespaceDictionaryKey::encoded(namespace, dictionary), row.freeze())?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_codec::row::catalog::EncodedCatalogRow;
	use reifydb_test_harness::engine::create_test_admin_transaction;
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

		let result = CatalogStore::create_dictionary(&mut txn, to_create.clone()).unwrap();
		assert!(result.id.0 > 0);
		assert_eq!(result.namespace, test_namespace.id());
		assert_eq!(result.name, "test_dict");

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

		let links: Vec<_> = txn
			.range(NamespaceDictionaryKey::full_scan(test_namespace.id()), RangeScope::All, 1024)
			.unwrap()
			.collect::<Result<Vec<_>>>()
			.unwrap();
		assert_eq!(links.len(), 2);

		// Keys are descending, so the later dictionary comes first.
		let link = &links[0];
		let bytes = &link.bytes;
		let id2 = dictionary_namespace::get_id(EncodedCatalogRow::view(bytes));
		assert!(id2 > 0);
		assert_eq!(dictionary_namespace::get_name(EncodedCatalogRow::view(bytes)), "dict2");

		let link = &links[1];
		let bytes = &link.bytes;
		let id1 = dictionary_namespace::get_id(EncodedCatalogRow::view(bytes));
		assert!(id2 > id1);
		assert_eq!(dictionary_namespace::get_name(EncodedCatalogRow::view(bytes)), "dict1");
	}

	#[test]
	fn test_create_dictionary_with_various_types() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = DictionaryToCreate {
			namespace: test_namespace.id(),
			name: Fragment::internal("small_dict"),
			value_type: ValueType::Utf8,
			id_type: ValueType::Uint1,
		};
		let result = CatalogStore::create_dictionary(&mut txn, to_create).unwrap();
		assert_eq!(result.id_type, ValueType::Uint1);

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
