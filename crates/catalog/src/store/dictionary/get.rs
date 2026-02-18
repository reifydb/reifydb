// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::dictionary::DictionaryDef, return_internal_error};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::dictionary::DictionaryId;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn get_dictionary(
		rx: &mut Transaction<'_>,
		dictionary: DictionaryId,
	) -> crate::Result<DictionaryDef> {
		match Self::find_dictionary(rx, dictionary)? {
			Some(dict) => Ok(dict),
			None => return_internal_error!(
				"Dictionary with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				dictionary
			),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::{
		fragment::Fragment,
		value::{dictionary::DictionaryId, r#type::Type},
	};

	use crate::{CatalogStore, store::dictionary::create::DictionaryToCreate, test_utils::ensure_test_namespace};

	#[test]
	fn test_get_dictionary_exists() {
		let mut txn = create_test_admin_transaction();
		let test_namespace = ensure_test_namespace(&mut txn);

		let to_create = DictionaryToCreate {
			namespace: test_namespace.id,
			name: Fragment::internal("test_dict"),
			value_type: Type::Utf8,
			id_type: Type::Uint2,
		};

		let created = CatalogStore::create_dictionary(&mut txn, to_create).unwrap();

		let result = CatalogStore::get_dictionary(&mut Transaction::Admin(&mut txn), created.id).unwrap();

		assert_eq!(result.id, created.id);
		assert_eq!(result.name, "test_dict");
		assert_eq!(result.value_type, Type::Utf8);
		assert_eq!(result.id_type, Type::Uint2);
	}

	#[test]
	fn test_get_dictionary_not_exists() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::get_dictionary(&mut Transaction::Admin(&mut txn), DictionaryId(999));

		assert!(result.is_err());
	}
}
