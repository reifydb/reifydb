// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{DictionaryDef, DictionaryId, QueryTransaction},
	return_internal_error,
};

use crate::CatalogStore;

impl CatalogStore {
	pub async fn get_dictionary(
		rx: &mut impl QueryTransaction,
		dictionary: DictionaryId,
	) -> crate::Result<DictionaryDef> {
		match Self::find_dictionary(rx, dictionary).await? {
			Some(dict) => Ok(dict),
			None => return_internal_error!(
				"Dictionary with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				dictionary
			),
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::DictionaryId;
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::Type;

	use crate::{CatalogStore, store::dictionary::create::DictionaryToCreate, test_utils::ensure_test_namespace};

	#[tokio::test]
	async fn test_get_dictionary_exists() {
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

		let result = CatalogStore::get_dictionary(&mut txn, created.id).await.unwrap();

		assert_eq!(result.id, created.id);
		assert_eq!(result.name, "test_dict");
		assert_eq!(result.value_type, Type::Utf8);
		assert_eq!(result.id_type, Type::Uint2);
	}

	#[tokio::test]
	async fn test_get_dictionary_not_exists() {
		let mut txn = create_test_command_transaction().await;

		let result = CatalogStore::get_dictionary(&mut txn, DictionaryId(999));

		assert!(result.await.is_err());
	}
}
