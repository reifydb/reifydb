// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{NamespaceDef, NamespaceId, NamespaceKey, QueryTransaction},
	value::encoded::EncodedValues,
};

use crate::{
	CatalogStore,
	store::namespace::{convert_namespace, layout::namespace},
};

impl CatalogStore {
	pub async fn find_namespace_by_name(
		rx: &mut impl QueryTransaction,
		name: impl AsRef<str>,
	) -> crate::Result<Option<NamespaceDef>> {
		let name = name.as_ref();

		// Special case for system namespace - hardcoded with fixed ID
		if name == "system" {
			return Ok(Some(NamespaceDef::system()));
		}

		let batch = rx.range(NamespaceKey::full_scan()).await?;
		Ok(batch.items.iter().find_map(|multi| {
			let row: &EncodedValues = &multi.values;
			let namespace_name = namespace::LAYOUT.get_utf8(row, namespace::NAME);
			if name == namespace_name {
				Some(convert_namespace(multi.clone()))
			} else {
				None
			}
		}))
	}

	pub async fn find_namespace(
		rx: &mut impl QueryTransaction,
		id: NamespaceId,
	) -> crate::Result<Option<NamespaceDef>> {
		// Special case for system namespace - hardcoded with fixed ID
		if id == NamespaceId(1) {
			return Ok(Some(NamespaceDef::system()));
		}

		Ok(rx.get(&NamespaceKey::encoded(id)).await?.map(convert_namespace))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, store::namespace::NamespaceId, test_utils::create_namespace};

	#[tokio::test]
	fn test_ok() {
		let mut txn = create_test_command_transaction().await;

		create_namespace(&mut txn, "test_namespace").await;

		let namespace = CatalogStore::find_namespace_by_name(&mut txn, "test_namespace").unwrap().unwrap();

		assert_eq!(namespace.id, NamespaceId(1025));
		assert_eq!(namespace.name, "test_namespace");
	}

	#[tokio::test]
	fn test_empty() {
		let mut txn = create_test_command_transaction().await;

		let result = CatalogStore::find_namespace_by_name(&mut txn, "test_namespace").unwrap();

		assert_eq!(result, None);
	}

	#[tokio::test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction().await;

		create_namespace(&mut txn, "another_namespace").await;

		let result = CatalogStore::find_namespace_by_name(&mut txn, "test_namespace").unwrap();
		assert_eq!(result, None);
	}
}
