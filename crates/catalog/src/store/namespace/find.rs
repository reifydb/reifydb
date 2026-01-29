// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	encoded::encoded::EncodedValues,
	interface::catalog::{id::NamespaceId, namespace::NamespaceDef},
	key::namespace::NamespaceKey,
};
use reifydb_transaction::transaction::AsTransaction;

use crate::{
	CatalogStore,
	store::namespace::{convert_namespace, schema::namespace},
};

impl CatalogStore {
	pub(crate) fn find_namespace_by_name(
		rx: &mut impl AsTransaction,
		name: impl AsRef<str>,
	) -> crate::Result<Option<NamespaceDef>> {
		let name = name.as_ref();

		// Special case for system namespace - hardcoded with fixed ID
		if name == "system" {
			return Ok(Some(NamespaceDef::system()));
		}

		let mut txn = rx.as_transaction();
		let mut stream = txn.range(NamespaceKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let multi = entry?;
			let row: &EncodedValues = &multi.values;
			let namespace_name = namespace::SCHEMA.get_utf8(row, namespace::NAME);
			if name == namespace_name {
				return Ok(Some(convert_namespace(multi)));
			}
		}

		Ok(None)
	}

	pub(crate) fn find_namespace(
		rx: &mut impl AsTransaction,
		id: NamespaceId,
	) -> crate::Result<Option<NamespaceDef>> {
		// Special case for system namespace - hardcoded with fixed ID
		if id == NamespaceId(1) {
			return Ok(Some(NamespaceDef::system()));
		}

		let mut txn = rx.as_transaction();
		Ok(txn.get(&NamespaceKey::encoded(id))?.map(convert_namespace))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_utils::create_test_admin_transaction;

	use crate::{CatalogStore, store::namespace::NamespaceId, test_utils::create_namespace};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();

		create_namespace(&mut txn, "test_namespace");

		let namespace = CatalogStore::find_namespace_by_name(&mut txn, "test_namespace").unwrap().unwrap();

		assert_eq!(namespace.id, NamespaceId(1025));
		assert_eq!(namespace.name, "test_namespace");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::find_namespace_by_name(&mut txn, "test_namespace").unwrap();

		assert_eq!(result, None);
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_admin_transaction();

		create_namespace(&mut txn, "another_namespace");

		let result = CatalogStore::find_namespace_by_name(&mut txn, "test_namespace").unwrap();
		assert_eq!(result, None);
	}
}
