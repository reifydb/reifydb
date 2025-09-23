// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{EncodableKey, NamespaceDef, NamespaceId, NamespaceKey, QueryTransaction},
	value::row::EncodedRow,
};

use crate::{
	CatalogStore,
	namespace::{convert_namespace, layout::namespace},
};

impl CatalogStore {
	pub fn find_namespace_by_name(
		rx: &mut impl QueryTransaction,
		name: impl AsRef<str>,
	) -> crate::Result<Option<NamespaceDef>> {
		let name = name.as_ref();

		// Special case for system namespace - hardcoded with fixed ID
		if name == "system" {
			return Ok(Some(NamespaceDef::system()));
		}

		Ok(rx.range(NamespaceKey::full_scan())?.find_map(|multi| {
			let row: &EncodedRow = &multi.row;
			let namespace_name = namespace::LAYOUT.get_utf8(row, namespace::NAME);
			if name == namespace_name {
				Some(convert_namespace(multi))
			} else {
				None
			}
		}))
	}

	pub fn find_namespace(rx: &mut impl QueryTransaction, id: NamespaceId) -> crate::Result<Option<NamespaceDef>> {
		// Special case for system namespace - hardcoded with fixed ID
		if id == NamespaceId(1) {
			return Ok(Some(NamespaceDef::system()));
		}

		Ok(rx.get(&NamespaceKey {
			namespace: id,
		}
		.encode())?
			.map(convert_namespace))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, namespace::NamespaceId, test_utils::create_namespace};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();

		create_namespace(&mut txn, "test_namespace");

		let namespace = CatalogStore::find_namespace_by_name(&mut txn, "test_namespace").unwrap().unwrap();

		assert_eq!(namespace.id, NamespaceId(1025));
		assert_eq!(namespace.name, "test_namespace");
	}

	#[test]
	fn test_empty() {
		let mut txn = create_test_command_transaction();

		let result = CatalogStore::find_namespace_by_name(&mut txn, "test_namespace").unwrap();

		assert_eq!(result, None);
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();

		create_namespace(&mut txn, "another_namespace");

		let result = CatalogStore::find_namespace_by_name(&mut txn, "test_namespace").unwrap();
		assert_eq!(result, None);
	}
}
