// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{NamespaceDef, NamespaceId, QueryTransaction},
};
use reifydb_type::internal_error;

use crate::CatalogStore;

impl CatalogStore {
	pub fn get_namespace(
		rx: &mut impl QueryTransaction,
		namespace: NamespaceId,
	) -> crate::Result<NamespaceDef> {
		CatalogStore::find_namespace(rx, namespace)?.ok_or_else(|| {
			Error(internal_error!(
				"Namespace with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				namespace
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore, namespace::NamespaceId,
		test_utils::create_namespace,
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();

		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		let result = CatalogStore::get_namespace(
			&mut txn,
			NamespaceId(1026),
		)
		.unwrap();

		assert_eq!(result.id, NamespaceId(1026));
		assert_eq!(result.name, "namespace_two");
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();

		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		let err =
			CatalogStore::get_namespace(&mut txn, NamespaceId(23))
				.unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("NamespaceId(23)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
