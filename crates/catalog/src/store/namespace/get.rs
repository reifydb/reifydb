// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::catalog::{id::NamespaceId, namespace::NamespaceDef};
use reifydb_transaction::standard::IntoStandardTransaction;
use reifydb_type::{error::Error, internal};

use crate::CatalogStore;

impl CatalogStore {
	pub fn get_namespace(
		rx: &mut impl IntoStandardTransaction,
		namespace: NamespaceId,
	) -> crate::Result<NamespaceDef> {
		CatalogStore::find_namespace(rx, namespace)?.ok_or_else(|| {
			Error(internal!(
				"Namespace with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				namespace
			))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, store::namespace::NamespaceId, test_utils::create_namespace};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();

		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		let result = CatalogStore::get_namespace(&mut txn, NamespaceId(1026)).unwrap();

		assert_eq!(result.id, NamespaceId(1026));
		assert_eq!(result.name, "namespace_two");
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();

		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		let err = CatalogStore::get_namespace(&mut txn, NamespaceId(23)).unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("NamespaceId(23)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
