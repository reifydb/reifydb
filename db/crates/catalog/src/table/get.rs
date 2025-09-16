// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{QueryTransaction, TableDef, TableId},
};
use reifydb_type::internal_error;

use crate::CatalogStore;

impl CatalogStore {
	pub fn get_table(rx: &mut impl QueryTransaction, table: TableId) -> crate::Result<TableDef> {
		CatalogStore::find_table(rx, table)?.ok_or_else(|| {
			Error(internal_error!(
				"Table with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				table
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{NamespaceId, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_table, ensure_test_namespace},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_table(&mut txn, "namespace_one", "table_one", &[]);
		create_table(&mut txn, "namespace_two", "table_two", &[]);
		create_table(&mut txn, "namespace_three", "table_three", &[]);

		let result = CatalogStore::get_table(&mut txn, TableId(1026)).unwrap();

		assert_eq!(result.id, TableId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "table_two");
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_command_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_table(&mut txn, "namespace_one", "table_one", &[]);
		create_table(&mut txn, "namespace_two", "table_two", &[]);
		create_table(&mut txn, "namespace_three", "table_three", &[]);

		let err = CatalogStore::get_table(&mut txn, TableId(42)).unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("TableId(42)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
