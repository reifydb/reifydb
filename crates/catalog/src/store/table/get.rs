// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::TableId, table::TableDef},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_type::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_table(rx: &mut Transaction<'_>, table: TableId) -> Result<TableDef> {
		CatalogStore::find_table(rx, table)?.ok_or_else(|| {
			Error(internal!(
				"Table with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				table
			))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{NamespaceId, TableId};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_table, ensure_test_namespace},
	};

	#[test]
	fn test_ok() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_table(&mut txn, "namespace_one", "table_one", &[]);
		create_table(&mut txn, "namespace_two", "table_two", &[]);
		create_table(&mut txn, "namespace_three", "table_three", &[]);

		let result = CatalogStore::get_table(&mut Transaction::Admin(&mut txn), TableId(1026)).unwrap();

		assert_eq!(result.id, TableId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "table_two");
	}

	#[test]
	fn test_not_found() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		create_namespace(&mut txn, "namespace_one");
		create_namespace(&mut txn, "namespace_two");
		create_namespace(&mut txn, "namespace_three");

		create_table(&mut txn, "namespace_one", "table_one", &[]);
		create_table(&mut txn, "namespace_two", "table_two", &[]);
		create_table(&mut txn, "namespace_three", "table_three", &[]);

		let err = CatalogStore::get_table(&mut Transaction::Admin(&mut txn), TableId(42)).unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("TableId(42)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
