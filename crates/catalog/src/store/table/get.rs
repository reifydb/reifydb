// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	Error,
	interface::{TableDef, TableId},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::internal;

use crate::CatalogStore;

impl CatalogStore {
	pub async fn get_table(rx: &mut impl IntoStandardTransaction, table: TableId) -> crate::Result<TableDef> {
		CatalogStore::find_table(rx, table).await?.ok_or_else(|| {
			Error(internal!(
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

	#[tokio::test]
	async fn test_ok() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_namespace(&mut txn).await;
		create_namespace(&mut txn, "namespace_one").await;
		create_namespace(&mut txn, "namespace_two").await;
		create_namespace(&mut txn, "namespace_three").await;

		create_table(&mut txn, "namespace_one", "table_one", &[]).await;
		create_table(&mut txn, "namespace_two", "table_two", &[]).await;
		create_table(&mut txn, "namespace_three", "table_three", &[]).await;

		let result = CatalogStore::get_table(&mut txn, TableId(1026)).await.unwrap();

		assert_eq!(result.id, TableId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "table_two");
	}

	#[tokio::test]
	async fn test_not_found() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_namespace(&mut txn).await;
		create_namespace(&mut txn, "namespace_one").await;
		create_namespace(&mut txn, "namespace_two").await;
		create_namespace(&mut txn, "namespace_three").await;

		create_table(&mut txn, "namespace_one", "table_one", &[]).await;
		create_table(&mut txn, "namespace_two", "table_two", &[]).await;
		create_table(&mut txn, "namespace_three", "table_three", &[]).await;

		let err = CatalogStore::get_table(&mut txn, TableId(42)).await.unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("TableId(42)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
