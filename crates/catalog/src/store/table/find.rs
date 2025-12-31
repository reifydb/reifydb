// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{
	MultiVersionValues, NamespaceId, NamespaceTableKey, QueryTransaction, TableDef, TableId, TableKey,
};

use crate::{
	CatalogStore,
	store::table::layout::{table, table_namespace},
};

impl CatalogStore {
	pub async fn find_table(rx: &mut impl QueryTransaction, table: TableId) -> crate::Result<Option<TableDef>> {
		let Some(multi) = rx.get(&TableKey::encoded(table)).await? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = TableId(table::LAYOUT.get_u64(&row, table::ID));
		let namespace = NamespaceId(table::LAYOUT.get_u64(&row, table::NAMESPACE));
		let name = table::LAYOUT.get_utf8(&row, table::NAME).to_string();

		Ok(Some(TableDef {
			id,
			name,
			namespace,
			columns: Self::list_columns(rx, id).await?,
			primary_key: Self::find_primary_key(rx, id).await?,
		}))
	}

	pub async fn find_table_by_name(
		rx: &mut impl QueryTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<TableDef>> {
		let name = name.as_ref();
		let batch = rx.range(NamespaceTableKey::full_scan(namespace)).await?;
		let Some(table) = batch.items.iter().find_map(|multi: &MultiVersionValues| {
			let row = &multi.values;
			let table_name = table_namespace::LAYOUT.get_utf8(row, table_namespace::NAME);
			if name == table_name {
				Some(TableId(table_namespace::LAYOUT.get_u64(row, table_namespace::ID)))
			} else {
				None
			}
		}) else {
			return Ok(None);
		};

		Ok(Some(Self::get_table(rx, table).await?))
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

		let result = CatalogStore::find_table_by_name(&mut txn, NamespaceId(1027), "table_two")
			.await
			.unwrap()
			.unwrap();
		assert_eq!(result.id, TableId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "table_two");
	}

	#[tokio::test]
	async fn test_empty() {
		let mut txn = create_test_command_transaction().await;

		let result = CatalogStore::find_table_by_name(&mut txn, NamespaceId(1025), "some_table").await.unwrap();
		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_not_found_different_table() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_namespace(&mut txn).await;
		create_namespace(&mut txn, "namespace_one").await;
		create_namespace(&mut txn, "namespace_two").await;
		create_namespace(&mut txn, "namespace_three").await;

		create_table(&mut txn, "namespace_one", "table_one", &[]).await;
		create_table(&mut txn, "namespace_two", "table_two", &[]).await;
		create_table(&mut txn, "namespace_three", "table_three", &[]).await;

		let result =
			CatalogStore::find_table_by_name(&mut txn, NamespaceId(1025), "table_four_two").await.unwrap();
		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_not_found_different_namespace() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_namespace(&mut txn).await;
		create_namespace(&mut txn, "namespace_one").await;
		create_namespace(&mut txn, "namespace_two").await;
		create_namespace(&mut txn, "namespace_three").await;

		create_table(&mut txn, "namespace_one", "table_one", &[]).await;
		create_table(&mut txn, "namespace_two", "table_two", &[]).await;
		create_table(&mut txn, "namespace_three", "table_three", &[]).await;

		let result = CatalogStore::find_table_by_name(&mut txn, NamespaceId(2), "table_two").await.unwrap();
		assert!(result.is_none());
	}
}
