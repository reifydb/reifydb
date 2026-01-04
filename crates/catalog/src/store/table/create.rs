// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	diagnostic::catalog::table_already_exists,
	interface::{
		ColumnPolicyKind, DictionaryId, NamespaceId, NamespaceTableKey, PrimitiveId, TableDef, TableId,
		TableKey,
	},
	retention::RetentionPolicy,
	return_error,
};
use reifydb_transaction::StandardCommandTransaction;
use reifydb_type::{Fragment, TypeConstraint};

use crate::{
	CatalogStore,
	store::{
		column::{ColumnIndex, ColumnToCreate},
		retention_policy::create::create_primitive_retention_policy,
		sequence::SystemSequence,
		table::layout::{table, table_namespace},
	},
};

#[derive(Debug, Clone)]
pub struct TableColumnToCreate {
	pub name: String,
	pub constraint: TypeConstraint,
	pub policies: Vec<ColumnPolicyKind>,
	pub auto_increment: bool,
	pub fragment: Option<Fragment>,
	pub dictionary_id: Option<DictionaryId>,
}

#[derive(Debug, Clone)]
pub struct TableToCreate {
	pub fragment: Option<Fragment>,
	pub table: String,
	pub namespace: NamespaceId,
	pub columns: Vec<TableColumnToCreate>,
	pub retention_policy: Option<RetentionPolicy>,
}

impl CatalogStore {
	pub async fn create_table(
		txn: &mut StandardCommandTransaction,
		to_create: TableToCreate,
	) -> crate::Result<TableDef> {
		let namespace_id = to_create.namespace;

		if let Some(table) = CatalogStore::find_table_by_name(txn, namespace_id, &to_create.table).await? {
			let namespace = CatalogStore::get_namespace(txn, namespace_id).await?;
			return_error!(table_already_exists(
				to_create.fragment.unwrap_or_else(|| Fragment::None),
				&namespace.name,
				&table.name
			));
		}

		let table_id = SystemSequence::next_table_id(txn).await?;
		Self::store_table(txn, table_id, namespace_id, &to_create).await?;
		Self::link_table_to_namespace(txn, namespace_id, table_id, &to_create.table).await?;

		if let Some(retention_policy) = &to_create.retention_policy {
			create_primitive_retention_policy(txn, PrimitiveId::Table(table_id), retention_policy).await?;
		}

		Self::insert_columns(txn, table_id, to_create).await?;

		Ok(Self::get_table(txn, table_id).await?)
	}

	async fn store_table(
		txn: &mut StandardCommandTransaction,
		table: TableId,
		namespace: NamespaceId,
		to_create: &TableToCreate,
	) -> crate::Result<()> {
		let mut row = table::LAYOUT.allocate();
		table::LAYOUT.set_u64(&mut row, table::ID, table);
		table::LAYOUT.set_u64(&mut row, table::NAMESPACE, namespace);
		table::LAYOUT.set_utf8(&mut row, table::NAME, &to_create.table);

		// Initialize with no primary key
		table::LAYOUT.set_u64(&mut row, table::PRIMARY_KEY, 0u64);

		txn.set(&TableKey::encoded(table), row).await?;

		Ok(())
	}

	async fn link_table_to_namespace(
		txn: &mut StandardCommandTransaction,
		namespace: NamespaceId,
		table: TableId,
		name: &str,
	) -> crate::Result<()> {
		let mut row = table_namespace::LAYOUT.allocate();
		table_namespace::LAYOUT.set_u64(&mut row, table_namespace::ID, table);
		table_namespace::LAYOUT.set_utf8(&mut row, table_namespace::NAME, name);
		txn.set(&NamespaceTableKey::encoded(namespace, table), row).await?;
		Ok(())
	}

	async fn insert_columns(
		txn: &mut StandardCommandTransaction,
		table: TableId,
		to_create: TableToCreate,
	) -> crate::Result<()> {
		// Look up namespace name for error messages
		let namespace_name = Self::find_namespace(txn, to_create.namespace)
			.await?
			.map(|s| s.name)
			.unwrap_or_else(|| format!("namespace_{}", to_create.namespace));

		for (idx, column_to_create) in to_create.columns.into_iter().enumerate() {
			Self::create_column(
				txn,
				table,
				ColumnToCreate {
					fragment: column_to_create.fragment.clone(),
					namespace_name: namespace_name.clone(),
					table,
					table_name: to_create.table.clone(),
					column: column_to_create.name,
					constraint: column_to_create.constraint.clone(),
					if_not_exists: false,
					policies: column_to_create.policies.clone(),
					index: ColumnIndex(idx as u8),
					auto_increment: column_to_create.auto_increment,
					dictionary_id: column_to_create.dictionary_id,
				},
			)
			.await?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use futures_util::TryStreamExt;
	use reifydb_core::interface::{NamespaceId, NamespaceTableKey, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		store::table::{TableToCreate, layout::table_namespace},
		test_utils::ensure_test_namespace,
	};

	#[tokio::test]
	async fn test_create_table() {
		let mut txn = create_test_command_transaction().await;

		let test_namespace = ensure_test_namespace(&mut txn).await;

		let to_create = TableToCreate {
			namespace: test_namespace.id,
			table: "test_table".to_string(),
			columns: vec![],
			fragment: None,
			retention_policy: None,
		};

		// First creation should succeed
		let result = CatalogStore::create_table(&mut txn, to_create.clone()).await.unwrap();
		assert_eq!(result.id, TableId(1025));
		assert_eq!(result.namespace, NamespaceId(1025));
		assert_eq!(result.name, "test_table");

		let err = CatalogStore::create_table(&mut txn, to_create).await.unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_003");
	}

	#[tokio::test]
	async fn test_table_linked_to_namespace() {
		let mut txn = create_test_command_transaction().await;
		let test_namespace = ensure_test_namespace(&mut txn).await;

		let to_create = TableToCreate {
			namespace: test_namespace.id,
			table: "test_table".to_string(),
			columns: vec![],
			fragment: None,
			retention_policy: None,
		};

		CatalogStore::create_table(&mut txn, to_create).await.unwrap();

		let to_create = TableToCreate {
			namespace: test_namespace.id,
			table: "another_table".to_string(),
			columns: vec![],
			fragment: None,
			retention_policy: None,
		};

		CatalogStore::create_table(&mut txn, to_create).await.unwrap();

		let links: Vec<_> = txn
			.range(NamespaceTableKey::full_scan(test_namespace.id), 1024)
			.unwrap()
			.try_collect::<Vec<_>>()
			.await
			.unwrap();
		assert_eq!(links.len(), 2);

		let link = &links[1];
		let row = &link.values;
		assert_eq!(table_namespace::LAYOUT.get_u64(row, table_namespace::ID), 1025);
		assert_eq!(table_namespace::LAYOUT.get_utf8(row, table_namespace::NAME), "test_table");

		let link = &links[0];
		let row = &link.values;
		assert_eq!(table_namespace::LAYOUT.get_u64(row, table_namespace::ID), 1026);
		assert_eq!(table_namespace::LAYOUT.get_utf8(row, table_namespace::NAME), "another_table");
	}
}
