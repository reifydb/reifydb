// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::TableId, shape::ShapeId},
	key::{namespace_table::NamespaceTableKey, table::TableKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result, store::shape::drop::drop_shape_metadata};

impl CatalogStore {
	pub(crate) fn drop_table(txn: &mut AdminTransaction, table: TableId) -> Result<()> {
		// First, find the table to get its namespace
		if let Some(table_def) = Self::find_table(&mut Transaction::Admin(&mut *txn), table)? {
			// Delete the namespace-table link (secondary index)
			txn.remove(&NamespaceTableKey::encoded(table_def.namespace, table))?;
		}

		// Clean up all associated metadata (columns, policies, sequences, pk, retention)
		let pk_id = Self::get_table_pk_id(&mut Transaction::Admin(&mut *txn), table)?;
		drop_shape_metadata(txn, ShapeId::Table(table), pk_id)?;

		// Delete the table metadata
		txn.remove(&TableKey::encoded(table))?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		interface::catalog::{
			id::{NamespaceId, TableId},
			shape::ShapeId,
		},
		retention::RetentionStrategy,
	};
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, r#type::Type},
	};

	use crate::{
		CatalogStore,
		store::{
			namespace::create::NamespaceToCreate,
			retention_strategy::create::create_shape_retention_strategy,
			table::create::{TableColumnToCreate, TableToCreate},
		},
		test_utils::{create_table, ensure_test_namespace},
	};

	#[test]
	fn test_drop_table() {
		let mut txn = create_test_admin_transaction();

		// Create a namespace first
		let namespace = CatalogStore::create_namespace(
			&mut txn,
			NamespaceToCreate {
				namespace_fragment: Some(Fragment::internal("test_ns")),
				name: "test_ns".to_string(),
				local_name: "test_ns".to_string(),
				parent_id: NamespaceId::ROOT,
				grpc: None,
				token: None,
			},
		)
		.unwrap();

		// Create a table
		let created = CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				name: Fragment::internal("test_table"),
				namespace: namespace.id(),
				columns: vec![],
				retention_strategy: None,
				underlying: false,
			},
		)
		.unwrap();

		// Verify it exists
		let found = CatalogStore::find_table_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace.id(),
			"test_table",
		)
		.unwrap();
		assert!(found.is_some());

		// Delete it
		CatalogStore::drop_table(&mut txn, created.id).unwrap();

		// Verify it's gone
		let found = CatalogStore::find_table_by_name(
			&mut Transaction::Admin(&mut txn),
			namespace.id(),
			"test_table",
		)
		.unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_drop_nonexistent_table() {
		let mut txn = create_test_admin_transaction();

		// Deleting a non-existent table should not error
		let non_existent = TableId(999999);
		let result = CatalogStore::drop_table(&mut txn, non_existent);
		assert!(result.is_ok());
	}

	#[test]
	fn test_drop_table_cleans_up_metadata() {
		let mut txn = create_test_admin_transaction();
		let ns = ensure_test_namespace(&mut txn);

		// Create a table with 2 columns
		let table = create_table(
			&mut txn,
			"test_namespace",
			"meta_table",
			&[
				TableColumnToCreate {
					name: Fragment::internal("col_a"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Int4),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
				TableColumnToCreate {
					name: Fragment::internal("col_b"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Utf8),
					properties: vec![],
					auto_increment: false,
					dictionary_id: None,
				},
			],
		);

		// Add retention strategy
		create_shape_retention_strategy(&mut txn, ShapeId::Table(table.id), &RetentionStrategy::KeepForever)
			.unwrap();

		// Verify columns exist before drop
		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), table.id).unwrap();
		assert_eq!(columns.len(), 2);

		// Verify retention strategy exists before drop
		let policy = CatalogStore::find_shape_retention_strategy(
			&mut Transaction::Admin(&mut txn),
			ShapeId::Table(table.id),
		)
		.unwrap();
		assert!(policy.is_some());

		// Drop the table
		CatalogStore::drop_table(&mut txn, table.id).unwrap();

		// Verify columns are cleaned up
		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), table.id).unwrap();
		assert!(columns.is_empty());

		// Verify retention strategy is cleaned up
		let policy = CatalogStore::find_shape_retention_strategy(
			&mut Transaction::Admin(&mut txn),
			ShapeId::Table(table.id),
		)
		.unwrap();
		assert!(policy.is_none());

		// Verify table itself is gone
		let found = CatalogStore::find_table_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "meta_table")
			.unwrap();
		assert!(found.is_none());
	}
}
