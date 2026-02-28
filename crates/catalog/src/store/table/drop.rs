// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::TableId, primitive::PrimitiveId},
	key::{namespace_table::NamespaceTableKey, table::TableKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result, store::primitive::drop::drop_primitive_metadata};

impl CatalogStore {
	pub(crate) fn drop_table(txn: &mut AdminTransaction, table: TableId) -> Result<()> {
		// First, find the table to get its namespace
		if let Some(table_def) = Self::find_table(&mut Transaction::Admin(&mut *txn), table)? {
			// Delete the namespace-table link (secondary index)
			txn.remove(&NamespaceTableKey::encoded(table_def.namespace, table))?;
		}

		// Clean up all associated metadata (columns, policies, sequences, pk, retention)
		let pk_id = Self::get_table_pk_id(&mut Transaction::Admin(&mut *txn), table)?;
		drop_primitive_metadata(txn, PrimitiveId::Table(table), pk_id)?;

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
			primitive::PrimitiveId,
		},
		retention::RetentionPolicy,
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, r#type::Type},
	};

	use crate::{
		CatalogStore,
		store::{
			namespace::create::NamespaceToCreate,
			retention_policy::create::create_primitive_retention_policy,
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
				namespace_fragment: Some(Fragment::internal("test_ns".to_string())),
				name: "test_ns".to_string(),
				parent_id: NamespaceId::ROOT,
			},
		)
		.unwrap();

		// Create a table
		let created = CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				name: Fragment::internal("test_table"),
				namespace: namespace.id,
				columns: vec![],
				retention_policy: None,
			},
		)
		.unwrap();

		// Verify it exists
		let found =
			CatalogStore::find_table_by_name(&mut Transaction::Admin(&mut txn), namespace.id, "test_table")
				.unwrap();
		assert!(found.is_some());

		// Delete it
		CatalogStore::drop_table(&mut txn, created.id).unwrap();

		// Verify it's gone
		let found =
			CatalogStore::find_table_by_name(&mut Transaction::Admin(&mut txn), namespace.id, "test_table")
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

		// Add retention policy
		create_primitive_retention_policy(
			&mut txn,
			PrimitiveId::Table(table.id),
			&RetentionPolicy::KeepForever,
		)
		.unwrap();

		// Verify columns exist before drop
		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), table.id).unwrap();
		assert_eq!(columns.len(), 2);

		// Verify retention policy exists before drop
		let policy = CatalogStore::find_primitive_retention_policy(
			&mut Transaction::Admin(&mut txn),
			PrimitiveId::Table(table.id),
		)
		.unwrap();
		assert!(policy.is_some());

		// Drop the table
		CatalogStore::drop_table(&mut txn, table.id).unwrap();

		// Verify columns are cleaned up
		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), table.id).unwrap();
		assert!(columns.is_empty());

		// Verify retention policy is cleaned up
		let policy = CatalogStore::find_primitive_retention_policy(
			&mut Transaction::Admin(&mut txn),
			PrimitiveId::Table(table.id),
		)
		.unwrap();
		assert!(policy.is_none());

		// Verify table itself is gone
		let found = CatalogStore::find_table_by_name(&mut Transaction::Admin(&mut txn), ns.id, "meta_table")
			.unwrap();
		assert!(found.is_none());
	}
}
