// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	diagnostic::catalog::{primary_key_column_not_found, primary_key_empty},
	interface::{ColumnId, CommandTransaction, PrimaryKeyId, PrimaryKeyKey, SourceId},
	return_error, return_internal_error,
};

use crate::{
	CatalogStore,
	store::{
		primary_key::layout::{
			primary_key,
			primary_key::{LAYOUT, serialize_column_ids},
		},
		sequence::SystemSequence,
	},
};

pub struct PrimaryKeyToCreate {
	pub source: SourceId,
	pub column_ids: Vec<ColumnId>,
}

impl CatalogStore {
	pub async fn create_primary_key(
		txn: &mut impl CommandTransaction,
		to_create: PrimaryKeyToCreate,
	) -> crate::Result<PrimaryKeyId> {
		// Validate that primary key has at least one column
		if to_create.column_ids.is_empty() {
			return_error!(primary_key_empty(None));
		}

		// Get the columns for the table/view and validate all primary
		// key columns belong to it
		let source_columns = Self::list_columns(txn, to_create.source).await?;
		let source_column_ids: std::collections::HashSet<_> = source_columns.iter().map(|c| c.id).collect();

		// Validate that all columns belong to the table/view
		for column_id in &to_create.column_ids {
			if !source_column_ids.contains(column_id) {
				return_error!(primary_key_column_not_found(None, column_id.0));
			}
		}

		let id = SystemSequence::next_primary_key_id(txn).await?;

		// Create primary key encoded
		let mut row = LAYOUT.allocate();
		LAYOUT.set_u64(&mut row, primary_key::ID, id.0);
		LAYOUT.set_u64(&mut row, primary_key::SOURCE, to_create.source.as_u64());
		LAYOUT.set_blob(&mut row, primary_key::COLUMN_IDS, &serialize_column_ids(&to_create.column_ids));

		// Store the primary key
		txn.set(&PrimaryKeyKey::encoded(id), row).await?;

		// Update the table or view to reference this primary key
		match to_create.source {
			SourceId::Table(table_id) => {
				Self::set_table_primary_key(txn, table_id, id).await?;
			}
			SourceId::View(view_id) => {
				Self::set_view_primary_key(txn, view_id, id).await?;
			}
			SourceId::Flow(_) => {
				// Flows don't support primary keys
				return_internal_error!(
					"Cannot create primary key for flow. Flows do not support primary keys."
				);
			}
			SourceId::TableVirtual(_) => {
				// Virtual tables don't support primary keys
				return_internal_error!(
					"Cannot create primary key for virtual table. Virtual tables do not support primary keys."
				);
			}
			SourceId::RingBuffer(ringbuffer_id) => {
				Self::set_ringbuffer_primary_key(txn, ringbuffer_id, id).await?;
			}
			SourceId::Dictionary(_) => {
				// Dictionaries don't support traditional primary keys
				return_internal_error!(
					"Cannot create primary key for dictionary. Dictionaries have their own key structure."
				);
			}
		}

		Ok(id)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{ColumnId, PrimaryKeyId, SourceId, TableId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use super::PrimaryKeyToCreate;
	use crate::{
		CatalogStore,
		column::{ColumnIndex, ColumnToCreate},
		table::TableToCreate,
		test_utils::{ensure_test_namespace, ensure_test_table},
		view::{ViewColumnToCreate, ViewToCreate},
	};

	#[tokio::test]
	async fn test_create_primary_key_for_table() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn).await;

		// Create columns for the table
		let col1 = CatalogStore::create_column(
			&mut txn,
			table.id,
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace",
				table: table.id,
				table_name: "test_table",
				column: "id".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.await
		.unwrap();

		let col2 = CatalogStore::create_column(
			&mut txn,
			table.id,
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace",
				table: table.id,
				table_name: "test_table",
				column: "tenant_id".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(1),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.await
		.unwrap();

		// Create primary key
		let primary_key_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				source: SourceId::Table(table.id),
				column_ids: vec![col1.id, col2.id],
			},
		)
		.await
		.unwrap();

		// Verify the primary key was created
		assert_eq!(primary_key_id, PrimaryKeyId(1));

		// Find and verify the primary key
		let found_pk = CatalogStore::find_primary_key(&mut txn, table.id)
			.await
			.unwrap()
			.expect("Primary key should exist");

		assert_eq!(found_pk.id, primary_key_id);
		assert_eq!(found_pk.columns.len(), 2);
		assert_eq!(found_pk.columns[0].id, col1.id);
		assert_eq!(found_pk.columns[0].name, "id");
		assert_eq!(found_pk.columns[1].id, col2.id);
		assert_eq!(found_pk.columns[1].name, "tenant_id");
	}

	#[tokio::test]
	async fn test_create_primary_key_for_view() {
		let mut txn = create_test_command_transaction();
		let namespace = ensure_test_namespace(&mut txn).await;

		// Create a view
		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				fragment: None,
				namespace: namespace.id,
				name: "test_view".to_string(),
				columns: vec![
					ViewColumnToCreate {
						name: "id".to_string(),
						constraint: TypeConstraint::unconstrained(Type::Uint8),
						fragment: None,
					},
					ViewColumnToCreate {
						name: "name".to_string(),
						constraint: TypeConstraint::unconstrained(Type::Utf8),
						fragment: None,
					},
				],
			},
		)
		.await
		.unwrap();

		// Get column IDs for the view
		let columns = CatalogStore::list_columns(&mut txn, view.id).await.unwrap();
		assert_eq!(columns.len(), 2);

		// Create primary key on first column only
		let primary_key_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				source: SourceId::View(view.id),
				column_ids: vec![columns[0].id],
			},
		)
		.await
		.unwrap();

		// Verify the primary key was created
		assert_eq!(primary_key_id, PrimaryKeyId(1));

		// Find and verify the primary key
		let found_pk = CatalogStore::find_primary_key(&mut txn, view.id)
			.await
			.unwrap()
			.expect("Primary key should exist");

		assert_eq!(found_pk.id, primary_key_id);
		assert_eq!(found_pk.columns.len(), 1);
		assert_eq!(found_pk.columns[0].id, columns[0].id);
		assert_eq!(found_pk.columns[0].name, "id");
	}

	#[tokio::test]
	async fn test_create_composite_primary_key() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn).await;

		// Create multiple columns
		let mut column_ids = Vec::new();
		for i in 0..3 {
			let col = CatalogStore::create_column(
				&mut txn,
				table.id,
				ColumnToCreate {
					fragment: None,
					namespace_name: "test_namespace",
					table: table.id,
					table_name: "test_table",
					column: format!("col_{}", i),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					if_not_exists: false,
					policies: vec![],
					index: ColumnIndex(i as u8),
					auto_increment: false,
					dictionary_id: None,
				},
			)
			.await
			.unwrap();
			column_ids.push(col.id);
		}

		// Create composite primary key
		let primary_key_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				source: SourceId::Table(table.id),
				column_ids: column_ids.clone(),
			},
		)
		.await
		.unwrap();

		// Find and verify the primary key
		let found_pk = CatalogStore::find_primary_key(&mut txn, table.id)
			.await
			.unwrap()
			.expect("Primary key should exist");

		assert_eq!(found_pk.id, primary_key_id);
		assert_eq!(found_pk.columns.len(), 3);
		for (i, col) in found_pk.columns.iter().enumerate() {
			assert_eq!(col.id, column_ids[i]);
			assert_eq!(col.name, format!("col_{}", i));
		}
	}

	#[tokio::test]
	async fn test_create_primary_key_updates_table() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn).await;

		// Initially, table does not have primary key
		let initial_pk = CatalogStore::find_primary_key(&mut txn, table.id).await.unwrap();
		assert!(initial_pk.is_none());

		// Create a column
		let col = CatalogStore::create_column(
			&mut txn,
			table.id,
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace",
				table: table.id,
				table_name: "test_table",
				column: "id".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.await
		.unwrap();

		// Create primary key
		let primary_key_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				source: SourceId::Table(table.id),
				column_ids: vec![col.id],
			},
		)
		.await
		.unwrap();

		// Now table should have the primary key
		let updated_pk = CatalogStore::find_primary_key(&mut txn, table.id)
			.await
			.unwrap()
			.expect("Primary key should exist");

		assert_eq!(updated_pk.id, primary_key_id);
	}

	#[tokio::test]
	async fn test_create_primary_key_on_nonexistent_table() {
		let mut txn = create_test_command_transaction();

		// Try to create primary key on non-existent table
		// list_table_columns will return empty list for non-existent
		// table, so the column validation will fail
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				source: SourceId::Table(TableId(999)),
				column_ids: vec![ColumnId(1)],
			},
		)
		.await;

		assert!(result.is_err());
		let err = result.unwrap_err();
		// Fails with CA_021 because column 1 won't be in the empty
		// column list
		assert_eq!(err.code, "CA_021");
	}

	#[tokio::test]
	async fn test_create_primary_key_on_nonexistent_view() {
		let mut txn = create_test_command_transaction();

		// Try to create primary key on non-existent view
		// list_table_columns will return empty list for non-existent
		// view, so the column validation will fail
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				source: SourceId::View(ViewId(999)),
				column_ids: vec![ColumnId(1)],
			},
		)
		.await;

		assert!(result.is_err());
		let err = result.unwrap_err();
		// Fails with CA_021 because column 1 won't be in the empty
		// column list
		assert_eq!(err.code, "CA_021");
	}

	#[tokio::test]
	async fn test_create_empty_primary_key() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn).await;

		// Try to create primary key with no columns - should fail
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				source: SourceId::Table(table.id),
				column_ids: vec![],
			},
		)
		.await;

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "CA_020");
	}

	#[tokio::test]
	async fn test_create_primary_key_with_nonexistent_column() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn).await;

		// Try to create primary key with non-existent column ID
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				source: SourceId::Table(table.id),
				column_ids: vec![ColumnId(999)],
			},
		)
		.await;

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "CA_021");
	}

	#[tokio::test]
	async fn test_create_primary_key_with_column_from_different_table() {
		let mut txn = create_test_command_transaction();
		let table1 = ensure_test_table(&mut txn).await;

		// Create a column for table1
		let _col1 = CatalogStore::create_column(
			&mut txn,
			table1.id,
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace",
				table: table1.id,
				table_name: "test_table",
				column: "id".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.await
		.unwrap();

		// Create another table
		let namespace = CatalogStore::get_namespace(&mut txn, table1.namespace).await.unwrap();
		let table2 = CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				fragment: None,
				table: "test_table2".to_string(),
				namespace: namespace.id,
				columns: vec![],
				retention_policy: None,
			},
		)
		.await
		.unwrap();

		// Create a column for table2
		let col2 = CatalogStore::create_column(
			&mut txn,
			table2.id,
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace",
				table: table2.id,
				table_name: "test_table2",
				column: "id".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.await
		.unwrap();

		// Try to create primary key for table1 using column from table2
		// This must fail because we validate columns belong to the
		// specific table
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				source: SourceId::Table(table1.id),
				column_ids: vec![col2.id],
			},
		)
		.await;

		// Should fail with CA_021 because col2 doesn't belong to table1
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "CA_021");
	}
}
