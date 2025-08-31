// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use primary_key::LAYOUT;
use reifydb_core::{
	diagnostic::catalog::{
		primary_key_column_not_found, primary_key_empty,
	},
	interface::{
		ColumnId, CommandTransaction, Key, PrimaryKeyId, PrimaryKeyKey,
		StoreId,
	},
	return_error,
};

use crate::{
	CatalogStore,
	primary_key::layout::{primary_key, primary_key::serialize_column_ids},
	sequence::SystemSequence,
};

pub struct PrimaryKeyToCreate {
	pub store: StoreId,
	pub column_ids: Vec<ColumnId>,
}

impl CatalogStore {
	pub fn create_primary_key(
		txn: &mut impl CommandTransaction,
		to_create: PrimaryKeyToCreate,
	) -> crate::Result<PrimaryKeyId> {
		// Validate that primary key has at least one column
		if to_create.column_ids.is_empty() {
			return_error!(primary_key_empty(None));
		}

		// Get the columns for the table/view and validate all primary
		// key columns belong to it
		let store_columns =
			Self::list_table_columns(txn, to_create.store)?;
		let store_column_ids: std::collections::HashSet<_> =
			store_columns.iter().map(|c| c.id).collect();

		// Validate that all columns belong to the table/view
		for column_id in &to_create.column_ids {
			if !store_column_ids.contains(column_id) {
				return_error!(primary_key_column_not_found(
					None,
					column_id.0
				));
			}
		}

		let id = SystemSequence::next_primary_key_id(txn)?;

		// Create primary key row
		let mut row = LAYOUT.allocate_row();
		LAYOUT.set_u64(&mut row, primary_key::ID, id.0);
		LAYOUT.set_u64(
			&mut row,
			primary_key::STORE,
			to_create.store.as_u64(),
		);
		LAYOUT.set_blob(
			&mut row,
			primary_key::COLUMN_IDS,
			&serialize_column_ids(&to_create.column_ids),
		);

		// Store the primary key
		txn.set(
			&Key::PrimaryKey(PrimaryKeyKey {
				primary_key: id,
			})
			.encode(),
			row,
		)?;

		// Update the table or view to reference this primary key
		match to_create.store {
			StoreId::Table(table_id) => {
				Self::set_table_primary_key(txn, table_id, id)?;
			}
			StoreId::View(view_id) => {
				Self::set_view_primary_key(txn, view_id, id)?;
			}
		}

		Ok(id)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		Type,
		interface::{ColumnId, PrimaryKeyId, StoreId, TableId, ViewId},
	};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use super::PrimaryKeyToCreate;
	use crate::{
		CatalogStore,
		column::{ColumnIndex, ColumnToCreate},
		test_utils::{ensure_test_schema, ensure_test_table},
		view::{ViewColumnToCreate, ViewToCreate},
	};

	#[test]
	fn test_create_primary_key_for_table() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn);

		// Create columns for the table
		let col1 = CatalogStore::create_column(
			&mut txn,
			table.id,
			ColumnToCreate {
				fragment: None,
				schema_name: "test_schema",
				table: table.id,
				table_name: "test_table",
				column: "id".to_string(),
				value: Type::Uint8,
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
			},
		)
		.unwrap();

		let col2 = CatalogStore::create_column(
			&mut txn,
			table.id,
			ColumnToCreate {
				fragment: None,
				schema_name: "test_schema",
				table: table.id,
				table_name: "test_table",
				column: "tenant_id".to_string(),
				value: Type::Uint8,
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(1),
				auto_increment: false,
			},
		)
		.unwrap();

		// Create primary key
		let primary_key_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				store: StoreId::Table(table.id),
				column_ids: vec![col1.id, col2.id],
			},
		)
		.unwrap();

		// Verify the primary key was created
		assert_eq!(primary_key_id, PrimaryKeyId(1));

		// Find and verify the primary key
		let found_pk =
			CatalogStore::find_primary_key(&mut txn, table.id)
				.unwrap()
				.expect("Primary key should exist");

		assert_eq!(found_pk.id, primary_key_id);
		assert_eq!(found_pk.columns.len(), 2);
		assert_eq!(found_pk.columns[0].id, col1.id);
		assert_eq!(found_pk.columns[0].name, "id");
		assert_eq!(found_pk.columns[1].id, col2.id);
		assert_eq!(found_pk.columns[1].name, "tenant_id");
	}

	#[test]
	fn test_create_primary_key_for_view() {
		let mut txn = create_test_command_transaction();
		let schema = ensure_test_schema(&mut txn);

		// Create a view
		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				fragment: None,
				schema: schema.id,
				name: "test_view".to_string(),
				columns: vec![
					ViewColumnToCreate {
						name: "id".to_string(),
						ty: Type::Uint8,
						fragment: None,
					},
					ViewColumnToCreate {
						name: "name".to_string(),
						ty: Type::Utf8,
						fragment: None,
					},
				],
			},
		)
		.unwrap();

		// Get column IDs for the view
		let columns =
			CatalogStore::list_table_columns(&mut txn, view.id)
				.unwrap();
		assert_eq!(columns.len(), 2);

		// Create primary key on first column only
		let primary_key_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				store: StoreId::View(view.id),
				column_ids: vec![columns[0].id],
			},
		)
		.unwrap();

		// Verify the primary key was created
		assert_eq!(primary_key_id, PrimaryKeyId(1));

		// Find and verify the primary key
		let found_pk =
			CatalogStore::find_primary_key(&mut txn, view.id)
				.unwrap()
				.expect("Primary key should exist");

		assert_eq!(found_pk.id, primary_key_id);
		assert_eq!(found_pk.columns.len(), 1);
		assert_eq!(found_pk.columns[0].id, columns[0].id);
		assert_eq!(found_pk.columns[0].name, "id");
	}

	#[test]
	fn test_create_composite_primary_key() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn);

		// Create multiple columns
		let mut column_ids = Vec::new();
		for i in 0..3 {
			let col = CatalogStore::create_column(
				&mut txn,
				table.id,
				ColumnToCreate {
					fragment: None,
					schema_name: "test_schema",
					table: table.id,
					table_name: "test_table",
					column: format!("col_{}", i),
					value: Type::Uint8,
					if_not_exists: false,
					policies: vec![],
					index: ColumnIndex(i as u16),
					auto_increment: false,
				},
			)
			.unwrap();
			column_ids.push(col.id);
		}

		// Create composite primary key
		let primary_key_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				store: StoreId::Table(table.id),
				column_ids: column_ids.clone(),
			},
		)
		.unwrap();

		// Find and verify the primary key
		let found_pk =
			CatalogStore::find_primary_key(&mut txn, table.id)
				.unwrap()
				.expect("Primary key should exist");

		assert_eq!(found_pk.id, primary_key_id);
		assert_eq!(found_pk.columns.len(), 3);
		for (i, col) in found_pk.columns.iter().enumerate() {
			assert_eq!(col.id, column_ids[i]);
			assert_eq!(col.name, format!("col_{}", i));
		}
	}

	#[test]
	fn test_create_primary_key_updates_table() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn);

		// Initially, table does not have primary key
		let initial_pk =
			CatalogStore::find_primary_key(&mut txn, table.id)
				.unwrap();
		assert!(initial_pk.is_none());

		// Create a column
		let col = CatalogStore::create_column(
			&mut txn,
			table.id,
			ColumnToCreate {
				fragment: None,
				schema_name: "test_schema",
				table: table.id,
				table_name: "test_table",
				column: "id".to_string(),
				value: Type::Uint8,
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
			},
		)
		.unwrap();

		// Create primary key
		let primary_key_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				store: StoreId::Table(table.id),
				column_ids: vec![col.id],
			},
		)
		.unwrap();

		// Now table should have the primary key
		let updated_pk =
			CatalogStore::find_primary_key(&mut txn, table.id)
				.unwrap()
				.expect("Primary key should exist");

		assert_eq!(updated_pk.id, primary_key_id);
	}

	#[test]
	fn test_create_primary_key_on_nonexistent_table() {
		let mut txn = create_test_command_transaction();

		// Try to create primary key on non-existent table
		// list_table_columns will return empty list for non-existent
		// table, so the column validation will fail
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				store: StoreId::Table(TableId(999)),
				column_ids: vec![ColumnId(1)],
			},
		);

		assert!(result.is_err());
		let err = result.unwrap_err();
		// Fails with CA_021 because column 1 won't be in the empty
		// column list
		assert_eq!(err.code, "CA_021");
	}

	#[test]
	fn test_create_primary_key_on_nonexistent_view() {
		let mut txn = create_test_command_transaction();

		// Try to create primary key on non-existent view
		// list_table_columns will return empty list for non-existent
		// view, so the column validation will fail
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				store: StoreId::View(ViewId(999)),
				column_ids: vec![ColumnId(1)],
			},
		);

		assert!(result.is_err());
		let err = result.unwrap_err();
		// Fails with CA_021 because column 1 won't be in the empty
		// column list
		assert_eq!(err.code, "CA_021");
	}

	#[test]
	fn test_create_empty_primary_key() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn);

		// Try to create primary key with no columns - should fail
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				store: StoreId::Table(table.id),
				column_ids: vec![],
			},
		);

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "CA_020");
	}

	#[test]
	fn test_create_primary_key_with_nonexistent_column() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn);

		// Try to create primary key with non-existent column ID
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				store: StoreId::Table(table.id),
				column_ids: vec![ColumnId(999)],
			},
		);

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "CA_021");
	}

	#[test]
	fn test_create_primary_key_with_column_from_different_table() {
		let mut txn = create_test_command_transaction();
		let table1 = ensure_test_table(&mut txn);

		// Create a column for table1
		let _col1 = CatalogStore::create_column(
			&mut txn,
			table1.id,
			ColumnToCreate {
				fragment: None,
				schema_name: "test_schema",
				table: table1.id,
				table_name: "test_table",
				column: "id".to_string(),
				value: Type::Uint8,
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
			},
		)
		.unwrap();

		// Create another table
		let schema = CatalogStore::get_schema(&mut txn, table1.schema)
			.unwrap();
		let table2 = CatalogStore::create_table(
			&mut txn,
			crate::table::TableToCreate {
				fragment: None,
				table: "test_table2".to_string(),
				schema: schema.id,
				columns: vec![],
			},
		)
		.unwrap();

		// Create a column for table2
		let col2 = CatalogStore::create_column(
			&mut txn,
			table2.id,
			ColumnToCreate {
				fragment: None,
				schema_name: "test_schema",
				table: table2.id,
				table_name: "test_table2",
				column: "id".to_string(),
				value: Type::Uint8,
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
			},
		)
		.unwrap();

		// Try to create primary key for table1 using column from table2
		// This must fail because we validate columns belong to the
		// specific table
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				store: StoreId::Table(table1.id),
				column_ids: vec![col2.id],
			},
		);

		// Should fail with CA_021 because col2 doesn't belong to table1
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "CA_021");
	}
}
