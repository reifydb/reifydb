// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		id::{ColumnId, PrimaryKeyId},
		primitive::PrimitiveId,
	},
	key::primary_key::PrimaryKeyKey,
	return_internal_error,
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_type::fragment::Fragment;

use crate::{
	CatalogStore, Result,
	error::CatalogError,
	store::{
		primary_key::schema::{
			primary_key,
			primary_key::{SCHEMA, serialize_column_ids},
		},
		sequence::system::SystemSequence,
	},
};

pub struct PrimaryKeyToCreate {
	pub primitive: PrimitiveId,
	pub column_ids: Vec<ColumnId>,
}

impl CatalogStore {
	pub(crate) fn create_primary_key(
		txn: &mut AdminTransaction,
		to_create: PrimaryKeyToCreate,
	) -> Result<PrimaryKeyId> {
		// Validate that primary key has at least one column
		if to_create.column_ids.is_empty() {
			return Err(CatalogError::PrimaryKeyEmpty {
				fragment: Fragment::None,
			}
			.into());
		}

		// Get the columns for the table/view and validate all primary
		// key columns belong to it
		let source_columns = Self::list_columns(&mut Transaction::Admin(&mut *txn), to_create.primitive)?;
		let source_column_ids: std::collections::HashSet<_> = source_columns.iter().map(|c| c.id).collect();

		// Validate that all columns belong to the table/view
		for column_id in &to_create.column_ids {
			if !source_column_ids.contains(column_id) {
				return Err(CatalogError::PrimaryKeyColumnNotFound {
					fragment: Fragment::None,
					column_id: column_id.0,
				}
				.into());
			}
		}

		let id = SystemSequence::next_primary_key_id(txn)?;

		// Create primary key encoded
		let mut row = SCHEMA.allocate();
		SCHEMA.set_u64(&mut row, primary_key::ID, id.0);
		SCHEMA.set_u64(&mut row, primary_key::SOURCE, to_create.primitive.as_u64());
		SCHEMA.set_blob(&mut row, primary_key::COLUMN_IDS, &serialize_column_ids(&to_create.column_ids));

		// Store the primary key
		txn.set(&PrimaryKeyKey::encoded(id), row)?;

		// Update the table or view to reference this primary key
		match to_create.primitive {
			PrimitiveId::Table(table_id) => {
				Self::set_table_primary_key(txn, table_id, id)?;
			}
			PrimitiveId::View(view_id) => {
				Self::set_view_primary_key(txn, view_id, id)?;
			}
			PrimitiveId::TableVirtual(_) => {
				// Virtual tables don't support primary keys
				return_internal_error!(
					"Cannot create primary key for virtual table. Virtual tables do not support primary keys."
				);
			}
			PrimitiveId::RingBuffer(ringbuffer_id) => {
				Self::set_ringbuffer_primary_key(txn, ringbuffer_id, id)?;
			}
			PrimitiveId::Dictionary(_) => {
				// Dictionaries don't support traditional primary keys
				return_internal_error!(
					"Cannot create primary key for dictionary. Dictionaries have their own key structure."
				);
			}
			PrimitiveId::Series(_) => {
				// Series don't support traditional primary keys
				return_internal_error!(
					"Cannot create primary key for series. Series use timestamp-based key ordering."
				);
			}
		}

		Ok(id)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{
		column::ColumnIndex,
		id::{ColumnId, PrimaryKeyId, TableId, ViewId},
		primitive::PrimitiveId,
	};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, r#type::Type},
	};

	use super::PrimaryKeyToCreate;
	use crate::{
		CatalogStore,
		store::{
			column::create::ColumnToCreate,
			table::create::TableToCreate,
			view::create::{ViewColumnToCreate, ViewToCreate},
		},
		test_utils::{ensure_test_namespace, ensure_test_table},
	};

	#[test]
	fn test_create_primary_key_for_table() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		// Create columns for the table
		let col1 = CatalogStore::create_column(
			&mut txn,
			table.id,
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				primitive_name: "test_table".to_string(),
				column: "id".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.unwrap();

		let col2 = CatalogStore::create_column(
			&mut txn,
			table.id,
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				primitive_name: "test_table".to_string(),
				column: "tenant_id".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				properties: vec![],
				index: ColumnIndex(1),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		// Create primary key
		let primary_key_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				primitive: PrimitiveId::Table(table.id),
				column_ids: vec![col1.id, col2.id],
			},
		)
		.unwrap();

		// Verify the primary key was created
		assert_eq!(primary_key_id, PrimaryKeyId(1));

		// Find and verify the primary key
		let found_pk = CatalogStore::find_primary_key(&mut Transaction::Admin(&mut txn), table.id)
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
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		// Create a view
		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				name: Fragment::internal("test_view"),
				namespace: namespace.id,
				columns: vec![
					ViewColumnToCreate {
						name: Fragment::internal("id"),
						fragment: Fragment::None,
						constraint: TypeConstraint::unconstrained(Type::Uint8),
					},
					ViewColumnToCreate {
						name: Fragment::internal("name"),
						fragment: Fragment::None,
						constraint: TypeConstraint::unconstrained(Type::Utf8),
					},
				],
			},
		)
		.unwrap();

		// Get column IDs for the view
		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), view.id).unwrap();
		assert_eq!(columns.len(), 2);

		// Create primary key on first column only
		let primary_key_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				primitive: PrimitiveId::View(view.id),
				column_ids: vec![columns[0].id],
			},
		)
		.unwrap();

		// Verify the primary key was created
		assert_eq!(primary_key_id, PrimaryKeyId(1));

		// Find and verify the primary key
		let found_pk = CatalogStore::find_primary_key(&mut Transaction::Admin(&mut txn), view.id)
			.unwrap()
			.expect("Primary key should exist");

		assert_eq!(found_pk.id, primary_key_id);
		assert_eq!(found_pk.columns.len(), 1);
		assert_eq!(found_pk.columns[0].id, columns[0].id);
		assert_eq!(found_pk.columns[0].name, "id");
	}

	#[test]
	fn test_create_composite_primary_key() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		// Create multiple columns
		let mut column_ids = Vec::new();
		for i in 0..3 {
			let col = CatalogStore::create_column(
				&mut txn,
				table.id,
				ColumnToCreate {
					fragment: None,
					namespace_name: "test_namespace".to_string(),
					primitive_name: "test_table".to_string(),
					column: format!("col_{}", i),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					properties: vec![],
					index: ColumnIndex(i as u8),
					auto_increment: false,
					dictionary_id: None,
				},
			)
			.unwrap();
			column_ids.push(col.id);
		}

		// Create composite primary key
		let primary_key_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				primitive: PrimitiveId::Table(table.id),
				column_ids: column_ids.clone(),
			},
		)
		.unwrap();

		// Find and verify the primary key
		let found_pk = CatalogStore::find_primary_key(&mut Transaction::Admin(&mut txn), table.id)
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
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		// Initially, table does not have primary key
		let initial_pk = CatalogStore::find_primary_key(&mut Transaction::Admin(&mut txn), table.id).unwrap();
		assert!(initial_pk.is_none());

		// Create a column
		let col = CatalogStore::create_column(
			&mut txn,
			table.id,
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				primitive_name: "test_table".to_string(),
				column: "id".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: true,
				dictionary_id: None,
			},
		)
		.unwrap();

		// Create primary key
		let primary_key_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				primitive: PrimitiveId::Table(table.id),
				column_ids: vec![col.id],
			},
		)
		.unwrap();

		// Now table should have the primary key
		let updated_pk = CatalogStore::find_primary_key(&mut Transaction::Admin(&mut txn), table.id)
			.unwrap()
			.expect("Primary key should exist");

		assert_eq!(updated_pk.id, primary_key_id);
	}

	#[test]
	fn test_create_primary_key_on_nonexistent_table() {
		let mut txn = create_test_admin_transaction();

		// Try to create primary key on non-existent table
		// list_table_columns will return empty list for non-existent
		// table, so the column validation will fail
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				primitive: PrimitiveId::Table(TableId(999)),
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
		let mut txn = create_test_admin_transaction();

		// Try to create primary key on non-existent view
		// list_table_columns will return empty list for non-existent
		// view, so the column validation will fail
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				primitive: PrimitiveId::View(ViewId(999)),
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
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		// Try to create primary key with no columns - should fail
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				primitive: PrimitiveId::Table(table.id),
				column_ids: vec![],
			},
		);

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "CA_020");
	}

	#[test]
	fn test_create_primary_key_with_nonexistent_column() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		// Try to create primary key with non-existent column ID
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				primitive: PrimitiveId::Table(table.id),
				column_ids: vec![ColumnId(999)],
			},
		);

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "CA_021");
	}

	#[test]
	fn test_create_primary_key_with_column_from_different_table() {
		let mut txn = create_test_admin_transaction();
		let table1 = ensure_test_table(&mut txn);

		// Create a column for table1
		let _col1 = CatalogStore::create_column(
			&mut txn,
			table1.id,
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				primitive_name: "test_table".to_string(),
				column: "id".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		// Create another table
		let namespace =
			CatalogStore::get_namespace(&mut Transaction::Admin(&mut txn), table1.namespace).unwrap();
		let table2 = CatalogStore::create_table(
			&mut txn,
			TableToCreate {
				name: Fragment::internal("test_table2"),
				namespace: namespace.id,
				columns: vec![],
				retention_policy: None,
			},
		)
		.unwrap();

		// Create a column for table2
		let col2 = CatalogStore::create_column(
			&mut txn,
			table2.id,
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				primitive_name: "test_table2".to_string(),
				column: "id".to_string(),
				constraint: TypeConstraint::unconstrained(Type::Uint8),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		// Try to create primary key for table1 using column from table2
		// This must fail because we validate columns belong to the
		// specific table
		let result = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				primitive: PrimitiveId::Table(table1.id),
				column_ids: vec![col2.id],
			},
		);

		// Should fail with CA_021 because col2 doesn't belong to table1
		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "CA_021");
	}
}
