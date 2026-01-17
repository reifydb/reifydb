// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::{PrimaryKeyId, TableId},
	key::table::TableKey,
};
use reifydb_transaction::standard::IntoStandardTransaction;

use crate::{CatalogStore, store::table::schema::table};

impl CatalogStore {
	/// Get the primary key ID for a table
	/// Returns None if the table doesn't exist or has no primary key
	pub fn get_table_pk_id(
		rx: &mut impl IntoStandardTransaction,
		table_id: TableId,
	) -> crate::Result<Option<PrimaryKeyId>> {
		let mut txn = rx.into_standard_transaction();
		let multi = match txn.get(&TableKey::encoded(table_id))? {
			Some(v) => v,
			None => return Ok(None),
		};

		let pk_id = table::SCHEMA.get_u64(&multi.values, table::PRIMARY_KEY);

		if pk_id == 0 {
			Ok(None)
		} else {
			Ok(Some(PrimaryKeyId(pk_id)))
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{column::ColumnIndex, id::TableId, primitive::PrimitiveId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		store::{column::create::ColumnToCreate, primary_key::create::PrimaryKeyToCreate},
		test_utils::ensure_test_table,
	};

	#[test]
	fn test_get_table_pk_id_with_primary_key() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn);

		// Create a column
		let col = CatalogStore::create_column(
			&mut txn,
			table.id,
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				table: table.id,
				table_name: "test_table".to_string(),
				column: "id".to_string(),
				constraint: reifydb_type::value::constraint::TypeConstraint::unconstrained(
					reifydb_type::value::r#type::Type::Uint8,
				),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		// Create primary key
		let pk_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				source: PrimitiveId::Table(table.id),
				column_ids: vec![col.id],
			},
		)
		.unwrap();

		// Get the primary key ID
		let retrieved_pk_id = CatalogStore::get_table_pk_id(&mut txn, table.id)
			.unwrap()
			.expect("Primary key ID should exist");

		assert_eq!(retrieved_pk_id, pk_id);
	}

	#[test]
	fn test_get_table_pk_id_without_primary_key() {
		let mut txn = create_test_command_transaction();
		let table = ensure_test_table(&mut txn);

		// Get the primary key ID - should be None
		let pk_id = CatalogStore::get_table_pk_id(&mut txn, table.id).unwrap();

		assert!(pk_id.is_none());
	}

	#[test]
	fn test_get_table_pk_id_nonexistent_table() {
		let mut txn = create_test_command_transaction();

		// Get the primary key ID for non-existent table - should be
		// None
		let pk_id = CatalogStore::get_table_pk_id(&mut txn, TableId(999)).unwrap();

		assert!(pk_id.is_none());
	}
}
