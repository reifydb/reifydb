// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{PrimaryKeyId, QueryTransaction, TableId, TableKey};

use crate::{CatalogStore, store::table::layout::table};

impl CatalogStore {
	/// Get the primary key ID for a table
	/// Returns None if the table doesn't exist or has no primary key
	pub async fn get_table_pk_id(
		rx: &mut impl QueryTransaction,
		table_id: TableId,
	) -> crate::Result<Option<PrimaryKeyId>> {
		let multi = match rx.get(&TableKey::encoded(table_id)).await? {
			Some(v) => v,
			None => return Ok(None),
		};

		let pk_id = table::LAYOUT.get_u64(&multi.values, table::PRIMARY_KEY);

		if pk_id == 0 {
			Ok(None)
		} else {
			Ok(Some(PrimaryKeyId(pk_id)))
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{PrimitiveId, TableId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		column::{ColumnIndex, ColumnToCreate},
		primary_key::PrimaryKeyToCreate,
		test_utils::ensure_test_table,
	};

	#[tokio::test]
	async fn test_get_table_pk_id_with_primary_key() {
		let mut txn = create_test_command_transaction().await;
		let table = ensure_test_table(&mut txn).await;

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
				constraint: reifydb_type::TypeConstraint::unconstrained(reifydb_type::Type::Uint8),
				if_not_exists: false,
				policies: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.await
		.unwrap();

		// Create primary key
		let pk_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				source: PrimitiveId::Table(table.id),
				column_ids: vec![col.id],
			},
		)
		.await
		.unwrap();

		// Get the primary key ID
		let retrieved_pk_id = CatalogStore::get_table_pk_id(&mut txn, table.id)
			.await
			.unwrap()
			.expect("Primary key ID should exist");

		assert_eq!(retrieved_pk_id, pk_id);
	}

	#[tokio::test]
	async fn test_get_table_pk_id_without_primary_key() {
		let mut txn = create_test_command_transaction().await;
		let table = ensure_test_table(&mut txn).await;

		// Get the primary key ID - should be None
		let pk_id = CatalogStore::get_table_pk_id(&mut txn, table.id).await.unwrap();

		assert!(pk_id.is_none());
	}

	#[tokio::test]
	async fn test_get_table_pk_id_nonexistent_table() {
		let mut txn = create_test_command_transaction().await;

		// Get the primary key ID for non-existent table - should be
		// None
		let pk_id = CatalogStore::get_table_pk_id(&mut txn, TableId(999)).await.unwrap();

		assert!(pk_id.is_none());
	}
}
