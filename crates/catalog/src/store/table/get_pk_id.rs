// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::id::{PrimaryKeyId, TableId},
	key::table::TableKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::table::shape::table};

impl CatalogStore {
	pub(crate) fn get_table_pk_id(rx: &mut Transaction<'_>, table_id: TableId) -> Result<Option<PrimaryKeyId>> {
		let multi = match rx.get(&TableKey::encoded(table_id))? {
			Some(v) => v,
			None => return Ok(None),
		};

		let pk_id = table::get_primary_key(EncodedCatalogRow::view(&multi.bytes));

		if pk_id == 0 {
			Ok(None)
		} else {
			Ok(Some(PrimaryKeyId(pk_id)))
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{column::ColumnIndex, id::TableId, object::ObjectId};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::{constraint::TypeConstraint, value_type::ValueType};

	use crate::{
		CatalogStore,
		store::{column::create::ColumnToCreate, primary_key::create::PrimaryKeyToCreate},
		test_utils::ensure_test_table,
	};

	#[test]
	fn test_get_table_pk_id_with_primary_key() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		let col = CatalogStore::create_column(
			&mut txn,
			table.id,
			ColumnToCreate {
				fragment: None,
				namespace_name: "test_namespace".to_string(),
				object_name: "test_table".to_string(),
				column: "id".to_string(),
				constraint: TypeConstraint::unconstrained(ValueType::Uint8),
				properties: vec![],
				index: ColumnIndex(0),
				auto_increment: false,
				dictionary_id: None,
			},
		)
		.unwrap();

		let pk_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				object: ObjectId::Table(table.id),
				column_ids: vec![col.id],
			},
		)
		.unwrap();

		let retrieved_pk_id = CatalogStore::get_table_pk_id(&mut Transaction::Admin(&mut txn), table.id)
			.unwrap()
			.expect("Primary key ID should exist");

		assert_eq!(retrieved_pk_id, pk_id);
	}

	#[test]
	fn test_get_table_pk_id_without_primary_key() {
		let mut txn = create_test_admin_transaction();
		let table = ensure_test_table(&mut txn);

		let pk_id = CatalogStore::get_table_pk_id(&mut Transaction::Admin(&mut txn), table.id).unwrap();

		assert!(pk_id.is_none());
	}

	#[test]
	fn test_get_table_pk_id_nonexistent_table() {
		let mut txn = create_test_admin_transaction();

		let pk_id = CatalogStore::get_table_pk_id(&mut Transaction::Admin(&mut txn), TableId(999)).unwrap();

		assert!(pk_id.is_none());
	}
}
