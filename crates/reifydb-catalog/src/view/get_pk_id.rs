// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{
	Key, PrimaryKeyId, QueryTransaction, ViewId, ViewKey,
};

use crate::{CatalogStore, view::layout::view};

impl CatalogStore {
	/// Get the primary key ID for a view
	/// Returns None if the view doesn't exist or has no primary key
	pub fn get_view_pk_id(
		rx: &mut impl QueryTransaction,
		view_id: ViewId,
	) -> crate::Result<Option<PrimaryKeyId>> {
		let versioned = match rx.get(&Key::View(ViewKey {
			view: view_id,
		})
		.encode())?
		{
			Some(v) => v,
			None => return Ok(None),
		};

		let pk_id =
			view::LAYOUT.get_u64(&versioned.row, view::PRIMARY_KEY);

		if pk_id == 0 {
			Ok(None)
		} else {
			Ok(Some(PrimaryKeyId(pk_id)))
		}
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{StoreId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::Type;

	use crate::{
		CatalogStore,
		primary_key::PrimaryKeyToCreate,
		test_utils::ensure_test_schema,
		view::{ViewColumnToCreate, ViewToCreate},
	};

	#[test]
	fn test_get_view_pk_id_with_primary_key() {
		let mut txn = create_test_command_transaction();
		let schema = ensure_test_schema(&mut txn);

		// Create a view
		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				fragment: None,
				schema: schema.id,
				name: "test_view".to_string(),
				columns: vec![ViewColumnToCreate {
					name: "id".to_string(),
					ty: Type::Uint8,
					fragment: None,
				}],
			},
		)
		.unwrap();

		// Get column IDs for the view
		let columns =
			CatalogStore::list_columns(&mut txn, view.id).unwrap();

		// Create primary key
		let pk_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				store: StoreId::View(view.id),
				column_ids: vec![columns[0].id],
			},
		)
		.unwrap();

		// Get the primary key ID
		let retrieved_pk_id =
			CatalogStore::get_view_pk_id(&mut txn, view.id)
				.unwrap()
				.expect("Primary key ID should exist");

		assert_eq!(retrieved_pk_id, pk_id);
	}

	#[test]
	fn test_get_view_pk_id_without_primary_key() {
		let mut txn = create_test_command_transaction();
		let schema = ensure_test_schema(&mut txn);

		// Create a view
		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				fragment: None,
				schema: schema.id,
				name: "test_view".to_string(),
				columns: vec![ViewColumnToCreate {
					name: "id".to_string(),
					ty: Type::Uint8,
					fragment: None,
				}],
			},
		)
		.unwrap();

		// Get the primary key ID - should be None
		let pk_id = CatalogStore::get_view_pk_id(&mut txn, view.id)
			.unwrap();

		assert!(pk_id.is_none());
	}

	#[test]
	fn test_get_view_pk_id_nonexistent_view() {
		let mut txn = create_test_command_transaction();

		// Get the primary key ID for non-existent view - should be None
		let pk_id = CatalogStore::get_view_pk_id(&mut txn, ViewId(999))
			.unwrap();

		assert!(pk_id.is_none());
	}
}
