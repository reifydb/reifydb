// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	interface::{CommandTransaction, Key, PrimaryKeyId, ViewId, ViewKey},
	return_internal_error,
};

use crate::{CatalogStore, view::layout::view};

impl CatalogStore {
	/// Set the primary key ID for a view
	/// Returns an internal error if the view doesn't exist
	pub fn set_view_primary_key(
		txn: &mut impl CommandTransaction,
		view_id: ViewId,
		primary_key_id: PrimaryKeyId,
	) -> crate::Result<()> {
		let versioned = match txn.get(&Key::View(ViewKey {
			view: view_id,
		})
		.encode())?
		{
			Some(v) => v,
			None => return_internal_error!(format!(
				"View with ID {} not found when setting primary key. This indicates a critical catalog inconsistency.",
				view_id.0
			)),
		};

		let mut updated_row = versioned.row.clone();
		view::LAYOUT.set_u64(
			&mut updated_row,
			view::PRIMARY_KEY,
			primary_key_id.0,
		);

		txn.set(
			&Key::View(ViewKey {
				view: view_id,
			})
			.encode(),
			updated_row,
		)?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{PrimaryKeyId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::Type;

	use crate::{
		CatalogStore,
		test_utils::ensure_test_schema,
		view::{ViewColumnToCreate, ViewToCreate},
	};

	#[test]
	fn test_set_view_primary_key() {
		let mut txn = create_test_command_transaction();
		let schema = ensure_test_schema(&mut txn);

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

		// Set primary key
		CatalogStore::set_view_primary_key(
			&mut txn,
			view.id,
			PrimaryKeyId(42),
		)
		.unwrap();

		// The test succeeds if no error is thrown.
		// In real usage, create_primary_key would create both the
		// PrimaryKey record and update the view, and find_primary_key
		// would find it.
	}

	#[test]
	fn test_set_view_primary_key_nonexistent() {
		let mut txn = create_test_command_transaction();

		// Try to set primary key on non-existent view
		let result = CatalogStore::set_view_primary_key(
			&mut txn,
			ViewId(999),
			PrimaryKeyId(1),
		);

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("View with ID 999 not found"));
		assert!(err
			.to_string()
			.contains("critical catalog inconsistency"));
	}
}
