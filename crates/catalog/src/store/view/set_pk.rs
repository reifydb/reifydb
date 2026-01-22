// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::{PrimaryKeyId, ViewId},
	key::view::ViewKey,
	return_internal_error,
};
use reifydb_transaction::standard::command::StandardCommandTransaction;

use crate::{CatalogStore, store::view::schema::view};

impl CatalogStore {
	/// Set the primary key ID for a view
	/// Returns an internal error if the view doesn't exist
	pub(crate) fn set_view_primary_key(
		txn: &mut StandardCommandTransaction,
		view_id: ViewId,
		primary_key_id: PrimaryKeyId,
	) -> crate::Result<()> {
		let multi = match txn.get(&ViewKey::encoded(view_id))? {
			Some(v) => v,
			None => return_internal_error!(format!(
				"View with ID {} not found when setting primary key. This indicates a critical catalog inconsistency.",
				view_id.0
			)),
		};

		let mut updated_row = multi.values.clone();
		view::SCHEMA.set_u64(&mut updated_row, view::PRIMARY_KEY, primary_key_id.0);

		txn.set(&ViewKey::encoded(view_id), updated_row)?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{PrimaryKeyId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::value::{constraint::TypeConstraint, r#type::Type};

	use crate::{
		CatalogStore,
		store::view::create::{ViewColumnToCreate, ViewToCreate},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_set_view_primary_key() {
		let mut txn = create_test_command_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				fragment: None,
				namespace: namespace.id,
				name: "test_view".to_string(),
				columns: vec![ViewColumnToCreate {
					name: "id".to_string(),
					constraint: TypeConstraint::unconstrained(Type::Uint8),
					fragment: None,
				}],
			},
		)
		.unwrap();

		// Set primary key
		CatalogStore::set_view_primary_key(&mut txn, view.id, PrimaryKeyId(42)).unwrap();

		// The test succeeds if no error is thrown.
		// In real usage, create_primary_key would create both the
		// PrimaryKey record and update the view, and find_primary_key
		// would find it.
	}

	#[test]
	fn test_set_view_primary_key_nonexistent() {
		let mut txn = create_test_command_transaction();

		// Try to set primary key on non-existent view
		let result = CatalogStore::set_view_primary_key(&mut txn, ViewId(999), PrimaryKeyId(1));

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("View with ID 999 not found"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}
}
