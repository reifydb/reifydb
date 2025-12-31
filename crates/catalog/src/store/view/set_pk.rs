// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::{CommandTransaction, PrimaryKeyId, ViewId, ViewKey},
	return_internal_error,
};

use crate::{CatalogStore, store::view::layout::view};

impl CatalogStore {
	/// Set the primary key ID for a view
	/// Returns an internal error if the view doesn't exist
	pub async fn set_view_primary_key(
		txn: &mut impl CommandTransaction,
		view_id: ViewId,
		primary_key_id: PrimaryKeyId,
	) -> crate::Result<()> {
		let multi = match txn.get(&ViewKey::encoded(view_id)).await? {
			Some(v) => v,
			None => return_internal_error!(format!(
				"View with ID {} not found when setting primary key. This indicates a critical catalog inconsistency.",
				view_id.0
			)),
		};

		let mut updated_row = multi.values.clone();
		view::LAYOUT.set_u64(&mut updated_row, view::PRIMARY_KEY, primary_key_id.0);

		txn.set(&ViewKey::encoded(view_id), updated_row).await?;

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{PrimaryKeyId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;
	use reifydb_type::{Type, TypeConstraint};

	use crate::{
		CatalogStore,
		store::view::{ViewColumnToCreate, ViewToCreate},
		test_utils::ensure_test_namespace,
	};

	#[tokio::test]
	async fn test_set_view_primary_key() {
		let mut txn = create_test_command_transaction().await;
		let namespace = ensure_test_namespace(&mut txn).await;

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
		.await
		.unwrap();

		// Set primary key
		CatalogStore::set_view_primary_key(&mut txn, view.id, PrimaryKeyId(42)).await.unwrap();

		// The test succeeds if no error is thrown.
		// In real usage, create_primary_key would create both the
		// PrimaryKey record and update the view, and find_primary_key
		// would find it.
	}

	#[tokio::test]
	async fn test_set_view_primary_key_nonexistent() {
		let mut txn = create_test_command_transaction().await;

		// Try to set primary key on non-existent view
		let result = CatalogStore::set_view_primary_key(&mut txn, ViewId(999), PrimaryKeyId(1)).await;

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert!(err.to_string().contains("View with ID 999 not found"));
		assert!(err.to_string().contains("critical catalog inconsistency"));
	}
}
