// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::{PrimaryKeyId, ViewId},
	key::view::ViewKey,
};
use reifydb_transaction::transaction::AsTransaction;

use crate::{CatalogStore, store::view::schema::view};

impl CatalogStore {
	/// Get the primary key ID for a view
	/// Returns None if the view doesn't exist or has no primary key
	pub(crate) fn get_view_pk_id(
		rx: &mut impl AsTransaction,
		view_id: ViewId,
	) -> crate::Result<Option<PrimaryKeyId>> {
		let mut txn = rx.as_transaction();
		let multi = match txn.get(&ViewKey::encoded(view_id))? {
			Some(v) => v,
			None => return Ok(None),
		};

		let pk_id = view::SCHEMA.get_u64(&multi.values, view::PRIMARY_KEY);

		if pk_id == 0 {
			Ok(None)
		} else {
			Ok(Some(PrimaryKeyId(pk_id)))
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{id::ViewId, primitive::PrimitiveId};
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_type::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, r#type::Type},
	};

	use crate::{
		CatalogStore,
		store::{
			primary_key::create::PrimaryKeyToCreate,
			view::create::{ViewColumnToCreate, ViewToCreate},
		},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_get_view_pk_id_with_primary_key() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		// Create a view
		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				name: Fragment::internal("test_view"),
				namespace: namespace.id,
				columns: vec![ViewColumnToCreate {
					name: Fragment::internal("id"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Uint8),
				}],
			},
		)
		.unwrap();

		// Get column IDs for the view
		let columns = CatalogStore::list_columns(&mut txn, view.id).unwrap();

		// Create primary key
		let pk_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				primitive: PrimitiveId::View(view.id),
				column_ids: vec![columns[0].id],
			},
		)
		.unwrap();

		// Get the primary key ID
		let retrieved_pk_id =
			CatalogStore::get_view_pk_id(&mut txn, view.id).unwrap().expect("Primary key ID should exist");

		assert_eq!(retrieved_pk_id, pk_id);
	}

	#[test]
	fn test_get_view_pk_id_without_primary_key() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		// Create a view
		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				name: Fragment::internal("test_view"),
				namespace: namespace.id,
				columns: vec![ViewColumnToCreate {
					name: Fragment::internal("id"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Uint8),
				}],
			},
		)
		.unwrap();

		// Get the primary key ID - should be None
		let pk_id = CatalogStore::get_view_pk_id(&mut txn, view.id).unwrap();

		assert!(pk_id.is_none());
	}

	#[test]
	fn test_get_view_pk_id_nonexistent_view() {
		let mut txn = create_test_admin_transaction();

		// Get the primary key ID for non-existent view - should be None
		let pk_id = CatalogStore::get_view_pk_id(&mut txn, ViewId(999)).unwrap();

		assert!(pk_id.is_none());
	}
}
