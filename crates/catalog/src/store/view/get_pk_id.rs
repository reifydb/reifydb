// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	interface::catalog::id::{PrimaryKeyId, ViewId},
	key::catalog::ViewKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::view::shape::view};

impl CatalogStore {
	pub(crate) fn get_view_pk_id(rx: &mut Transaction<'_>, view_id: ViewId) -> Result<Option<PrimaryKeyId>> {
		let multi = match rx.get(&ViewKey::encoded(view_id))? {
			Some(v) => v,
			None => return Ok(None),
		};

		let pk_id = view::get_primary_key(EncodedCatalogRow::view(&multi.bytes));

		if pk_id == 0 {
			Ok(None)
		} else {
			Ok(Some(PrimaryKeyId(pk_id)))
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{id::ViewId, object::ObjectId};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, value_type::ValueType},
	};

	use crate::{
		CatalogStore,
		store::{
			primary_key::create::PrimaryKeyToCreate,
			view::create::{ViewColumnToCreate, ViewStorage, ViewToCreate},
		},
		test_utils::ensure_test_namespace,
	};

	#[test]
	fn test_get_view_pk_id_with_primary_key() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				name: Fragment::internal("test_view"),
				namespace: namespace.id(),
				columns: vec![ViewColumnToCreate {
					name: Fragment::internal("id"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					dictionary_id: None,
				}],
				storage: ViewStorage::default(),
				sort: vec![],
			},
		)
		.unwrap();

		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), view.id()).unwrap();

		let pk_id = CatalogStore::create_primary_key(
			&mut txn,
			PrimaryKeyToCreate {
				object: ObjectId::View(view.id()),
				column_ids: vec![columns[0].id],
			},
		)
		.unwrap();

		let retrieved_pk_id = CatalogStore::get_view_pk_id(&mut Transaction::Admin(&mut txn), view.id())
			.unwrap()
			.expect("Primary key ID should exist");

		assert_eq!(retrieved_pk_id, pk_id);
	}

	#[test]
	fn test_get_view_pk_id_without_primary_key() {
		let mut txn = create_test_admin_transaction();
		let namespace = ensure_test_namespace(&mut txn);

		let view = CatalogStore::create_deferred_view(
			&mut txn,
			ViewToCreate {
				name: Fragment::internal("test_view"),
				namespace: namespace.id(),
				columns: vec![ViewColumnToCreate {
					name: Fragment::internal("id"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Uint8),
					dictionary_id: None,
				}],
				storage: ViewStorage::default(),
				sort: vec![],
			},
		)
		.unwrap();

		let pk_id = CatalogStore::get_view_pk_id(&mut Transaction::Admin(&mut txn), view.id()).unwrap();

		assert!(pk_id.is_none());
	}

	#[test]
	fn test_get_view_pk_id_nonexistent_view() {
		let mut txn = create_test_admin_transaction();

		let pk_id = CatalogStore::get_view_pk_id(&mut Transaction::Admin(&mut txn), ViewId(999)).unwrap();

		assert!(pk_id.is_none());
	}
}
