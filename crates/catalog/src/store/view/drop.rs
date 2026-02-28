// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::ViewId,
	key::{namespace_view::NamespaceViewKey, view::ViewKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result, store::primitive::drop::drop_primitive_metadata};

impl CatalogStore {
	pub(crate) fn drop_view(txn: &mut AdminTransaction, view: ViewId) -> Result<()> {
		// First, find the view to get its namespace and primary key
		let pk_id = if let Some(view_def) = Self::find_view(&mut Transaction::Admin(&mut *txn), view)? {
			// Remove the namespace-view link (secondary index)
			txn.remove(&NamespaceViewKey::encoded(view_def.namespace, view))?;
			view_def.primary_key.as_ref().map(|pk| pk.id)
		} else {
			None
		};

		// Clean up all associated metadata (columns, policies, sequences, pk, retention)
		drop_primitive_metadata(txn, view.into(), pk_id)?;

		// Remove the view metadata
		txn.remove(&ViewKey::encoded(view))?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::ViewId;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, r#type::Type},
	};

	use crate::{
		CatalogStore,
		store::view::create::ViewColumnToCreate,
		test_utils::{create_namespace, create_view, ensure_test_namespace},
	};

	#[test]
	fn test_drop_view() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		let ns = create_namespace(&mut txn, "test_ns");

		let created = create_view(&mut txn, "test_ns", "test_view", &[]);

		// Verify it exists
		let found =
			CatalogStore::find_view_by_name(&mut Transaction::Admin(&mut txn), ns.id, "test_view").unwrap();
		assert!(found.is_some());

		// Drop it
		CatalogStore::drop_view(&mut txn, created.id).unwrap();

		// Verify it's gone
		let found =
			CatalogStore::find_view_by_name(&mut Transaction::Admin(&mut txn), ns.id, "test_view").unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_drop_nonexistent_view() {
		let mut txn = create_test_admin_transaction();

		// Dropping a non-existent view should not error
		let non_existent = ViewId(999999);
		let result = CatalogStore::drop_view(&mut txn, non_existent);
		assert!(result.is_ok());
	}

	#[test]
	fn test_drop_view_cleans_up_metadata() {
		let mut txn = create_test_admin_transaction();
		ensure_test_namespace(&mut txn);
		let ns = create_namespace(&mut txn, "view_meta_ns");

		let view = create_view(
			&mut txn,
			"view_meta_ns",
			"meta_view",
			&[
				ViewColumnToCreate {
					name: Fragment::internal("col_a"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Int4),
				},
				ViewColumnToCreate {
					name: Fragment::internal("col_b"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(Type::Utf8),
				},
			],
		);

		// Verify columns exist before drop
		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), view.id).unwrap();
		assert_eq!(columns.len(), 2);

		// Drop the view
		CatalogStore::drop_view(&mut txn, view.id).unwrap();

		// Verify columns are cleaned up
		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), view.id).unwrap();
		assert!(columns.is_empty());

		// Verify view itself is gone
		let found =
			CatalogStore::find_view_by_name(&mut Transaction::Admin(&mut txn), ns.id, "meta_view").unwrap();
		assert!(found.is_none());
	}
}
