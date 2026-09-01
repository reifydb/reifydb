// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{id::ViewId, storage::StorageId, view::ViewStorageKind},
	key::{catalog::ViewKey, namespace::NamespaceViewKey, ringbuffer::RingBufferMetadataKey, row::RowSettingsKey},
};
use reifydb_transaction::{
	multi::RangeScope,
	transaction::{Transaction, admin::AdminTransaction},
};

use crate::{CatalogStore, Result, store::object::drop::drop_object_metadata};

impl CatalogStore {
	pub(crate) fn drop_view(txn: &mut AdminTransaction, view: ViewId) -> Result<()> {
		let pk_id = if let Some(view_def) = Self::find_view(&mut Transaction::Admin(&mut *txn), view)? {
			txn.remove(&NamespaceViewKey::encoded(view_def.namespace(), view))?;
			Self::drop_view_family_metadata(txn, view, view_def.storage_kind())?;
			view_def.primary_key().map(|pk| pk.id)
		} else {
			None
		};

		drop_object_metadata(txn, view.into(), pk_id)?;

		txn.remove(&RowSettingsKey::encoded(StorageId::View(view)))?;

		txn.remove(&ViewKey::encoded(view))?;

		Ok(())
	}

	fn drop_view_family_metadata(txn: &mut AdminTransaction, view: ViewId, kind: ViewStorageKind) -> Result<()> {
		match kind {
			ViewStorageKind::Table => Ok(()),
			ViewStorageKind::RingBuffer => {
				let range = RingBufferMetadataKey::full_scan_for_storage(StorageId::View(view));
				let mut stream = txn.range(range, RangeScope::All, 1024)?;
				let mut keys = Vec::new();
				for entry in stream.by_ref() {
					keys.push(entry?.key.clone());
				}
				drop(stream);
				for key in keys {
					txn.remove(&key)?;
				}
				Ok(())
			}
			ViewStorageKind::Series => Ok(()),
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::ViewId;
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::{
		fragment::Fragment,
		value::{constraint::TypeConstraint, value_type::ValueType},
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

		let found = CatalogStore::find_view_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "test_view")
			.unwrap();
		assert!(found.is_some());

		CatalogStore::drop_view(&mut txn, created.id()).unwrap();

		let found = CatalogStore::find_view_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "test_view")
			.unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_drop_nonexistent_view() {
		// Dropping a view that never existed is a no-op, not an error.
		let mut txn = create_test_admin_transaction();

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
					constraint: TypeConstraint::unconstrained(ValueType::Int4),
					dictionary_id: None,
				},
				ViewColumnToCreate {
					name: Fragment::internal("col_b"),
					fragment: Fragment::None,
					constraint: TypeConstraint::unconstrained(ValueType::Utf8),
					dictionary_id: None,
				},
			],
		);

		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), view.id()).unwrap();
		assert_eq!(columns.len(), 2);

		CatalogStore::drop_view(&mut txn, view.id()).unwrap();

		let columns = CatalogStore::list_columns(&mut Transaction::Admin(&mut txn), view.id()).unwrap();
		assert!(columns.is_empty());

		let found = CatalogStore::find_view_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "meta_view")
			.unwrap();
		assert!(found.is_none());
	}
}
