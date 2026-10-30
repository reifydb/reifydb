// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::id::WindowedViewGroupId,
	key::{catalog::WindowedViewGroupKey, namespace::NamespaceWindowedViewGroupKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_windowed_view_group(
		txn: &mut AdminTransaction,
		group_id: WindowedViewGroupId,
	) -> Result<()> {
		let group = CatalogStore::find_windowed_view_group(&mut Transaction::Admin(&mut *txn), group_id)?;

		if let Some(group) = group {
			txn.remove(vec![(NamespaceWindowedViewGroupKey::new(group.namespace, group_id).into(), None)])?;

			txn.remove(vec![(WindowedViewGroupKey::new(group_id).into(), None)])?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::{ViewId, WindowedViewGroupId};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_windowed_view_group},
	};

	#[test]
	fn test_drop_windowed_view_group() {
		let mut txn = create_test_admin_transaction();
		let ns = create_namespace(&mut txn, "test_namespace");
		let group = create_windowed_view_group(&mut txn, "test_namespace", "drop_test_group", &[ViewId(1)]);

		assert!(CatalogStore::find_windowed_view_group(&mut Transaction::Admin(&mut txn), group.id)
			.unwrap()
			.is_some());

		assert!(CatalogStore::find_windowed_view_group_by_name(
			&mut Transaction::Admin(&mut txn),
			ns.id(),
			"drop_test_group"
		)
		.unwrap()
		.is_some());

		CatalogStore::drop_windowed_view_group(&mut txn, group.id).unwrap();

		assert!(CatalogStore::find_windowed_view_group(&mut Transaction::Admin(&mut txn), group.id)
			.unwrap()
			.is_none());

		assert!(CatalogStore::find_windowed_view_group_by_name(
			&mut Transaction::Admin(&mut txn),
			ns.id(),
			"drop_test_group"
		)
		.unwrap()
		.is_none());
	}

	#[test]
	fn test_drop_nonexistent_windowed_view_group() {
		// Dropping a group that never existed is a no-op, not an error.
		let mut txn = create_test_admin_transaction();

		CatalogStore::drop_windowed_view_group(&mut txn, WindowedViewGroupId(999)).unwrap();
	}
}
