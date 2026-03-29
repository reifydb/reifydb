// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::SourceId,
	key::{namespace_source::NamespaceSourceKey, source::SourceKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_source(txn: &mut AdminTransaction, shape_id: SourceId) -> Result<()> {
		let source = CatalogStore::find_source(&mut Transaction::Admin(&mut *txn), shape_id)?;

		if let Some(source) = source {
			// Delete from namespace index
			txn.remove(&NamespaceSourceKey::encoded(source.namespace, shape_id))?;

			// Delete from main source table
			txn.remove(&SourceKey::encoded(shape_id))?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::SourceId;
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_source},
	};

	#[test]
	fn test_drop_source() {
		let mut txn = create_test_admin_transaction();
		let ns = create_namespace(&mut txn, "test_namespace");
		let source = create_source(&mut txn, "test_namespace", "drop_test_source", "kafka");

		// Verify source exists by ID
		assert!(CatalogStore::find_source(&mut Transaction::Admin(&mut txn), source.id).unwrap().is_some());

		// Verify source exists by name
		assert!(CatalogStore::find_source_by_name(
			&mut Transaction::Admin(&mut txn),
			ns.id(),
			"drop_test_source"
		)
		.unwrap()
		.is_some());

		// Drop the source
		CatalogStore::drop_source(&mut txn, source.id).unwrap();

		// Verify source is gone by ID
		assert!(CatalogStore::find_source(&mut Transaction::Admin(&mut txn), source.id).unwrap().is_none());

		// Verify source is gone by name
		assert!(CatalogStore::find_source_by_name(
			&mut Transaction::Admin(&mut txn),
			ns.id(),
			"drop_test_source"
		)
		.unwrap()
		.is_none());
	}

	#[test]
	fn test_drop_nonexistent_source() {
		let mut txn = create_test_admin_transaction();

		// Dropping a non-existent source should succeed silently
		CatalogStore::drop_source(&mut txn, SourceId(999)).unwrap();
	}
}
