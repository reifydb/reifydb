// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::id::SourceId,
	key::{catalog::SourceKey, namespace::NamespaceSourceKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_source(txn: &mut AdminTransaction, object_id: SourceId) -> Result<()> {
		let source = CatalogStore::find_source(&mut Transaction::Admin(&mut *txn), object_id)?;

		if let Some(source) = source {
			txn.remove(&NamespaceSourceKey::encoded(source.namespace, object_id))?;

			txn.remove(&SourceKey::encoded(object_id))?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::SourceId;
	use reifydb_test_harness::engine::create_test_admin_transaction;
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

		assert!(CatalogStore::find_source(&mut Transaction::Admin(&mut txn), source.id).unwrap().is_some());

		assert!(CatalogStore::find_source_by_name(
			&mut Transaction::Admin(&mut txn),
			ns.id(),
			"drop_test_source"
		)
		.unwrap()
		.is_some());

		CatalogStore::drop_source(&mut txn, source.id).unwrap();

		assert!(CatalogStore::find_source(&mut Transaction::Admin(&mut txn), source.id).unwrap().is_none());

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
		// Dropping a source that never existed is a no-op, not an error.
		let mut txn = create_test_admin_transaction();

		CatalogStore::drop_source(&mut txn, SourceId(999)).unwrap();
	}
}
