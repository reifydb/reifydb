// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::SinkId,
	key::{namespace_sink::NamespaceSinkKey, sink::SinkKey},
};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn drop_sink(txn: &mut AdminTransaction, sink_id: SinkId) -> Result<()> {
		let sink = CatalogStore::find_sink(&mut Transaction::Admin(&mut *txn), sink_id)?;

		if let Some(sink) = sink {
			// Delete from namespace index
			txn.remove(&NamespaceSinkKey::encoded(sink.namespace, sink_id))?;

			// Delete from main sink table
			txn.remove(&SinkKey::encoded(sink_id))?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::SinkId;
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_sink},
	};

	#[test]
	fn test_drop_sink() {
		let mut txn = create_test_admin_transaction();
		let ns = create_namespace(&mut txn, "test_namespace");
		let sink = create_sink(&mut txn, "test_namespace", "drop_test_sink", "kafka");

		// Verify sink exists by ID
		assert!(CatalogStore::find_sink(&mut Transaction::Admin(&mut txn), sink.id).unwrap().is_some());

		// Verify sink exists by name
		assert!(CatalogStore::find_sink_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "drop_test_sink")
			.unwrap()
			.is_some());

		// Drop the sink
		CatalogStore::drop_sink(&mut txn, sink.id).unwrap();

		// Verify sink is gone by ID
		assert!(CatalogStore::find_sink(&mut Transaction::Admin(&mut txn), sink.id).unwrap().is_none());

		// Verify sink is gone by name
		assert!(CatalogStore::find_sink_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "drop_test_sink")
			.unwrap()
			.is_none());
	}

	#[test]
	fn test_drop_nonexistent_sink() {
		let mut txn = create_test_admin_transaction();

		// Dropping a non-existent sink should succeed silently
		CatalogStore::drop_sink(&mut txn, SinkId(999)).unwrap();
	}
}
