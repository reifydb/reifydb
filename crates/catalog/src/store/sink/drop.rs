// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

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
			txn.remove(&NamespaceSinkKey::encoded(sink.namespace, sink_id))?;

			txn.remove(&SinkKey::encoded(sink_id))?;
		}

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::SinkId;
	use reifydb_test_harness::engine::create_test_admin_transaction;
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

		assert!(CatalogStore::find_sink(&mut Transaction::Admin(&mut txn), sink.id).unwrap().is_some());

		assert!(CatalogStore::find_sink_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "drop_test_sink")
			.unwrap()
			.is_some());

		CatalogStore::drop_sink(&mut txn, sink.id).unwrap();

		assert!(CatalogStore::find_sink(&mut Transaction::Admin(&mut txn), sink.id).unwrap().is_none());

		assert!(CatalogStore::find_sink_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "drop_test_sink")
			.unwrap()
			.is_none());
	}

	#[test]
	fn test_drop_nonexistent_sink() {
		// Dropping a sink that never existed is a no-op, not an error.
		let mut txn = create_test_admin_transaction();

		CatalogStore::drop_sink(&mut txn, SinkId(999)).unwrap();
	}
}
