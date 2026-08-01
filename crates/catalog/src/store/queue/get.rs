// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{id::QueueId, queue::Queue},
	internal,
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{CatalogStore, Result};

impl CatalogStore {
	pub(crate) fn get_queue(rx: &mut Transaction<'_>, queue: QueueId) -> Result<Queue> {
		Self::find_queue(rx, queue)?.ok_or_else(|| {
			Error(Box::new(internal!(
				"Queue with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				queue
			)))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::QueueId;
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::CatalogStore;

	#[test]
	fn test_get_queue_missing_is_an_internal_error() {
		// A missing id means the catalog is inconsistent, so get must raise rather than
		// degrade to none the way find does.
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::get_queue(&mut Transaction::Admin(&mut txn), QueueId(999));

		assert!(result.is_err());
		let err = result.unwrap_err();
		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("QueueId(999)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
