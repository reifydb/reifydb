// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{interface::catalog::id::SubscriptionId, key::subscription::SubscriptionKey};
use reifydb_transaction::standard::command::StandardCommandTransaction;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn delete_subscription(
		txn: &mut StandardCommandTransaction,
		subscription: SubscriptionId,
	) -> crate::Result<()> {
		// Delete the subscription metadata
		txn.remove(&SubscriptionKey::encoded(subscription))?;

		// Note: Column deletion would require iterating through and removing columns
		// For now, the columns associated with the subscription are orphaned when deleted
		// This is acceptable since subscriptions are ephemeral

		// Note: Subscription deltas should be cleaned up by the consumer API (ack/close)
		// before calling delete_subscription

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, store::subscription::create::SubscriptionToCreate};

	#[test]
	fn test_delete_subscription() {
		let mut txn = create_test_command_transaction();

		let created = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.unwrap();

		// Verify it exists
		let found = CatalogStore::find_subscription(&mut txn, created.id).unwrap();
		assert!(found.is_some());

		// Delete it
		CatalogStore::delete_subscription(&mut txn, created.id).unwrap();

		// Verify it's gone
		let found = CatalogStore::find_subscription(&mut txn, created.id).unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_delete_nonexistent_subscription() {
		let mut txn = create_test_command_transaction();

		use reifydb_core::interface::catalog::id::SubscriptionId;
		// Deleting a non-existent subscription should not error
		let non_existent = SubscriptionId::new();
		let result = CatalogStore::delete_subscription(&mut txn, non_existent);
		assert!(result.is_ok());
	}
}
