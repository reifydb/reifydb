// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{SubscriptionId, SubscriptionKey};
use reifydb_transaction::StandardCommandTransaction;

use crate::CatalogStore;

impl CatalogStore {
	pub async fn delete_subscription(
		txn: &mut StandardCommandTransaction,
		subscription: SubscriptionId,
	) -> crate::Result<()> {
		// Delete the subscription metadata
		txn.remove(&SubscriptionKey::encoded(subscription)).await?;

		// Note: Column deletion would require iterating through and removing columns
		// For now, the columns associated with the subscription are orphaned when deleted
		// This is acceptable since subscriptions are ephemeral

		// Note: Subscription deltas should be cleaned up by the consumer API (ack/close)
		// before calling delete_subscription

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, store::subscription::SubscriptionToCreate};

	#[tokio::test]
	async fn test_delete_subscription() {
		let mut txn = create_test_command_transaction().await;

		let created = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.await
		.unwrap();

		// Verify it exists
		let found = CatalogStore::find_subscription(&mut txn, created.id).await.unwrap();
		assert!(found.is_some());

		// Delete it
		CatalogStore::delete_subscription(&mut txn, created.id).await.unwrap();

		// Verify it's gone
		let found = CatalogStore::find_subscription(&mut txn, created.id).await.unwrap();
		assert!(found.is_none());
	}

	#[tokio::test]
	async fn test_delete_nonexistent_subscription() {
		let mut txn = create_test_command_transaction().await;

		use reifydb_core::interface::SubscriptionId;
		// Deleting a non-existent subscription should not error
		let non_existent = SubscriptionId::new();
		let result = CatalogStore::delete_subscription(&mut txn, non_existent).await;
		assert!(result.is_ok());
	}
}
