// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	CommitVersion,
	interface::{Key, SubscriptionDef, SubscriptionKey},
};
use reifydb_transaction::{IntoStandardTransaction, StandardTransaction};

use crate::{CatalogStore, store::subscription::layout::subscription};

impl CatalogStore {
	pub async fn list_subscriptions_all(
		rx: &mut impl IntoStandardTransaction,
	) -> crate::Result<Vec<SubscriptionDef>> {
		let mut txn = rx.into_standard_transaction();
		let mut result = Vec::new();

		let batch = txn.range_batch(SubscriptionKey::full_scan(), 1024).await?;

		for entry in batch.items {
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::Subscription(sub_key) = key {
					let subscription_id = sub_key.subscription;

					let acknowledged_version = CommitVersion(
						subscription::LAYOUT
							.get_u64(&entry.values, subscription::ACKNOWLEDGED_VERSION),
					);

					// Load columns based on transaction type
					let columns = match &mut txn {
						StandardTransaction::Command(cmd) => {
							Self::list_subscription_columns(cmd, subscription_id).await?
						}
						StandardTransaction::Query(_) => vec![],
					};

					let subscription_def = SubscriptionDef {
						id: subscription_id,
						columns,
						// Subscriptions don't have primary keys (they use UUID v7 as their
						// identifier)
						primary_key: None,
						acknowledged_version,
					};

					result.push(subscription_def);
				}
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::SubscriptionId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, store::subscription::SubscriptionToCreate};

	#[tokio::test]
	async fn test_list_subscriptions_empty() {
		let mut txn = create_test_command_transaction().await;

		let result = CatalogStore::list_subscriptions_all(&mut txn).await.unwrap();
		assert!(result.is_empty());
	}

	#[tokio::test]
	async fn test_list_subscriptions() {
		let mut txn = create_test_command_transaction().await;

		let sub1 = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.await
		.unwrap();

		let sub2 = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.await
		.unwrap();

		let sub3 = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.await
		.unwrap();

		let result = CatalogStore::list_subscriptions_all(&mut txn).await.unwrap();
		assert_eq!(result.len(), 3);

		// Verify all have unique IDs (order may vary due to key encoding)
		let ids: Vec<SubscriptionId> = result.iter().map(|s| s.id).collect();
		assert!(ids.contains(&sub1.id));
		assert!(ids.contains(&sub2.id));
		assert!(ids.contains(&sub3.id));
	}
}
