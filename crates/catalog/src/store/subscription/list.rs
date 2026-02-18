// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::subscription::SubscriptionDef,
	key::{Key, subscription::SubscriptionKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, store::subscription::schema::subscription};

impl CatalogStore {
	pub(crate) fn list_subscriptions_all(rx: &mut Transaction<'_>) -> crate::Result<Vec<SubscriptionDef>> {
		// First, collect all subscription IDs and metadata
		let mut subscription_data = Vec::new();
		{
			let mut stream = rx.range(SubscriptionKey::full_scan(), 1024)?;

			while let Some(result_entry) = stream.next() {
				let entry = result_entry?;
				if let Some(key) = Key::decode(&entry.key) {
					if let Key::Subscription(sub_key) = key {
						let subscription_id = sub_key.subscription;

						let acknowledged_version =
							CommitVersion(subscription::SCHEMA.get_u64(
								&entry.values,
								subscription::ACKNOWLEDGED_VERSION,
							));

						subscription_data.push((subscription_id, acknowledged_version));
					}
				}
			}
		} // stream dropped here, releasing the borrow on rx

		// Now load columns for each subscription
		let mut result = Vec::new();
		for (subscription_id, acknowledged_version) in subscription_data {
			// Load columns (works for all transaction types)
			let columns = Self::list_subscription_columns(rx, subscription_id)?;

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

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::SubscriptionId;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{CatalogStore, store::subscription::create::SubscriptionToCreate};

	#[test]
	fn test_list_subscriptions_empty() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::list_subscriptions_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert!(result.is_empty());
	}

	#[test]
	fn test_list_subscriptions() {
		let mut txn = create_test_admin_transaction();

		let sub1 = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.unwrap();

		let sub2 = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.unwrap();

		let sub3 = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.unwrap();

		let result = CatalogStore::list_subscriptions_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(result.len(), 3);

		// Verify all have unique IDs (order may vary due to key encoding)
		let ids: Vec<SubscriptionId> = result.iter().map(|s| s.id).collect();
		assert!(ids.contains(&sub1.id));
		assert!(ids.contains(&sub2.id));
		assert!(ids.contains(&sub3.id));
	}
}
