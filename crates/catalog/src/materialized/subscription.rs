// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{id::SubscriptionId, subscription::SubscriptionDef},
};

use crate::materialized::{MaterializedCatalog, MultiVersionSubscriptionDef};

impl MaterializedCatalog {
	/// Find a subscription by ID at a specific version
	pub fn find_subscription(
		&self,
		subscription: SubscriptionId,
		version: CommitVersion,
	) -> Option<SubscriptionDef> {
		self.subscriptions.get(&subscription).and_then(|entry| {
			let multi = entry.value();
			multi.get(version)
		})
	}

	pub fn set_subscription(
		&self,
		id: SubscriptionId,
		version: CommitVersion,
		subscription: Option<SubscriptionDef>,
	) {
		let multi = self.subscriptions.get_or_insert_with(id, MultiVersionSubscriptionDef::new);
		if let Some(new) = subscription {
			multi.value().insert(version, new);
		} else {
			multi.value().remove(version);
		}
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::catalog::{id::SubscriptionColumnId, subscription::SubscriptionColumnDef},
	};
	use reifydb_type::value::r#type::Type;

	use super::*;

	fn create_test_subscription(id: SubscriptionId) -> SubscriptionDef {
		SubscriptionDef {
			id,
			columns: vec![
				SubscriptionColumnDef {
					id: SubscriptionColumnId(0),
					name: "id".to_string(),
					ty: Type::Int1,
				},
				SubscriptionColumnDef {
					id: SubscriptionColumnId(1),
					name: "name".to_string(),
					ty: Type::Utf8,
				},
			],
			primary_key: None,
			acknowledged_version: CommitVersion(0),
		}
	}

	#[test]
	fn test_set_and_find_subscription() {
		let catalog = MaterializedCatalog::new();
		let subscription_id = SubscriptionId(1);
		let subscription = create_test_subscription(subscription_id);

		// Set subscription at version 1
		catalog.set_subscription(subscription_id, CommitVersion(1), Some(subscription.clone()));

		// Find subscription at version 1
		let found = catalog.find_subscription(subscription_id, CommitVersion(1));
		assert_eq!(found, Some(subscription.clone()));

		// Find subscription at later version (should return same subscription)
		let found = catalog.find_subscription(subscription_id, CommitVersion(5));
		assert_eq!(found, Some(subscription));

		// Subscription shouldn't exist at version 0
		let found = catalog.find_subscription(subscription_id, CommitVersion(0));
		assert_eq!(found, None);
	}

	#[test]
	fn test_subscription_deletion() {
		let catalog = MaterializedCatalog::new();
		let subscription_id = SubscriptionId(2);

		// Create and set subscription
		let subscription = create_test_subscription(subscription_id);
		catalog.set_subscription(subscription_id, CommitVersion(1), Some(subscription.clone()));

		// Verify it exists
		assert_eq!(catalog.find_subscription(subscription_id, CommitVersion(1)), Some(subscription.clone()));

		// Delete the subscription
		catalog.set_subscription(subscription_id, CommitVersion(2), None);

		// Should not exist at version 2
		assert_eq!(catalog.find_subscription(subscription_id, CommitVersion(2)), None);

		// Should still exist at version 1 (historical)
		assert_eq!(catalog.find_subscription(subscription_id, CommitVersion(1)), Some(subscription));
	}

	#[test]
	fn test_multiple_subscriptions() {
		let catalog = MaterializedCatalog::new();

		let id1 = SubscriptionId(10);
		let id2 = SubscriptionId(11);
		let id3 = SubscriptionId(12);

		let sub1 = create_test_subscription(id1);
		let sub2 = create_test_subscription(id2);
		let sub3 = create_test_subscription(id3);

		// Set multiple subscriptions
		catalog.set_subscription(id1, CommitVersion(1), Some(sub1.clone()));
		catalog.set_subscription(id2, CommitVersion(1), Some(sub2.clone()));
		catalog.set_subscription(id3, CommitVersion(1), Some(sub3.clone()));

		// All should be findable by ID
		assert_eq!(catalog.find_subscription(id1, CommitVersion(1)), Some(sub1));
		assert_eq!(catalog.find_subscription(id2, CommitVersion(1)), Some(sub2));
		assert_eq!(catalog.find_subscription(id3, CommitVersion(1)), Some(sub3));
	}

	#[test]
	fn test_subscription_version_isolation() {
		let catalog = MaterializedCatalog::new();
		let subscription_id = SubscriptionId(20);

		// Create subscription v1 with one column
		let subscription_v1 = SubscriptionDef {
			id: subscription_id,
			columns: vec![SubscriptionColumnDef {
				id: SubscriptionColumnId(0),
				name: "id".to_string(),
				ty: Type::Int1,
			}],
			primary_key: None,
			acknowledged_version: CommitVersion(0),
		};
		catalog.set_subscription(subscription_id, CommitVersion(1), Some(subscription_v1.clone()));

		// Create subscription v2 with two columns
		let subscription_v2 = SubscriptionDef {
			id: subscription_id,
			columns: vec![
				SubscriptionColumnDef {
					id: SubscriptionColumnId(0),
					name: "id".to_string(),
					ty: Type::Int1,
				},
				SubscriptionColumnDef {
					id: SubscriptionColumnId(1),
					name: "value".to_string(),
					ty: Type::Utf8,
				},
			],
			primary_key: None,
			acknowledged_version: CommitVersion(0),
		};
		catalog.set_subscription(subscription_id, CommitVersion(2), Some(subscription_v2.clone()));

		// Historical query at version 1 should show v1
		assert_eq!(catalog.find_subscription(subscription_id, CommitVersion(1)), Some(subscription_v1));

		// Query at version 2+ should show v2
		assert_eq!(catalog.find_subscription(subscription_id, CommitVersion(2)), Some(subscription_v2));
	}
}
