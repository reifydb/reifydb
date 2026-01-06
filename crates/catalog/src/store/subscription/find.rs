// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	CommitVersion,
	interface::{SubscriptionDef, SubscriptionId, SubscriptionKey},
};
use reifydb_transaction::{IntoStandardTransaction, StandardTransaction};

use crate::{CatalogStore, store::subscription::layout::subscription};

impl CatalogStore {
	pub fn find_subscription(
		rx: &mut impl IntoStandardTransaction,
		id: SubscriptionId,
	) -> crate::Result<Option<SubscriptionDef>> {
		let mut txn = rx.into_standard_transaction();
		let Some(multi) = txn.get(&SubscriptionKey::encoded(id))? else {
			return Ok(None);
		};

		let row = multi.values;
		let uuid = subscription::LAYOUT.get_uuid7(&row, subscription::ID);
		let id = SubscriptionId(uuid.into());
		let acknowledged_version =
			CommitVersion(subscription::LAYOUT.get_u64(&row, subscription::ACKNOWLEDGED_VERSION));

		// Load columns using the new subscription column storage
		let columns = match &mut txn {
			StandardTransaction::Command(cmd) => Self::list_subscription_columns(cmd, id)?,
			StandardTransaction::Query(_) => {
				// For query transactions, we can't use StandardCommandTransaction
				// This is a limitation - for now return empty columns for queries
				vec![]
			}
		};

		Ok(Some(SubscriptionDef {
			id,
			columns,
			// Subscriptions don't have primary keys (they use UUID v7 as their identifier)
			primary_key: None,
			acknowledged_version,
		}))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::SubscriptionId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, store::subscription::SubscriptionToCreate};

	#[test]
	fn test_find_subscription_by_id() {
		let mut txn = create_test_command_transaction();

		let created = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.unwrap();

		let found = CatalogStore::find_subscription(&mut txn, created.id).unwrap().unwrap();
		assert_eq!(found.id, created.id);
	}

	#[test]
	fn test_find_subscription_not_found() {
		let mut txn = create_test_command_transaction();

		// Generate a random UUID that doesn't exist
		let non_existent = SubscriptionId::new();
		let result = CatalogStore::find_subscription(&mut txn, non_existent).unwrap();
		assert!(result.is_none());
	}
}
