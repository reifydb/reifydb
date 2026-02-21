// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::id::SubscriptionId,
	key::{
		subscription::SubscriptionKey, subscription_column::SubscriptionColumnKey,
		subscription_row::SubscriptionRowKey,
	},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn delete_subscription(
		txn: &mut AdminTransaction,
		subscription: SubscriptionId,
	) -> crate::Result<()> {
		// Step 1: Delete subscription columns
		let col_range = SubscriptionColumnKey::subscription_range(subscription);
		let mut col_stream = txn.range(col_range, 1000)?;
		let mut col_keys = Vec::new();
		while let Some(entry) = col_stream.next() {
			let entry = entry?;
			col_keys.push(entry.key.clone());
		}
		drop(col_stream);
		for key in col_keys {
			txn.remove(&key)?;
		}

		// Step 2: Delete subscription rows (unconsumed deltas)
		let row_range = SubscriptionRowKey::full_scan(subscription);
		let mut row_stream = txn.range(row_range, 10000)?;
		let mut row_keys = Vec::new();
		while let Some(entry) = row_stream.next() {
			let entry = entry?;
			row_keys.push(entry.key.clone());
		}
		drop(row_stream);
		for key in row_keys {
			txn.remove(&key)?;
		}

		// Step 3: Delete the subscription metadata
		txn.remove(&SubscriptionKey::encoded(subscription))?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::SubscriptionId;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{CatalogStore, store::subscription::create::SubscriptionToCreate};

	#[test]
	fn test_delete_subscription() {
		let mut txn = create_test_admin_transaction();

		let created = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.unwrap();

		// Verify it exists
		let found = CatalogStore::find_subscription(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_some());

		// Delete it
		CatalogStore::delete_subscription(&mut txn, created.id).unwrap();

		// Verify it's gone
		let found = CatalogStore::find_subscription(&mut Transaction::Admin(&mut txn), created.id).unwrap();
		assert!(found.is_none());
	}

	#[test]
	fn test_delete_nonexistent_subscription() {
		let mut txn = create_test_admin_transaction();

		let non_existent = SubscriptionId(999999);
		let result = CatalogStore::delete_subscription(&mut txn, non_existent);
		assert!(result.is_ok());
	}
}
