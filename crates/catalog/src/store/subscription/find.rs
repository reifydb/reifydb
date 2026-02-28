// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	common::CommitVersion,
	interface::catalog::{id::SubscriptionId, subscription::SubscriptionDef},
	key::subscription::SubscriptionKey,
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::subscription::schema::subscription};

impl CatalogStore {
	pub(crate) fn find_subscription(
		rx: &mut Transaction<'_>,
		id: SubscriptionId,
	) -> Result<Option<SubscriptionDef>> {
		let Some(multi) = rx.get(&SubscriptionKey::encoded(id))? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = SubscriptionId(subscription::SCHEMA.get_u64(&row, subscription::ID));
		let acknowledged_version =
			CommitVersion(subscription::SCHEMA.get_u64(&row, subscription::ACKNOWLEDGED_VERSION));

		let columns = Self::list_subscription_columns(rx, id)?;

		Ok(Some(SubscriptionDef {
			id,
			columns,
			primary_key: None,
			acknowledged_version,
		}))
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::SubscriptionId;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{CatalogStore, store::subscription::create::SubscriptionToCreate};

	#[test]
	fn test_find_subscription_by_id() {
		let mut txn = create_test_admin_transaction();

		let created = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.unwrap();

		let found = CatalogStore::find_subscription(&mut Transaction::Admin(&mut txn), created.id)
			.unwrap()
			.unwrap();
		assert_eq!(found.id, created.id);
	}

	#[test]
	fn test_find_subscription_not_found() {
		let mut txn = create_test_admin_transaction();

		let non_existent = SubscriptionId(999999);
		let result = CatalogStore::find_subscription(&mut Transaction::Admin(&mut txn), non_existent).unwrap();
		assert!(result.is_none());
	}
}
