// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{id::SubscriptionId, subscription::SubscriptionDef},
	internal,
};
use reifydb_transaction::transaction::AsTransaction;
use reifydb_type::error::Error;

use crate::CatalogStore;

impl CatalogStore {
	pub(crate) fn get_subscription(
		rx: &mut impl AsTransaction,
		subscription: SubscriptionId,
	) -> crate::Result<SubscriptionDef> {
		CatalogStore::find_subscription(rx, subscription)?.ok_or_else(|| {
			Error(internal!(
				"Subscription with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				subscription
			))
		})
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::id::SubscriptionId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, store::subscription::create::SubscriptionToCreate};

	#[test]
	fn test_get_subscription_ok() {
		let mut txn = create_test_command_transaction();

		let created = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.unwrap();

		let result = CatalogStore::get_subscription(&mut txn, created.id).unwrap();
		assert_eq!(result.id, created.id);
	}

	#[test]
	fn test_get_subscription_not_found() {
		let mut txn = create_test_command_transaction();

		let non_existent = SubscriptionId(999999);
		let err = CatalogStore::get_subscription(&mut txn, non_existent).unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("not found in catalog"));
	}
}
