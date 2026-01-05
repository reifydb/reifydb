// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	Error,
	interface::{SubscriptionDef, SubscriptionId},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::internal;

use crate::CatalogStore;

impl CatalogStore {
	pub async fn get_subscription(
		rx: &mut impl IntoStandardTransaction,
		subscription: SubscriptionId,
	) -> crate::Result<SubscriptionDef> {
		CatalogStore::find_subscription(rx, subscription).await?.ok_or_else(|| {
			Error(internal!(
				"Subscription with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				subscription
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::SubscriptionId;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, store::subscription::SubscriptionToCreate};

	#[tokio::test]
	async fn test_get_subscription_ok() {
		let mut txn = create_test_command_transaction().await;

		let created = CatalogStore::create_subscription(
			&mut txn,
			SubscriptionToCreate {
				columns: vec![],
			},
		)
		.await
		.unwrap();

		let result = CatalogStore::get_subscription(&mut txn, created.id).await.unwrap();
		assert_eq!(result.id, created.id);
	}

	#[tokio::test]
	async fn test_get_subscription_not_found() {
		let mut txn = create_test_command_transaction().await;

		let non_existent = SubscriptionId::new();
		let err = CatalogStore::get_subscription(&mut txn, non_existent).await.unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("not found in catalog"));
	}
}
