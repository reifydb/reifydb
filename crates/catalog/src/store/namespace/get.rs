// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	Error,
	interface::{NamespaceDef, NamespaceId, QueryTransaction},
};
use reifydb_type::internal;

use crate::CatalogStore;

impl CatalogStore {
	pub async fn get_namespace(
		rx: &mut impl QueryTransaction,
		namespace: NamespaceId,
	) -> crate::Result<NamespaceDef> {
		CatalogStore::find_namespace(rx, namespace).await?.ok_or_else(|| {
			Error(internal!(
				"Namespace with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				namespace
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{CatalogStore, store::namespace::NamespaceId, test_utils::create_namespace};

	#[tokio::test]
	async fn test_ok() {
		let mut txn = create_test_command_transaction().await;

		create_namespace(&mut txn, "namespace_one").await;
		create_namespace(&mut txn, "namespace_two").await;
		create_namespace(&mut txn, "namespace_three").await;

		let result = CatalogStore::get_namespace(&mut txn, NamespaceId(1026)).await.unwrap();

		assert_eq!(result.id, NamespaceId(1026));
		assert_eq!(result.name, "namespace_two");
	}

	#[tokio::test]
	async fn test_not_found() {
		let mut txn = create_test_command_transaction().await;

		create_namespace(&mut txn, "namespace_one").await;
		create_namespace(&mut txn, "namespace_two").await;
		create_namespace(&mut txn, "namespace_three").await;

		let err = CatalogStore::get_namespace(&mut txn, NamespaceId(23)).await.unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("NamespaceId(23)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
