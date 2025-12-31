// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::{
	Error,
	interface::{ViewDef, ViewId},
};
use reifydb_transaction::IntoStandardTransaction;
use reifydb_type::internal;

use crate::CatalogStore;

impl CatalogStore {
	pub async fn get_view(rx: &mut impl IntoStandardTransaction, view: ViewId) -> crate::Result<ViewDef> {
		CatalogStore::find_view(rx, view).await?.ok_or_else(|| {
			Error(internal!(
				"View with ID {:?} not found in catalog. This indicates a critical catalog inconsistency.",
				view
			))
		})
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::{NamespaceId, ViewId};
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_namespace, create_view, ensure_test_namespace},
	};

	#[tokio::test]
	async fn test_ok() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_namespace(&mut txn).await;
		create_namespace(&mut txn, "namespace_one").await;
		create_namespace(&mut txn, "namespace_two").await;
		create_namespace(&mut txn, "namespace_three").await;

		create_view(&mut txn, "namespace_one", "view_one", &[]).await;
		create_view(&mut txn, "namespace_two", "view_two", &[]).await;
		create_view(&mut txn, "namespace_three", "view_three", &[]).await;

		let result = CatalogStore::get_view(&mut txn, ViewId(1026)).await.unwrap();

		assert_eq!(result.id, ViewId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "view_two");
	}

	#[tokio::test]
	async fn test_not_found() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_namespace(&mut txn).await;
		create_namespace(&mut txn, "namespace_one").await;
		create_namespace(&mut txn, "namespace_two").await;
		create_namespace(&mut txn, "namespace_three").await;

		create_view(&mut txn, "namespace_one", "view_one", &[]).await;
		create_view(&mut txn, "namespace_two", "view_two", &[]).await;
		create_view(&mut txn, "namespace_three", "view_three", &[]).await;

		let err = CatalogStore::get_view(&mut txn, ViewId(42)).await.unwrap_err();

		assert_eq!(err.code, "INTERNAL_ERROR");
		assert!(err.message.contains("ViewId(42)"));
		assert!(err.message.contains("not found in catalog"));
	}
}
