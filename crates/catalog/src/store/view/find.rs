// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::{
	MultiVersionValues, NamespaceId, NamespaceViewKey, QueryTransaction, ViewDef, ViewId, ViewKey, ViewKind,
};

use crate::{
	CatalogStore,
	store::view::layout::{view, view_namespace},
};

impl CatalogStore {
	pub async fn find_view(rx: &mut impl QueryTransaction, id: ViewId) -> crate::Result<Option<ViewDef>> {
		let Some(multi) = rx.get(&ViewKey::encoded(id)).await? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = ViewId(view::LAYOUT.get_u64(&row, view::ID));
		let namespace = NamespaceId(view::LAYOUT.get_u64(&row, view::NAMESPACE));
		let name = view::LAYOUT.get_utf8(&row, view::NAME).to_string();

		let kind = match view::LAYOUT.get_u8(&row, view::KIND) {
			0 => ViewKind::Deferred,
			1 => ViewKind::Transactional,
			_ => unimplemented!(),
		};

		Ok(Some(ViewDef {
			id,
			name,
			namespace,
			kind,
			columns: Self::list_columns(rx, id).await?,
			primary_key: Self::find_view_primary_key(rx, id).await?,
		}))
	}

	pub async fn find_view_by_name(
		rx: &mut impl QueryTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ViewDef>> {
		let name = name.as_ref();
		let batch = rx.range(NamespaceViewKey::full_scan(namespace)).await?;
		let Some(view) = batch.items.iter().find_map(|multi: &MultiVersionValues| {
			let row = &multi.values;
			let view_name = view_namespace::LAYOUT.get_utf8(row, view_namespace::NAME);
			if name == view_name {
				Some(ViewId(view_namespace::LAYOUT.get_u64(row, view_namespace::ID)))
			} else {
				None
			}
		}) else {
			return Ok(None);
		};

		Ok(Some(Self::get_view(rx, view).await?))
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

		let result = CatalogStore::find_view_by_name(&mut txn, NamespaceId(1027), "view_two")
			.await
			.unwrap()
			.unwrap();
		assert_eq!(result.id, ViewId(1026));
		assert_eq!(result.namespace, NamespaceId(1027));
		assert_eq!(result.name, "view_two");
	}

	#[tokio::test]
	async fn test_empty() {
		let mut txn = create_test_command_transaction().await;

		let result = CatalogStore::find_view_by_name(&mut txn, NamespaceId(1025), "some_view").await.unwrap();
		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_not_found_different_view() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_namespace(&mut txn).await;
		create_namespace(&mut txn, "namespace_one").await;
		create_namespace(&mut txn, "namespace_two").await;
		create_namespace(&mut txn, "namespace_three").await;

		create_view(&mut txn, "namespace_one", "view_one", &[]).await;
		create_view(&mut txn, "namespace_two", "view_two", &[]).await;
		create_view(&mut txn, "namespace_three", "view_three", &[]).await;

		let result =
			CatalogStore::find_view_by_name(&mut txn, NamespaceId(1025), "view_four_two").await.unwrap();
		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_not_found_different_namespace() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_namespace(&mut txn).await;
		create_namespace(&mut txn, "namespace_one").await;
		create_namespace(&mut txn, "namespace_two").await;
		create_namespace(&mut txn, "namespace_three").await;

		create_view(&mut txn, "namespace_one", "view_one", &[]).await;
		create_view(&mut txn, "namespace_two", "view_two", &[]).await;
		create_view(&mut txn, "namespace_three", "view_three", &[]).await;

		let result = CatalogStore::find_view_by_name(&mut txn, NamespaceId(2), "view_two").await.unwrap();
		assert!(result.is_none());
	}
}
