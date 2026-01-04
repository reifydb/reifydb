// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use futures_util::StreamExt;
use reifydb_core::interface::{NamespaceId, NamespaceViewKey, ViewDef, ViewId, ViewKey, ViewKind};
use reifydb_transaction::IntoStandardTransaction;

use crate::{
	CatalogStore,
	store::view::layout::{view, view_namespace},
};

impl CatalogStore {
	pub async fn find_view(rx: &mut impl IntoStandardTransaction, id: ViewId) -> crate::Result<Option<ViewDef>> {
		let mut txn = rx.into_standard_transaction();
		let Some(multi) = txn.get(&ViewKey::encoded(id)).await? else {
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
			columns: Self::list_columns(&mut txn, id).await?,
			primary_key: Self::find_view_primary_key(&mut txn, id).await?,
		}))
	}

	pub async fn find_view_by_name(
		rx: &mut impl IntoStandardTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<ViewDef>> {
		let name = name.as_ref();
		let mut txn = rx.into_standard_transaction();
		let mut stream = txn.range(NamespaceViewKey::full_scan(namespace), 1024)?;

		let mut found_view = None;
		while let Some(entry) = stream.next().await {
			let multi = entry?;
			let row = &multi.values;
			let view_name = view_namespace::LAYOUT.get_utf8(row, view_namespace::NAME);
			if name == view_name {
				found_view = Some(ViewId(view_namespace::LAYOUT.get_u64(row, view_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(view) = found_view else {
			return Ok(None);
		};

		Ok(Some(Self::get_view(&mut txn, view).await?))
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
