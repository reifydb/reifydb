// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use futures_util::StreamExt;
use reifydb_core::interface::{FlowDef, FlowId, FlowKey, FlowStatus, NamespaceFlowKey, NamespaceId};
use reifydb_transaction::IntoStandardTransaction;

use crate::{
	CatalogStore,
	store::flow::layout::{flow, flow_namespace},
};

impl CatalogStore {
	pub async fn find_flow(rx: &mut impl IntoStandardTransaction, id: FlowId) -> crate::Result<Option<FlowDef>> {
		let mut txn = rx.into_standard_transaction();
		let Some(multi) = txn.get(&FlowKey::encoded(id)).await? else {
			return Ok(None);
		};

		let row = multi.values;
		let id = FlowId(flow::LAYOUT.get_u64(&row, flow::ID));
		let namespace = NamespaceId(flow::LAYOUT.get_u64(&row, flow::NAMESPACE));
		let name = flow::LAYOUT.get_utf8(&row, flow::NAME).to_string();
		let status_u8 = flow::LAYOUT.get_u8(&row, flow::STATUS);
		let status = FlowStatus::from_u8(status_u8);

		Ok(Some(FlowDef {
			id,
			name,
			namespace,
			status,
		}))
	}

	pub async fn find_flow_by_name(
		rx: &mut impl IntoStandardTransaction,
		namespace: NamespaceId,
		name: impl AsRef<str>,
	) -> crate::Result<Option<FlowDef>> {
		let name = name.as_ref();
		let mut txn = rx.into_standard_transaction();
		let mut stream = txn.range(NamespaceFlowKey::full_scan(namespace), 1024)?;

		let mut found_flow = None;
		while let Some(entry) = stream.next().await {
			let multi = entry?;
			let row = &multi.values;
			let flow_name = flow_namespace::LAYOUT.get_utf8(row, flow_namespace::NAME);
			if name == flow_name {
				found_flow = Some(FlowId(flow_namespace::LAYOUT.get_u64(row, flow_namespace::ID)));
				break;
			}
		}

		drop(stream);

		let Some(flow) = found_flow else {
			return Ok(None);
		};

		Ok(Some(Self::get_flow(&mut txn, flow).await?))
	}
}

#[cfg(test)]
mod tests {
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow, create_namespace, ensure_test_namespace},
	};

	#[tokio::test]
	async fn test_find_flow_by_name_ok() {
		let mut txn = create_test_command_transaction().await;
		let _namespace_one = create_namespace(&mut txn, "namespace_one").await;
		let namespace_two = create_namespace(&mut txn, "namespace_two").await;

		create_flow(&mut txn, "namespace_one", "flow_one").await;
		create_flow(&mut txn, "namespace_two", "flow_two").await;

		let result =
			CatalogStore::find_flow_by_name(&mut txn, namespace_two.id, "flow_two").await.unwrap().unwrap();
		assert_eq!(result.name, "flow_two");
		assert_eq!(result.namespace, namespace_two.id);
	}

	#[tokio::test]
	async fn test_find_flow_by_name_empty() {
		let mut txn = create_test_command_transaction().await;
		let test_namespace = ensure_test_namespace(&mut txn).await;

		let result = CatalogStore::find_flow_by_name(&mut txn, test_namespace.id, "some_flow").await.unwrap();
		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_find_flow_by_name_not_found() {
		let mut txn = create_test_command_transaction().await;
		let test_namespace = ensure_test_namespace(&mut txn).await;

		create_flow(&mut txn, "test_namespace", "flow_one").await;
		create_flow(&mut txn, "test_namespace", "flow_two").await;

		let result = CatalogStore::find_flow_by_name(&mut txn, test_namespace.id, "flow_three").await.unwrap();
		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_find_flow_by_name_different_namespace() {
		let mut txn = create_test_command_transaction().await;
		let _namespace_one = create_namespace(&mut txn, "namespace_one").await;
		let namespace_two = create_namespace(&mut txn, "namespace_two").await;

		create_flow(&mut txn, "namespace_one", "my_flow").await;

		// Flow exists in namespace_one but not in namespace_two
		let result = CatalogStore::find_flow_by_name(&mut txn, namespace_two.id, "my_flow").await.unwrap();
		assert!(result.is_none());
	}

	#[tokio::test]
	async fn test_find_flow_by_name_case_sensitive() {
		let mut txn = create_test_command_transaction().await;
		let test_namespace = ensure_test_namespace(&mut txn).await;

		create_flow(&mut txn, "test_namespace", "MyFlow").await;

		// Flow names are case-sensitive
		let result = CatalogStore::find_flow_by_name(&mut txn, test_namespace.id, "myflow").await.unwrap();
		assert!(result.is_none());

		let result = CatalogStore::find_flow_by_name(&mut txn, test_namespace.id, "MyFlow").await.unwrap();
		assert!(result.is_some());
	}

	#[tokio::test]
	async fn test_find_flow_by_id() {
		let mut txn = create_test_command_transaction().await;
		ensure_test_namespace(&mut txn).await;

		let flow = create_flow(&mut txn, "test_namespace", "test_flow").await;

		let result = CatalogStore::find_flow(&mut txn, flow.id).await.unwrap().unwrap();
		assert_eq!(result.id, flow.id);
		assert_eq!(result.name, "test_flow");
	}

	#[tokio::test]
	async fn test_find_flow_by_id_not_found() {
		let mut txn = create_test_command_transaction().await;

		let result = CatalogStore::find_flow(&mut txn, 999.into()).await.unwrap();
		assert!(result.is_none());
	}
}
