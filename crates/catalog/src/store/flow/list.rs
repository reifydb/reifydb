// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use reifydb_core::interface::{FlowDef, FlowKey, FlowStatus, Key, NamespaceId, QueryTransaction};

use crate::{CatalogStore, store::flow::layout::flow};

impl CatalogStore {
	pub async fn list_flows_all(rx: &mut impl QueryTransaction) -> crate::Result<Vec<FlowDef>> {
		let mut result = Vec::new();

		let batch = rx.range(FlowKey::full_scan()).await?;

		for entry in batch.items {
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::Flow(flow_key) = key {
					let flow_id = flow_key.flow;

					let namespace_id =
						NamespaceId(flow::LAYOUT.get_u64(&entry.values, flow::NAMESPACE));

					let name = flow::LAYOUT.get_utf8(&entry.values, flow::NAME).to_string();

					let status_u8 = flow::LAYOUT.get_u8(&entry.values, flow::STATUS);
					let status = FlowStatus::from_u8(status_u8);

					let flow_def = FlowDef {
						id: flow_id,
						namespace: namespace_id,
						name,
						status,
					};

					result.push(flow_def);
				}
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::interface::FlowStatus;
	use reifydb_engine::test_utils::create_test_command_transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow, create_namespace},
	};

	#[tokio::test]
	fn test_list_flows_all() {
		let mut txn = create_test_command_transaction().await;
		let namespace_one = create_namespace(&mut txn, "namespace_one").await;
		let namespace_two = create_namespace(&mut txn, "namespace_two").await;

		create_flow(&mut txn, "namespace_one", "flow_one");
		create_flow(&mut txn, "namespace_one", "flow_two");
		create_flow(&mut txn, "namespace_two", "flow_three");

		let result = CatalogStore::list_flows_all(&mut txn).unwrap();
		assert_eq!(result.len(), 3);

		// Verify all flows are present (order may vary)
		let flow_names: Vec<_> = result.iter().map(|f| f.name.as_str()).collect();
		assert!(flow_names.contains(&"flow_one"));
		assert!(flow_names.contains(&"flow_two"));
		assert!(flow_names.contains(&"flow_three"));

		// Verify namespaces and status for each flow
		for flow in &result {
			match flow.name.as_str() {
				"flow_one" => {
					assert_eq!(flow.namespace, namespace_one.id);
					assert_eq!(flow.status, FlowStatus::Active);
				}
				"flow_two" => {
					assert_eq!(flow.namespace, namespace_one.id);
				}
				"flow_three" => {
					assert_eq!(flow.namespace, namespace_two.id);
				}
				_ => panic!("Unexpected flow name: {}", flow.name),
			}
		}
	}

	#[tokio::test]
	fn test_list_flows_empty() {
		let mut txn = create_test_command_transaction().await;

		let result = CatalogStore::list_flows_all(&mut txn).unwrap();
		assert_eq!(result.len(), 0);
	}

	#[tokio::test]
	fn test_list_flows_all_with_different_statuses() {
		let mut txn = create_test_command_transaction().await;
		create_namespace(&mut txn, "test_namespace").await;

		// Create flows with different statuses
		create_flow(&mut txn, "test_namespace", "active_flow");

		// Create a paused flow by directly using CatalogStore
		use crate::store::flow::create::FlowToCreate;
		let namespace = CatalogStore::find_namespace_by_name(&mut txn, "test_namespace").unwrap().unwrap();
		CatalogStore::create_flow(
			&mut txn,
			FlowToCreate {
				fragment: None,
				name: "paused_flow".to_string(),
				namespace: namespace.id,
				status: FlowStatus::Paused,
			},
		)
		.unwrap();

		let result = CatalogStore::list_flows_all(&mut txn).unwrap();
		assert_eq!(result.len(), 2);

		// Verify both flows are present with correct statuses (order may vary)
		for flow in &result {
			match flow.name.as_str() {
				"active_flow" => assert_eq!(flow.status, FlowStatus::Active),
				"paused_flow" => assert_eq!(flow.status, FlowStatus::Paused),
				_ => panic!("Unexpected flow name: {}", flow.name),
			}
		}
	}
}
