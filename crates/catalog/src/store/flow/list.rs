// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::{
	interface::catalog::{
		flow::{FlowDef, FlowStatus},
		id::NamespaceId,
	},
	key::{Key, flow::FlowKey},
};
use reifydb_transaction::transaction::Transaction;

use crate::{CatalogStore, Result, store::flow::schema::flow};

impl CatalogStore {
	pub(crate) fn list_flows_all(rx: &mut Transaction<'_>) -> Result<Vec<FlowDef>> {
		let mut result = Vec::new();

		let mut stream = rx.range(FlowKey::full_scan(), 1024)?;

		while let Some(entry) = stream.next() {
			let entry = entry?;
			if let Some(key) = Key::decode(&entry.key) {
				if let Key::Flow(flow_key) = key {
					let flow_id = flow_key.flow;

					let namespace_id =
						NamespaceId(flow::SCHEMA.get_u64(&entry.values, flow::NAMESPACE));
					let name = flow::SCHEMA.get_utf8(&entry.values, flow::NAME).to_string();
					let status_u8 = flow::SCHEMA.get_u8(&entry.values, flow::STATUS);
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
pub mod tests {
	use reifydb_core::interface::catalog::flow::FlowStatus;
	use reifydb_engine::test_utils::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_type::fragment::Fragment;

	use crate::{
		CatalogStore,
		store::flow::create::FlowToCreate,
		test_utils::{create_flow, create_namespace},
	};

	#[test]
	fn test_list_flows_all() {
		let mut txn = create_test_admin_transaction();
		let namespace_one = create_namespace(&mut txn, "namespace_one");
		let namespace_two = create_namespace(&mut txn, "namespace_two");

		create_flow(&mut txn, "namespace_one", "flow_one");
		create_flow(&mut txn, "namespace_one", "flow_two");
		create_flow(&mut txn, "namespace_two", "flow_three");

		let result = CatalogStore::list_flows_all(&mut Transaction::Admin(&mut txn)).unwrap();
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

	#[test]
	fn test_list_flows_empty() {
		let mut txn = create_test_admin_transaction();

		let result = CatalogStore::list_flows_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(result.len(), 0);
	}

	#[test]
	fn test_list_flows_all_with_different_statuses() {
		let mut txn = create_test_admin_transaction();
		create_namespace(&mut txn, "test_namespace");

		// Create flows with different statuses
		create_flow(&mut txn, "test_namespace", "active_flow");

		// Create a paused flow by directly using CatalogStore

		let namespace =
			CatalogStore::find_namespace_by_name(&mut Transaction::Admin(&mut txn), "test_namespace")
				.unwrap()
				.unwrap();
		CatalogStore::create_flow(
			&mut txn,
			FlowToCreate {
				name: Fragment::internal("paused_flow"),
				namespace: namespace.id,
				status: FlowStatus::Paused,
			},
		)
		.unwrap();

		let result = CatalogStore::list_flows_all(&mut Transaction::Admin(&mut txn)).unwrap();
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
