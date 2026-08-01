// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::flow::{FlowId, Operator, OperatorId},
	key::{
		EncodableKey,
		operator::{OperatorByFlowKey, OperatorKey},
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};

use crate::{
	CatalogStore, Result,
	store::operator::shape::{operator, operator_by_flow},
};

impl CatalogStore {
	pub fn list_operators_by_flow(rx: &mut Transaction<'_>, flow_id: FlowId) -> Result<Vec<Operator>> {
		let mut node_ids = Vec::new();
		{
			let stream = rx.range(OperatorByFlowKey::full_scan(flow_id), RangeScope::All, 1024)?;
			for entry in stream {
				let multi = entry?;
				node_ids.push(OperatorId(
					operator_by_flow::SHAPE.get::<u64>(&multi.row, operator_by_flow::ID),
				));
			}
		}

		let mut nodes = Vec::new();
		for node_id in node_ids {
			if let Some(node) = Self::find_operator(rx, node_id)? {
				nodes.push(node);
			}
		}

		Ok(nodes)
	}

	pub(crate) fn list_operators_all(rx: &mut Transaction<'_>) -> Result<Vec<Operator>> {
		let mut result = Vec::new();

		let stream = rx.range(OperatorKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let entry = entry?;
			if let Some(operator_key) = OperatorKey::decode(&entry.key) {
				let node_id = operator_key.node;
				let flow_id = FlowId(operator::SHAPE.get::<u64>(&entry.row, operator::FLOW));
				let node_type = operator::SHAPE.get::<u8>(&entry.row, operator::TYPE);
				let data = operator::SHAPE.get_blob(&entry.row, operator::DATA).clone();

				let node_def = Operator {
					id: node_id,
					flow: flow_id,
					node_type,
					data,
				};

				result.push(node_def);
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow, create_operator, create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_list_operators_by_flow() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node = create_operator(&mut txn, flow.id, 1, &[0x01]);

		let nodes = CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(nodes.len(), 1);
		assert_eq!(nodes[0].id, node.id);
	}

	#[test]
	fn test_list_operators_by_flow_empty() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let nodes = CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert!(nodes.is_empty());
	}

	#[test]
	fn test_list_operators_by_flow_multiple() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_operator(&mut txn, flow.id, 1, &[0x01]);
		let node2 = create_operator(&mut txn, flow.id, 4, &[0x02]);
		let node3 = create_operator(&mut txn, flow.id, 5, &[0x03]);

		let nodes = CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(nodes.len(), 3);

		let ids: Vec<_> = nodes.iter().map(|n| n.id).collect();
		assert!(ids.contains(&node1.id));
		assert!(ids.contains(&node2.id));
		assert!(ids.contains(&node3.id));
	}

	#[test]
	fn test_list_operators_all() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		create_operator(&mut txn, flow.id, 1, &[0x01]);
		create_operator(&mut txn, flow.id, 4, &[0x02]);

		let nodes = CatalogStore::list_operators_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(nodes.len(), 2);
	}

	#[test]
	fn test_list_operators_all_empty() {
		let mut txn = create_test_admin_transaction();

		let nodes = CatalogStore::list_operators_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert!(nodes.is_empty());
	}

	#[test]
	fn test_list_operators_all_multiple_flows() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");

		let flow1 = create_flow(&mut txn, "test_namespace", "flow_one");
		let flow2 = create_flow(&mut txn, "test_namespace", "flow_two");

		create_operator(&mut txn, flow1.id, 1, &[0x01]);
		create_operator(&mut txn, flow1.id, 4, &[0x02]);
		create_operator(&mut txn, flow2.id, 1, &[0x03]);

		let all_nodes = CatalogStore::list_operators_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(all_nodes.len(), 3);

		let flow1_nodes: Vec<_> = all_nodes.iter().filter(|n| n.flow == flow1.id).collect();
		let flow2_nodes: Vec<_> = all_nodes.iter().filter(|n| n.flow == flow2.id).collect();

		assert_eq!(flow1_nodes.len(), 2);
		assert_eq!(flow2_nodes.len(), 1);
	}
}
