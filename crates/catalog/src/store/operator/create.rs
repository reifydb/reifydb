// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::to_stdvec;
use reifydb_core::{
	flow::operator::FlowNode,
	interface::catalog::flow::FlowId,
	internal,
	key::operator::key::{OperatorByFlowKey, OperatorKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::{error::Error, value::blob::Blob};

use crate::{
	CatalogStore, Result,
	store::operator::shape::{operator, operator_by_flow},
};

impl CatalogStore {
	pub(crate) fn create_operator(txn: &mut AdminTransaction, flow: FlowId, node: &FlowNode) -> Result<()> {
		let data = to_stdvec(&node.ty)
			.map_err(|e| Error(Box::new(internal!("Failed to serialize OperatorDef: {}", e))))?;

		let mut row = operator::allocate();
		operator::set_id(&mut row, u64::from(node.id));
		operator::set_flow(&mut row, u64::from(flow));
		operator::set_type(&mut row, node.ty.discriminator());
		operator::set_data(&mut row, &Blob::from(data));

		txn.set(&OperatorKey::new(node.id), row.freeze())?;

		let mut index_row = operator_by_flow::allocate();
		operator_by_flow::set_flow(&mut index_row, u64::from(flow));
		operator_by_flow::set_id(&mut index_row, u64::from(node.id));

		txn.set(&OperatorByFlowKey::new(flow, node.id), index_row.freeze())?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::flow::operator::{FlowNode, OperatorDef};
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		store::sequence::flow::next_operator_id,
		test_utils::{create_flow, create_namespace, ensure_test_flow},
	};

	#[test]
	fn test_create_operator() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let operator_id = next_operator_id(&mut txn).unwrap();
		let node = FlowNode::new(operator_id, OperatorDef::SourceInlineData {});

		CatalogStore::create_operator(&mut txn, flow.id, &node).unwrap();

		let result =
			CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), operator_id).unwrap().unwrap();
		assert_eq!(result.id, operator_id);
		assert!(CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id)
			.unwrap()
			.iter()
			.any(|n| n.id == result.id));
		assert_eq!(result.ty, OperatorDef::SourceInlineData {});
	}

	#[test]
	fn test_create_multiple_nodes_same_flow() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1_id = next_operator_id(&mut txn).unwrap();
		let node1 = FlowNode::new(node1_id, OperatorDef::SourceInlineData {});
		CatalogStore::create_operator(&mut txn, flow.id, &node1).unwrap();

		let node2_id = next_operator_id(&mut txn).unwrap();
		let node2 = FlowNode::new(
			node2_id,
			OperatorDef::Take {
				limit: 1,
			},
		);
		CatalogStore::create_operator(&mut txn, flow.id, &node2).unwrap();

		let result1 =
			CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), node1_id).unwrap().unwrap();
		let result2 =
			CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), node2_id).unwrap().unwrap();

		assert_eq!(result1.ty, OperatorDef::SourceInlineData {});
		assert_eq!(
			result2.ty,
			OperatorDef::Take {
				limit: 1
			}
		);
	}

	#[test]
	fn test_create_nodes_different_flows() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");

		let flow1 = create_flow(&mut txn, "test_namespace", "flow_one");
		let flow2 = create_flow(&mut txn, "test_namespace", "flow_two");

		let node1_id = next_operator_id(&mut txn).unwrap();
		let node1 = FlowNode::new(node1_id, OperatorDef::SourceInlineData {});
		CatalogStore::create_operator(&mut txn, flow1.id, &node1).unwrap();

		let node2_id = next_operator_id(&mut txn).unwrap();
		let node2 = FlowNode::new(node2_id, OperatorDef::SourceInlineData {});
		CatalogStore::create_operator(&mut txn, flow2.id, &node2).unwrap();

		let result1 =
			CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), node1_id).unwrap().unwrap();
		let result2 =
			CatalogStore::find_operator(&mut Transaction::Admin(&mut txn), node2_id).unwrap().unwrap();

		assert!(CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow1.id)
			.unwrap()
			.iter()
			.any(|n| n.id == result1.id));
		assert!(CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow2.id)
			.unwrap()
			.iter()
			.any(|n| n.id == result2.id));
	}

	#[test]
	fn test_node_appears_in_index() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let operator_id = next_operator_id(&mut txn).unwrap();
		let node = FlowNode::new(operator_id, OperatorDef::SourceInlineData {});

		CatalogStore::create_operator(&mut txn, flow.id, &node).unwrap();

		let operators =
			CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(operators.len(), 1);
		assert_eq!(operators[0].id, operator_id);
	}
}
