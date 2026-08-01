// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::catalog::{change::CatalogTrackOperatorChangeOperations, flow::Operator},
	key::operator::{OperatorByFlowKey, OperatorKey},
};
use reifydb_transaction::transaction::admin::AdminTransaction;

use crate::{
	CatalogStore, Result,
	store::operator::shape::{operator, operator_by_flow},
};

impl CatalogStore {
	pub(crate) fn create_operator(txn: &mut AdminTransaction, node_def: &Operator) -> Result<()> {
		let mut row = operator::SHAPE.allocate();
		operator::SHAPE.set::<u64>(&mut row, operator::ID, u64::from(node_def.id));
		operator::SHAPE.set::<u64>(&mut row, operator::FLOW, u64::from(node_def.flow));
		operator::SHAPE.set::<u8>(&mut row, operator::TYPE, node_def.node_type);
		operator::SHAPE.set_blob(&mut row, operator::DATA, &node_def.data);

		txn.set(&OperatorKey::encoded(node_def.id), row)?;

		let mut index_row = operator_by_flow::SHAPE.allocate();
		operator_by_flow::SHAPE.set::<u64>(&mut index_row, operator_by_flow::FLOW, u64::from(node_def.flow));
		operator_by_flow::SHAPE.set::<u64>(&mut index_row, operator_by_flow::ID, u64::from(node_def.id));

		txn.set(&OperatorByFlowKey::encoded(node_def.flow, node_def.id), index_row)?;

		txn.track_operator_created(node_def.clone())?;

		Ok(())
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::flow::Operator;
	use reifydb_engine::test_harness::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;
	use reifydb_value::value::blob::Blob;

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
		let node_def = Operator {
			id: operator_id,
			flow: flow.id,
			node_type: 1, // SourceTable
			data: Blob::from([0x01u8, 0x02, 0x03].as_slice()),
		};

		CatalogStore::create_operator(&mut txn, &node_def).unwrap();

		let result = CatalogStore::get_operator(&mut Transaction::Admin(&mut txn), operator_id).unwrap();
		assert_eq!(result.id, operator_id);
		assert_eq!(result.flow, flow.id);
		assert_eq!(result.node_type, 1);
		assert_eq!(result.data.as_bytes(), &[0x01, 0x02, 0x03]);
	}

	#[test]
	fn test_create_multiple_nodes_same_flow() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1_id = next_operator_id(&mut txn).unwrap();
		let node1 = Operator {
			id: node1_id,
			flow: flow.id,
			node_type: 1, // SourceTable
			data: Blob::from([0x01u8].as_slice()),
		};
		CatalogStore::create_operator(&mut txn, &node1).unwrap();

		let node2_id = next_operator_id(&mut txn).unwrap();
		let node2 = Operator {
			id: node2_id,
			flow: flow.id,
			node_type: 4, // Filter
			data: Blob::from([0x02u8].as_slice()),
		};
		CatalogStore::create_operator(&mut txn, &node2).unwrap();

		let result1 = CatalogStore::get_operator(&mut Transaction::Admin(&mut txn), node1_id).unwrap();
		let result2 = CatalogStore::get_operator(&mut Transaction::Admin(&mut txn), node2_id).unwrap();

		assert_eq!(result1.node_type, 1);
		assert_eq!(result2.node_type, 4);
	}

	#[test]
	fn test_create_nodes_different_flows() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");

		let flow1 = create_flow(&mut txn, "test_namespace", "flow_one");
		let flow2 = create_flow(&mut txn, "test_namespace", "flow_two");

		let node1_id = next_operator_id(&mut txn).unwrap();
		let node1 = Operator {
			id: node1_id,
			flow: flow1.id,
			node_type: 1,
			data: Blob::from([0x01u8].as_slice()),
		};
		CatalogStore::create_operator(&mut txn, &node1).unwrap();

		let node2_id = next_operator_id(&mut txn).unwrap();
		let node2 = Operator {
			id: node2_id,
			flow: flow2.id,
			node_type: 1,
			data: Blob::from([0x02u8].as_slice()),
		};
		CatalogStore::create_operator(&mut txn, &node2).unwrap();

		let result1 = CatalogStore::get_operator(&mut Transaction::Admin(&mut txn), node1_id).unwrap();
		let result2 = CatalogStore::get_operator(&mut Transaction::Admin(&mut txn), node2_id).unwrap();

		assert_eq!(result1.flow, flow1.id);
		assert_eq!(result2.flow, flow2.id);
	}

	#[test]
	fn test_node_appears_in_index() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let operator_id = next_operator_id(&mut txn).unwrap();
		let node_def = Operator {
			id: operator_id,
			flow: flow.id,
			node_type: 1,
			data: Blob::from([0x01u8].as_slice()),
		};

		CatalogStore::create_operator(&mut txn, &node_def).unwrap();

		let operators =
			CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(operators.len(), 1);
		assert_eq!(operators[0].id, operator_id);
	}
}
