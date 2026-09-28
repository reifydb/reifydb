// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::from_bytes;
use reifydb_codec::row::catalog::EncodedCatalogRow;
use reifydb_core::{
	flow::operator::{FlowNode, OperatorDef},
	interface::catalog::flow::{FlowId, OperatorId},
	internal,
	key::{
		any::TaggedKey,
		operator::key::{OperatorByFlowKey, OperatorKey},
	},
};
use reifydb_transaction::{multi::RangeScope, transaction::Transaction};
use reifydb_value::error::Error;

use crate::{
	CatalogStore, Result,
	store::operator::shape::{operator, operator_by_flow},
};

impl CatalogStore {
	pub fn list_operators_by_flow(rx: &mut Transaction<'_>, flow_id: FlowId) -> Result<Vec<FlowNode>> {
		let mut node_ids = Vec::new();
		{
			let stream = rx.range(OperatorByFlowKey::full_scan(flow_id), RangeScope::All, 1024)?;
			for entry in stream {
				let multi = entry?;
				node_ids.push(OperatorId(operator_by_flow::get_id(EncodedCatalogRow::view(
					&multi.bytes,
				))));
			}
		}

		let mut operators = Vec::new();
		for operator_id in node_ids {
			if let Some(operator) = Self::find_operator(rx, operator_id)? {
				operators.push(operator);
			}
		}

		Ok(operators)
	}

	pub(crate) fn list_operators_all(rx: &mut Transaction<'_>) -> Result<Vec<(FlowId, FlowNode)>> {
		let mut result = Vec::new();

		let stream = rx.range(OperatorKey::full_scan(), RangeScope::All, 1024)?;

		for entry in stream {
			let entry = entry?;
			if let TaggedKey::Operator(operator_key) = &entry.key {
				let operator_id = operator_key.operator;
				let flow_id = FlowId(operator::get_flow(EncodedCatalogRow::view(&entry.bytes)));
				let ty: OperatorDef =
					from_bytes(operator::get_data(EncodedCatalogRow::view(&entry.bytes)).as_ref())
						.map_err(|e| {
							Error(Box::new(internal!(
								"Failed to deserialize OperatorDef: {}",
								e
							)))
						})?;

				result.push((flow_id, FlowNode::new(operator_id, ty)));
			}
		}

		Ok(result)
	}
}

#[cfg(test)]
pub mod tests {
	use reifydb_core::flow::operator::OperatorDef;
	use reifydb_test_harness::engine::create_test_admin_transaction;
	use reifydb_transaction::transaction::Transaction;

	use crate::{
		CatalogStore,
		test_utils::{create_flow, create_namespace, create_operator, ensure_test_flow},
	};

	#[test]
	fn test_list_operators_by_flow() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let operator = create_operator(&mut txn, flow.id, OperatorDef::SourceInlineData {});

		let operators =
			CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(operators.len(), 1);
		assert_eq!(operators[0].id, operator.id);
	}

	#[test]
	fn test_list_operators_by_flow_empty() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let operators =
			CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert!(operators.is_empty());
	}

	#[test]
	fn test_list_operators_by_flow_multiple() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		let node1 = create_operator(&mut txn, flow.id, OperatorDef::SourceInlineData {});
		let node2 = create_operator(&mut txn, flow.id, OperatorDef::SourceInlineData {});
		let node3 = create_operator(&mut txn, flow.id, OperatorDef::SourceInlineData {});

		let operators =
			CatalogStore::list_operators_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
		assert_eq!(operators.len(), 3);

		let ids: Vec<_> = operators.iter().map(|n| n.id).collect();
		assert!(ids.contains(&node1.id));
		assert!(ids.contains(&node2.id));
		assert!(ids.contains(&node3.id));
	}

	#[test]
	fn test_list_operators_all() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");
		let flow = ensure_test_flow(&mut txn);

		create_operator(&mut txn, flow.id, OperatorDef::SourceInlineData {});
		create_operator(&mut txn, flow.id, OperatorDef::SourceInlineData {});

		let operators = CatalogStore::list_operators_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(operators.len(), 2);
	}

	#[test]
	fn test_list_operators_all_empty() {
		let mut txn = create_test_admin_transaction();

		let operators = CatalogStore::list_operators_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert!(operators.is_empty());
	}

	#[test]
	fn test_list_operators_all_multiple_flows() {
		let mut txn = create_test_admin_transaction();
		let _namespace = create_namespace(&mut txn, "test_namespace");

		let flow1 = create_flow(&mut txn, "test_namespace", "flow_one");
		let flow2 = create_flow(&mut txn, "test_namespace", "flow_two");

		create_operator(&mut txn, flow1.id, OperatorDef::SourceInlineData {});
		create_operator(&mut txn, flow1.id, OperatorDef::SourceInlineData {});
		create_operator(&mut txn, flow2.id, OperatorDef::SourceInlineData {});

		let all_nodes = CatalogStore::list_operators_all(&mut Transaction::Admin(&mut txn)).unwrap();
		assert_eq!(all_nodes.len(), 3);

		let flow1_nodes: Vec<_> = all_nodes.iter().filter(|(flow, _)| *flow == flow1.id).collect();
		let flow2_nodes: Vec<_> = all_nodes.iter().filter(|(flow, _)| *flow == flow2.id).collect();

		assert_eq!(flow1_nodes.len(), 2);
		assert_eq!(flow2_nodes.len(), 1);
	}
}
