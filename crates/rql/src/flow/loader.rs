// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use postcard::from_bytes;
use reifydb_catalog::CatalogStore;
use reifydb_core::{interface::catalog::flow::FlowId, internal};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::error::Error;

use crate::{
	Result,
	flow::{
		flow::FlowDag,
		operator::{FlowEdge, FlowNode, OperatorDef},
	},
};

pub fn load_flow_dag(txn: &mut Transaction<'_>, flow_id: FlowId) -> Result<FlowDag> {
	let flow_def = CatalogStore::get_flow(txn, flow_id)?;
	let node_defs = CatalogStore::list_operators_by_flow(txn, flow_id)?;
	let edge_defs = CatalogStore::list_flow_edges_by_flow(txn, flow_id)?;

	let mut builder = FlowDag::builder(flow_id);

	for node_def in node_defs {
		let node_type: OperatorDef = from_bytes(node_def.data.as_ref())
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize OperatorDef: {}", e))))?;

		let node = FlowNode::new(node_def.id, node_type);
		builder.add_node(node);
	}

	for edge_def in edge_defs {
		let edge = FlowEdge::new(edge_def.id, edge_def.source, edge_def.target);
		builder.add_edge(edge)?;
	}

	let flow = builder.build();
	Ok(flow)
}

#[cfg(test)]
mod tests {
	use postcard::to_allocvec;
	use reifydb_catalog::test_utils::{create_flow, create_namespace, create_operator};
	use reifydb_engine::test_harness::create_test_admin_transaction;

	use super::*;

	#[test]
	fn the_loader_restores_every_persisted_operator() {
		// The loader is the only path from catalog rows back to a runnable graph, so an operator
		// that fails to come back is a silently shorter pipeline after every restart rather than
		// a load error. These two tests replace a pair that only proved the flow-level time
		// declaration round-tripped; that declaration is gone, but the loader still needs its own
		// contract pinned.
		let mut txn = create_test_admin_transaction();
		create_namespace(&mut txn, "test");
		let flow = create_flow(&mut txn, "test", "win");
		let encoded = to_allocvec(&OperatorDef::SourceInlineData {}).expect("encode");
		let first = create_operator(&mut txn, flow.id, 0, &encoded);
		let second = create_operator(&mut txn, flow.id, 0, &encoded);

		let dag = load_flow_dag(&mut Transaction::Admin(&mut txn), flow.id).unwrap();

		let mut restored: Vec<_> = dag.get_operator_ids().collect();
		restored.sort();
		let mut expected = vec![first.id, second.id];
		expected.sort();
		assert_eq!(restored, expected, "every persisted operator must come back");
		assert_eq!(dag.id, flow.id, "the loaded graph keeps the flow identity it was asked for");
	}

	#[test]
	fn a_flow_with_no_operators_loads_as_an_empty_graph() {
		// An empty flow must load rather than error: registration walks the graph, and turning
		// "nothing to do" into a hard failure would block startup on a half-created view.
		let mut txn = create_test_admin_transaction();
		create_namespace(&mut txn, "test");
		let flow = create_flow(&mut txn, "test", "empty");

		let dag = load_flow_dag(&mut Transaction::Admin(&mut txn), flow.id).unwrap();

		assert_eq!(dag.get_operator_ids().count(), 0, "no operators persisted, so none restored");
		assert!(dag.topological_order().unwrap().is_empty());
	}
}
