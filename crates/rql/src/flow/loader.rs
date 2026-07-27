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
		node::{FlowEdge, FlowNode, FlowNodeType},
	},
};

pub fn load_flow_dag(txn: &mut Transaction<'_>, flow_id: FlowId) -> Result<FlowDag> {
	let flow_def = CatalogStore::get_flow(txn, flow_id)?;
	let node_defs = CatalogStore::list_flow_nodes_by_flow(txn, flow_id)?;
	let edge_defs = CatalogStore::list_flow_edges_by_flow(txn, flow_id)?;

	let mut builder = FlowDag::builder(flow_id).time(flow_def.time);

	for node_def in node_defs {
		let node_type: FlowNodeType = from_bytes(node_def.data.as_ref())
			.map_err(|e| Error(Box::new(internal!("Failed to deserialize FlowNodeType: {}", e))))?;

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
	use reifydb_catalog::test_utils::{create_flow_with_time, create_namespace};
	use reifydb_core::common::TimeDomain;
	use reifydb_engine::test_harness::create_test_admin_transaction;

	use super::*;

	fn loaded(time: Option<TimeDomain>) -> FlowDag {
		let mut txn = create_test_admin_transaction();
		create_namespace(&mut txn, "test");
		let flow = create_flow_with_time(&mut txn, "test", "win", time);
		load_flow_dag(&mut Transaction::Admin(&mut txn), flow.id).unwrap()
	}

	#[test]
	// Intent: the loader rebuilds a flow from its persisted nodes and edges, and the declared time domain has to
	// come back with them. It did not: the domain was written to the flow row and never read, so every flow the
	// engine registered ran as processing no matter what its author declared. That is silent - an event-time flow
	// keeps producing rows, just bucketed by the wall clock - and it outlives a restart, so a flow accepted at
	// definition would be rejected or mis-bucketed on every subsequent boot.
	// Mutation: drop `.time(flow_def.time)` from the builder and this fails with None.
	fn the_loader_restores_a_declared_event_domain() {
		assert_eq!(loaded(Some(TimeDomain::Event)).time, Some(TimeDomain::Event));
	}

	#[test]
	// Intent: `None` and `Some(Processing)` are different declarations - silence is what lets the engine reject a
	// flow that reads an event-time source without saying so, while an explicit processing declaration is a
	// deliberate override of exactly that check. A loader that collapsed either one into the other would turn that
	// rejection into a silent domain switch, so both spellings have to survive the round trip distinctly.
	fn the_loader_keeps_silence_and_explicit_processing_apart() {
		assert_eq!(loaded(Some(TimeDomain::Processing)).time, Some(TimeDomain::Processing));
		assert_eq!(loaded(None).time, None);
	}
}
