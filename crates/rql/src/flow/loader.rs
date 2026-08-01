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

	let mut builder = FlowDag::builder(flow_id).time(flow_def.time);

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
	fn the_loader_restores_a_declared_event_domain() {
		// A domain persisted but not read back leaves every flow bucketing by the wall clock, silently and
		// across every restart.
		assert_eq!(loaded(Some(TimeDomain::Event)).time, Some(TimeDomain::Event));
	}

	#[test]
	fn the_loader_keeps_silence_and_explicit_processing_apart() {
		// Silence is what lets registration reject an event-time source; explicit processing overrides that
		// check. Collapsing either into the other turns a rejection into a silent domain switch.
		assert_eq!(loaded(Some(TimeDomain::Processing)).time, Some(TimeDomain::Processing));
		assert_eq!(loaded(None).time, None);
	}
}
