// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// A transactional view's flow must be loadable as an internally consistent DAG
// from the catalog read path that flow registration uses: every edge endpoint
// resolves to a loaded node, and every sink (a node that is no edge's source)
// has an incoming edge. A torn read that yields a sink with an empty edge set is
// exactly what makes `add_sink_table_view` panic on `inputs[0]` during the
// create-then-drop race.

use std::collections::HashSet;

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::{value::{identity::IdentityId}};
use reifydb_transaction::transaction::Transaction;

#[test]
fn transactional_view_flow_loads_with_a_connected_sink() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fn_t");
	t.admin("CREATE TABLE fn_t::src { id: int4, name: utf8 }");
	t.admin("CREATE TRANSACTIONAL VIEW fn_t::v { id: int4, name: utf8 } AS { FROM fn_t::src }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "fn_t").unwrap().unwrap();
	let flow = catalog
		.find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "v")
		.unwrap()
		.expect("a flow must back the transactional view");

	let nodes = catalog.list_flow_nodes_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
	let edges = catalog.list_flow_edges_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();

	assert!(nodes.len() >= 2, "`from src` must produce at least a source and a sink node, got {}", nodes.len());
	assert!(
		!edges.is_empty(),
		"the sink must have at least one incoming edge; an empty edge set is the drop-race that panics flow registration"
	);

	let node_ids: HashSet<_> = nodes.iter().map(|n| n.id).collect();
	let edge_sources: HashSet<_> = edges.iter().map(|e| e.source).collect();
	let edge_targets: HashSet<_> = edges.iter().map(|e| e.target).collect();

	for edge in &edges {
		assert!(node_ids.contains(&edge.source), "edge source {:?} must resolve to a loaded node", edge.source);
		assert!(node_ids.contains(&edge.target), "edge target {:?} must resolve to a loaded node", edge.target);
	}

	for id in &node_ids {
		if !edge_sources.contains(id) {
			assert!(
				edge_targets.contains(id),
				"sink node {:?} must have an incoming edge - registering it with empty inputs panics",
				id
			);
		}
	}
}
