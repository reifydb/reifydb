// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// Same DAG-consistency contract as the transactional case, for a deferred view:
// the catalog read path must hand flow registration a sink that is connected by
// an edge, never an orphaned sink with an empty input set.

use std::collections::HashSet;

use reifydb_engine::test_harness::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::identity::IdentityId;

#[test]
fn deferred_view_flow_loads_with_a_connected_sink() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fn_d");
	t.admin("CREATE TABLE fn_d::src { id: int4, name: utf8 }");
	t.admin("CREATE DEFERRED VIEW fn_d::v { id: int4, name: utf8 } AS { FROM fn_d::src }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "fn_d").unwrap().unwrap();
	let flow = catalog
		.find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "v")
		.unwrap()
		.expect("a flow must back the deferred view");

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
