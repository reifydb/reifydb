// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::CatalogStore;
use reifydb_rql::flow::loader::load_flow_dag;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::identity::IdentityId;

#[test]
fn load_flow_dag_reads_store_snapshot_not_torn_cache() {
	// The cache is filled non-atomically, so a concurrent loader can see the index before the
	// edges. Tombstoning every edge in the cache alone reproduces that torn shape: a loader
	// reading the cache would build a sink with no inputs and abort flow registration.
	let engine = TestEngine::new();
	let catalog = engine.catalog();
	engine.admin("CREATE NAMESPACE fdl");
	engine.admin("CREATE TABLE fdl::src { id: int4, name: utf8 }");
	engine.admin("CREATE DEFERRED VIEW fdl::v { id: int4, name: utf8 } AS { FROM fdl::src }");

	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "fdl").unwrap().unwrap();
	let flow = catalog
		.find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "v")
		.unwrap()
		.expect("a flow must back the deferred view");

	let store_edges = CatalogStore::list_flow_edges_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap();
	assert!(!store_edges.is_empty(), "`from src` must produce at least one edge into the sink");

	let version = txn.version();
	for edge in &store_edges {
		catalog.cache().set_flow_edge(edge.id, version, None);
	}

	// Without this the test could pass against a cache that was never torn.
	assert!(
		catalog.list_flow_edges_by_flow(&mut Transaction::Admin(&mut txn), flow.id).unwrap().is_empty(),
		"the cache view must be torn for this flow before exercising the loader"
	);

	let dag = load_flow_dag(&mut Transaction::Admin(&mut txn), flow.id).unwrap();

	assert_eq!(
		dag.edge_count(),
		store_edges.len(),
		"load_flow_dag must rebuild every committed edge from the store, not the torn cache"
	);
	for id in dag.get_operator_ids() {
		let node = dag.get_operator(&id).unwrap();
		if node.outputs.is_empty() {
			assert!(
				!node.inputs.is_empty(),
				"sink node {:?} must keep its incoming edge - empty inputs aborts flow registration",
				id
			);
		}
	}
}
