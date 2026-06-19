// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::CatalogStore;
use reifydb_engine::test_harness::TestEngine;
use reifydb_rql::flow::loader::load_flow_dag;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::identity::IdentityId;

// Regression for the deferred-view torn-read race. The post-commit CatalogCacheInterceptor
// populates flow nodes then edges incrementally and non-atomically into a lock-free cache, so a
// concurrent CDC-driven flow loader could observe the `flow_edges_by_flow` index after only some
// (or none) of the edges had been inserted, build a sink with empty inputs, and abort flow
// registration in sub-flow's register.rs at `inputs[0]` on the empty slice.
//
// load_flow_dag therefore reads the committed store snapshot through CatalogStore directly, not
// the cache. We reproduce the exact torn shape deterministically by tombstoning every edge in the
// cache only (the `_by_flow` index entry stays, but each edge stops being visible at the read
// version), then assert the rebuilt DAG still connects the sink to its incoming edge.
#[test]
fn load_flow_dag_reads_store_snapshot_not_torn_cache() {
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

	// Precondition: the cache-first read now hands back a torn (empty) edge set - this is the
	// state that produced an orphaned sink under the old loader.
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
	for id in dag.get_node_ids() {
		let node = dag.get_node(&id).unwrap();
		if node.outputs.is_empty() {
			assert!(
				!node.inputs.is_empty(),
				"sink node {:?} must keep its incoming edge - empty inputs aborts flow registration",
				id
			);
		}
	}
}
