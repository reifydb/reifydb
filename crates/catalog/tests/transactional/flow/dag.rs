// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_core::{
	flow::dag::FlowDag,
	interface::catalog::{
		flow::{Flow, FlowEntry, FlowId, FlowStatus},
		id::NamespaceId,
		policy::SessionOp,
	},
	testing::{CapturedEvent, CapturedInvocation},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::{TestTransaction, Transaction};
use reifydb_value::{params::Params, value::identity::IdentityId};

fn committed_flow_id(t: &TestEngine, catalog: &Catalog, namespace: &str) -> FlowId {
	// Must read committed state, so each test starts from the id every later reader sees.
	let mut probe = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut probe), namespace).unwrap().unwrap();
	catalog.find_flow_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "v").unwrap().unwrap().id
}

#[test]
fn find_flow_dag_returns_the_dag_at_its_version() {
	// A reader must load its own snapshot's graph, otherwise a drop after it began leaves it with nothing.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fns_dag_a");
	t.admin("CREATE TABLE fns_dag_a::src { id: int4 }");
	t.admin("CREATE DEFERRED VIEW fns_dag_a::v { id: int4 } AS { FROM fns_dag_a::src MAP { id: id } }");
	let flow_id = committed_flow_id(&t, &catalog, "fns_dag_a");

	let mut before_drop = t.begin_query(IdentityId::system()).unwrap();
	t.admin("DROP VIEW fns_dag_a::v");

	let dag = catalog
		.find_flow_dag(&mut Transaction::Query(&mut before_drop), flow_id)
		.unwrap()
		.expect("a reader from before the drop must still find the flow's DAG");
	assert_eq!(dag.id(), flow_id, "the DAG must belong to the requested flow");
	assert!(
		dag.node_count() >= 2,
		"`from src map` must load at least a source and a sink, got {}",
		dag.node_count()
	);
	assert!(dag.edge_count() >= 1, "the loaded operators must be connected, got {} edges", dag.edge_count());
}

#[test]
fn find_flow_dag_is_none_after_drop() {
	// Otherwise a dropped flow's graph is handed back and run again against a view that no longer exists.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fns_dag_b");
	t.admin("CREATE TABLE fns_dag_b::src { id: int4 }");
	t.admin("CREATE DEFERRED VIEW fns_dag_b::v { id: int4 } AS { FROM fns_dag_b::src MAP { id: id } }");
	let flow_id = committed_flow_id(&t, &catalog, "fns_dag_b");

	t.admin("DROP VIEW fns_dag_b::v");

	let mut after_drop = t.begin_query(IdentityId::system()).unwrap();
	let found = catalog.find_flow_dag(&mut Transaction::Query(&mut after_drop), flow_id).unwrap();
	assert!(found.is_none(), "a flow dropped before the reader began must have no DAG, got {found:?}");
}

#[test]
fn get_flow_dag_errors_when_missing() {
	// A missing flow must fail loudly, otherwise callers relying on get run on an empty graph.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fns_dag_c");
	t.admin("CREATE TABLE fns_dag_c::src { id: int4 }");
	t.admin("CREATE DEFERRED VIEW fns_dag_c::v { id: int4 } AS { FROM fns_dag_c::src MAP { id: id } }");
	let flow_id = committed_flow_id(&t, &catalog, "fns_dag_c");

	t.admin("DROP VIEW fns_dag_c::v");

	let mut after_drop = t.begin_query(IdentityId::system()).unwrap();
	let result = catalog.get_flow_dag(&mut Transaction::Query(&mut after_drop), flow_id);
	assert!(result.is_err(), "get_flow_dag must error for a dropped flow, got {result:?}");
}

#[test]
fn list_flow_dags_asc_includes_a_flow_created_in_the_same_txn() {
	// Without it a later DDL in the same txn misses the new flow as a dependent and drops its source.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fns_dag_d");
	t.admin("CREATE TABLE fns_dag_d::src { id: int4 }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE DEFERRED VIEW fns_dag_d::v { id: int4 } AS { FROM fns_dag_d::src MAP { id: id } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "fns_dag_d").unwrap().unwrap();
	let flow_id = catalog.find_flow_by_name(&mut Transaction::Admin(&mut txn), ns.id(), "v").unwrap().unwrap().id;

	let found = catalog.find_flow_dag(&mut Transaction::Admin(&mut txn), flow_id).unwrap();
	assert!(
		found.is_some_and(|dag| dag.node_count() >= 2),
		"the creating txn must find its own flow's DAG with its operators"
	);

	let dags = catalog.list_flow_dags_asc(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(
		dags.iter().any(|dag| dag.id() == flow_id),
		"the creating txn must list its own uncommitted flow's DAG"
	);
}

#[test]
fn list_flow_dags_asc_excludes_a_flow_dropped_in_the_same_txn() {
	// Without the pending-drop filter the dropping txn still reports the flow as a dependent of its sources.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fns_dag_e");
	t.admin("CREATE TABLE fns_dag_e::src { id: int4 }");
	t.admin("CREATE DEFERRED VIEW fns_dag_e::v { id: int4 } AS { FROM fns_dag_e::src MAP { id: id } }");
	let flow_id = committed_flow_id(&t, &catalog, "fns_dag_e");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP VIEW fns_dag_e::v", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let found = catalog.find_flow_dag(&mut Transaction::Admin(&mut txn), flow_id).unwrap();
	assert!(found.is_none(), "the dropping txn must not find the flow's DAG before commit, got {found:?}");

	let dags = catalog.list_flow_dags_asc(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(
		!dags.iter().any(|dag| dag.id() == flow_id),
		"the dropping txn must not list the flow's DAG before commit"
	);
}

#[test]
fn list_flow_dags_asc_is_in_ascending_flow_id_order() {
	// Drop errors list dependents in this order, so the ascending order must never flip.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fns_dag_f");
	t.admin("CREATE TABLE fns_dag_f::src { id: int4 }");
	t.admin("CREATE DEFERRED VIEW fns_dag_f::v { id: int4 } AS { FROM fns_dag_f::src MAP { id: id } }");
	t.admin("CREATE DEFERRED VIEW fns_dag_f::w { id: int4 } AS { FROM fns_dag_f::src MAP { id: id } }");
	t.admin("CREATE DEFERRED VIEW fns_dag_f::x { id: int4 } AS { FROM fns_dag_f::src MAP { id: id } }");

	let mut q = t.begin_query(IdentityId::system()).unwrap();
	let ids: Vec<_> =
		catalog.list_flow_dags_asc(&mut Transaction::Query(&mut q)).unwrap().iter().map(|d| d.id()).collect();
	assert!(ids.len() >= 3, "all three committed flows must be listed, got {ids:?}");
	assert!(ids.windows(2).all(|w| w[0] < w[1]), "DAGs must be listed in ascending flow id order, got {ids:?}");
}

#[test]
fn test_txn_find_flow_reads_the_cache() {
	// The flow lives only in the cache, so a Test txn that skips the cache and reads storage alone returns none.
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let cached = Flow {
		id: FlowId(u64::MAX - 7),
		namespace: NamespaceId(u64::MAX - 7),
		name: "cache_only".to_string(),
		status: FlowStatus::Active,
	};
	catalog.cache().set_flow(
		cached.id,
		txn.version(),
		Some(FlowEntry {
			flow: cached.clone(),
			dag: FlowDag::builder(cached.id).build(),
		}),
	);

	let mut events: Vec<CapturedEvent> = Vec::new();
	let mut invocations: Vec<CapturedInvocation> = Vec::new();
	let mut event_seq = 0;
	let mut handler_seq = 0;
	let mut test_txn = TestTransaction::new(
		&mut txn,
		&mut events,
		&mut invocations,
		&mut event_seq,
		&mut handler_seq,
		SessionOp::Admin,
		true,
	);

	let found = catalog.find_flow(&mut Transaction::Test(Box::new(test_txn.reborrow())), cached.id).unwrap();
	assert_eq!(found, Some(cached), "a Test txn must see a flow held in the cache at its version");
}
