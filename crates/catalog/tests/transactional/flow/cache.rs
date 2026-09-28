// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::{
	cache::{CatalogCache, load::CatalogCacheLoader},
	catalog::Catalog,
};
use reifydb_core::{
	flow::{
		dag::FlowDag,
		operator::{FlowNode, OperatorDef},
	},
	interface::catalog::{
		change::CatalogTrackFlowChangeOperations,
		flow::{Flow, FlowEntry, FlowId, FlowStatus, OperatorId},
		id::NamespaceId,
	},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{params::Params, value::identity::IdentityId};

fn committed_flow_id(t: &TestEngine, catalog: &Catalog, namespace: &str) -> FlowId {
	// Must read committed state, so each test starts from the id every later reader sees.
	let mut probe = t.begin_admin(IdentityId::system()).unwrap();
	let ns = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut probe), namespace).unwrap().unwrap();
	catalog.find_flow_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "v").unwrap().unwrap().id
}

fn create_view(t: &TestEngine, namespace: &str) {
	t.admin(&format!("CREATE NAMESPACE {namespace}"));
	t.admin(&format!("CREATE TABLE {namespace}::src {{ id: int4 }}"));
	t.admin(&format!(
		"CREATE DEFERRED VIEW {namespace}::v {{ id: int4 }} AS {{ FROM {namespace}::src FILTER {{ id > 0 }} MAP {{ id: id }} }}"
	));
}

#[test]
fn committed_entry_is_cached_from_its_version_on() {
	// The commit must fill the whole entry at exactly its version, otherwise readers miss it or see it too early.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE fns_cache_a");
	t.admin("CREATE TABLE fns_cache_a::src { id: int4 }");
	let before_create = t.begin_query(IdentityId::system()).unwrap();
	t.admin(
		"CREATE DEFERRED VIEW fns_cache_a::v { id: int4 } AS { FROM fns_cache_a::src FILTER { id > 0 } MAP { id: id } }",
	);
	let flow_id = committed_flow_id(&t, &catalog, "fns_cache_a");

	let mut after_create = t.begin_query(IdentityId::system()).unwrap();
	let entry = catalog
		.cache()
		.find_flow_entry_at(flow_id, after_create.version())
		.expect("the committed flow must be in the cache at the reader's version");
	assert_eq!(entry.flow.id, flow_id, "the entry must hold the flow it is keyed by");
	assert_eq!(entry.dag.id(), flow_id, "the entry's DAG must belong to the same flow");
	assert!(
		entry.dag.node_count() >= 3,
		"`from filter map` plus sink must be cached whole, got {}",
		entry.dag.node_count()
	);

	let found = catalog.find_flow_dag(&mut Transaction::Query(&mut after_create), flow_id).unwrap();
	assert_eq!(found, Some(entry.dag), "find_flow_dag must hand back the cached DAG");

	assert!(
		catalog.cache().find_flow_entry_at(flow_id, before_create.version()).is_none(),
		"a reader that began before the create must not see the entry"
	);
}

#[test]
fn dropped_entry_is_gone_from_the_drop_version_on() {
	// A drop must write a none into the cache, otherwise the dropped graph is still handed to later readers.
	let t = TestEngine::new();
	let catalog = t.catalog();
	create_view(&t, "fns_cache_b");
	let flow_id = committed_flow_id(&t, &catalog, "fns_cache_b");
	let before_drop = t.begin_query(IdentityId::system()).unwrap();

	t.admin("DROP VIEW fns_cache_b::v");

	let mut after_drop = t.begin_query(IdentityId::system()).unwrap();
	assert!(
		catalog.cache().find_flow_entry_at(flow_id, after_drop.version()).is_none(),
		"the cache must not hold the flow at or after its drop version"
	);
	let dags = catalog.list_flow_dags_asc(&mut Transaction::Query(&mut after_drop)).unwrap();
	assert!(!dags.iter().any(|dag| dag.id() == flow_id), "a dropped flow must not be listed");
	assert!(
		catalog.cache().find_flow_entry_at(flow_id, before_drop.version()).is_some(),
		"a reader that began before the drop must keep the entry"
	);
}

#[test]
fn same_txn_create_is_found_without_a_storage_read() {
	// The entry is only a pending change with no storage row, so any storage read here returns none or errors.
	let t = TestEngine::new();
	let catalog = t.catalog();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();

	let id = FlowId(u64::MAX - 11);
	let mut builder = FlowDag::builder(id);
	builder.add_node(FlowNode::new(OperatorId(u64::MAX - 11), OperatorDef::SourceInlineData {}));
	let dag = builder.build();
	let entry = FlowEntry {
		flow: Flow {
			id,
			namespace: NamespaceId(u64::MAX - 11),
			name: "pending_only".to_string(),
			status: FlowStatus::Active,
		},
		dag: dag.clone(),
	};
	txn.track_flow_created(entry).unwrap();

	let found = catalog.find_flow_dag(&mut Transaction::Admin(&mut txn), id).unwrap();
	assert_eq!(found, Some(dag.clone()), "the creating txn must get its pending DAG back");

	let dags = catalog.list_flow_dags_asc(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(dags.contains(&dag), "the creating txn must list its pending DAG");
}

#[test]
fn boot_entry_equals_commit_entry() {
	// A restart must rebuild the exact entry the commit cached, otherwise flows run a different graph after a
	// reboot.
	let t = TestEngine::new();
	let catalog = t.catalog();
	create_view(&t, "fns_cache_c");
	let flow_id = committed_flow_id(&t, &catalog, "fns_cache_c");

	let mut q = t.begin_query(IdentityId::system()).unwrap();
	let committed = catalog
		.cache()
		.find_flow_entry_at(flow_id, q.version())
		.expect("the commit must have cached the entry");

	let booted = CatalogCache::new();
	CatalogCacheLoader::load_all(&mut Transaction::Query(&mut q), &booted).unwrap();
	let loaded = booted.find_flow_entry_at(flow_id, q.version()).expect("the boot load must cache the flow");

	assert_eq!(loaded, committed, "the boot-built entry must equal the commit-built entry");
}

#[test]
fn namespace_drop_removes_its_flows_from_the_cache() {
	// A namespace drop removes its flows from storage, so without a tracked delete the cache keeps serving them.
	let t = TestEngine::new();
	let catalog = t.catalog();
	create_view(&t, "fns_cache_d");
	let flow_id = committed_flow_id(&t, &catalog, "fns_cache_d");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP NAMESPACE fns_cache_d", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut after_drop = t.begin_query(IdentityId::system()).unwrap();
	let found = catalog.find_flow_dag(&mut Transaction::Query(&mut after_drop), flow_id).unwrap();
	assert!(found.is_none(), "a flow of a dropped namespace must have no DAG, got {found:?}");
	let dags = catalog.list_flow_dags_asc(&mut Transaction::Query(&mut after_drop)).unwrap();
	assert!(!dags.iter().any(|dag| dag.id() == flow_id), "a flow of a dropped namespace must not be listed");
}
