// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The definition-time walk cannot cover this: there the flow and its sources are consistent by
// construction. Each test reloads the flow's own DAG and alters only the source object's TimeSource,
// which is the restart state where a flow legal at DDL is resolved against a catalog that has moved.

use reifydb_catalog::catalog::Catalog;
use reifydb_core::common::TimeSource;
use reifydb_engine::{flow::time_domain::check_time_domain, test_harness::TestEngine};
use reifydb_rql::flow::{flow::FlowDag, loader::load_flow_dag};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::identity::IdentityId;

const EVENT: &str = "at";

fn processing() -> TimeSource {
	TimeSource::Processing
}

fn event() -> TimeSource {
	TimeSource::Event {
		ts: EVENT.to_string(),
	}
}

fn alter_source_time(engine: &TestEngine, txn: &mut AdminTransaction, table: &str, time: TimeSource) {
	// Only the source's declaration moves; the flow's own record stays as DDL wrote it.
	let catalog = engine.catalog();
	let namespace = catalog.find_namespace_by_name(&mut Transaction::Admin(txn), "td").unwrap().unwrap();
	let mut altered =
		catalog.find_table_by_name(&mut Transaction::Admin(txn), namespace.id(), table).unwrap().unwrap();
	altered.time = time;
	catalog.cache().set_table(altered.id, txn.version(), Some(altered));
}

fn reload_dag(engine: &TestEngine, txn: &mut AdminTransaction, view: &str) -> FlowDag {
	let catalog = engine.catalog();
	let namespace = catalog.find_namespace_by_name(&mut Transaction::Admin(txn), "td").unwrap().unwrap();
	let flow = catalog
		.find_flow_by_name(&mut Transaction::Admin(txn), namespace.id(), view)
		.unwrap()
		.expect("a flow must back the deferred view");
	load_flow_dag(&mut Transaction::Admin(txn), flow.id).unwrap()
}

fn recheck(engine: &TestEngine, txn: &mut AdminTransaction, dag: &FlowDag) -> Option<String> {
	let catalog: Catalog = engine.catalog();
	match check_time_domain(&catalog, &mut Transaction::Admin(txn), dag) {
		Ok(()) => None,
		Err(err) => Some(err.diagnostic().code),
	}
}

#[test]
fn a_source_that_gains_event_time_invalidates_an_undeclared_flow() {
	// The author never edited the view, so nothing re-runs the DDL check; without the re-check
	// the flow comes back on restart and buckets event-time rows by wall clock.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE td");
	engine.admin("CREATE TABLE td::src { id: int4, at: datetime }");
	engine.admin("CREATE DEFERRED VIEW td::v { id: int4, at: datetime } AS { FROM td::src }");

	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let dag = reload_dag(&engine, &mut txn, "v");

	// Without this the test could pass against a DAG that was never valid in the first place.
	assert_eq!(recheck(&engine, &mut txn, &dag), None, "the flow must be legal before the source is altered");

	alter_source_time(&engine, &mut txn, "src", event());

	assert_eq!(
		recheck(&engine, &mut txn, &dag).as_deref(),
		Some("FLOW_041"),
		"an undeclared flow over a source that now declares event time must be rejected on re-registration"
	);
}

#[test]
fn a_source_that_loses_event_time_invalidates_an_event_time_flow() {
	// With the populator gone there is no column left to fill #time from, so every row falls
	// back to arrival while the flow still claims to bucket by event time.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE td");
	engine.admin("CREATE TABLE td::src { id: int4, at: datetime } WITH { time: event, ts: at }");
	engine.admin("CREATE DEFERRED VIEW td::v { id: int4, at: datetime } WITH { time: event } AS { FROM td::src }");

	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let dag = reload_dag(&engine, &mut txn, "v");

	assert_eq!(recheck(&engine, &mut txn, &dag), None, "the flow must be legal before the source is altered");

	alter_source_time(&engine, &mut txn, "src", processing());

	assert_eq!(
		recheck(&engine, &mut txn, &dag).as_deref(),
		Some("FLOW_040"),
		"an event-time flow over a source that no longer supplies one must be rejected on re-registration"
	);
}

#[test]
fn an_explicit_processing_override_survives_the_source_changing_underneath_it() {
	// Separates the re-check from a blanket "the source changed, reject" rule, which would
	// refuse to boot flows behaving exactly as declared.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE td");
	engine.admin("CREATE TABLE td::src { id: int4, at: datetime } WITH { time: event, ts: at }");
	engine.admin(
		"CREATE DEFERRED VIEW td::v { id: int4, at: datetime } WITH { time: processing } AS { FROM td::src }",
	);

	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let dag = reload_dag(&engine, &mut txn, "v");

	for time in [processing(), event()] {
		alter_source_time(&engine, &mut txn, "src", time.clone());
		assert_eq!(
			recheck(&engine, &mut txn, &dag),
			None,
			"an explicit processing override must stay legal over a {time:?} source"
		);
	}
}
