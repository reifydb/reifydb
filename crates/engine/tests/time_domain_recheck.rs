// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The second line of defence described in `engine::flow::time_domain`: the domain walk runs at
// DEFINITION time, and registration re-runs it for flows loaded from the catalog on restart, "whose
// sources may have been altered since".
//
// Nothing about that claim is exercised by the definition-time path, because there the flow and its
// sources are consistent by construction - the walk has just been run over them. What matters is the
// case the doc names: a flow that was legal when it was defined, reloaded against a catalog that has
// since moved underneath it. This file builds exactly that state - the flow's own DAG is reloaded from
// the catalog, and only the source object's `TimeSource` differs from what DDL saw - and pins that the
// re-check catches it, in both directions.

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

// Rewrites the source table's declared TimeSource in the catalog, leaving the flow's own record
// untouched. This is the drift the re-check exists for: on a restart the flow comes back exactly as it
// was defined, while the catalog it is resolved against has moved.
fn alter_source_time(engine: &TestEngine, txn: &mut AdminTransaction, table: &str, time: TimeSource) {
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
// Intent: a flow that declared nothing over a processing-time source was legal at DDL, and must stop
// being legal once that source starts declaring event time. This is the trap FLOW_041 exists for,
// arriving by the one route definition-time validation cannot see - the author never edited the view,
// so nothing re-runs the DDL check, and without the registration re-check the flow would come back on
// restart and quietly bucket event-time rows by wall clock.
// Mutation: drop the check_time_domain call from register_with_transaction and the reloaded DAG below
// sails through, while every definition-time test still passes.
fn a_source_that_gains_event_time_invalidates_an_undeclared_flow() {
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE td");
	engine.admin("CREATE TABLE td::src { id: int4, at: datetime }");
	engine.admin("CREATE DEFERRED VIEW td::v { id: int4, at: datetime } AS { FROM td::src }");

	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let dag = reload_dag(&engine, &mut txn, "v");

	// Precondition: as defined, the reloaded flow is accepted. Without this the test could pass
	// against a DAG that was never valid in the first place.
	assert_eq!(recheck(&engine, &mut txn, &dag), None, "the flow must be legal before the source is altered");

	alter_source_time(&engine, &mut txn, "src", event());

	assert_eq!(
		recheck(&engine, &mut txn, &dag).as_deref(),
		Some("FLOW_041"),
		"an undeclared flow over a source that now declares event time must be rejected on re-registration"
	);
}

#[test]
// Intent: the other direction, and the one that loses data rather than merely mislabelling it. A flow
// that explicitly declared event time was legal over an event-time source; if that source reverts to
// processing there is no column left to populate #time from, so every row would silently fall back to
// arrival while the flow still claims to bucket by event time.
// Mutation: reconcile only the (None, Event) cell and this flow re-registers happily.
fn a_source_that_loses_event_time_invalidates_an_event_time_flow() {
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
// Intent: the explicit processing override survives the same drift. This is the cell that separates the
// re-check from a blanket "the source changed, reject" rule - an author who wrote `time: processing`
// over an event-time source said so on purpose, and a source that gains or loses its populator must not
// retroactively invalidate that choice on restart. Without this the re-check would be indistinguishable
// from a staleness check, and would start refusing to boot flows that are behaving exactly as declared.
// Mutation: reject on any declared/source mismatch and this fails while both tests above still pass.
fn an_explicit_processing_override_survives_the_source_changing_underneath_it() {
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
