// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The grammar refuses `with { ttl }` on a stateless node, so the diagnostic is not reachable
// through RQL. Each test reloads the flow's own DAG and alters only the operator settings, which is
// the route the grammar cannot cover.

use reifydb_catalog::{catalog::Catalog, store::operator_settings::create::create_operator_settings};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	row::{OperatorSettings, OperatorTtl},
};
use reifydb_engine::{flow::span::check_declared_spans, test_harness::TestEngine};
use reifydb_rql::flow::{flow::FlowDag, loader::load_flow_dag, operator::OperatorDef};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::{duration::Duration, identity::IdentityId};

fn reload_dag(engine: &TestEngine, txn: &mut AdminTransaction, view: &str) -> FlowDag {
	let catalog = engine.catalog();
	let namespace = catalog.find_namespace_by_name(&mut Transaction::Admin(txn), "sp").unwrap().unwrap();
	let flow = catalog
		.find_flow_by_name(&mut Transaction::Admin(txn), namespace.id(), view)
		.unwrap()
		.expect("a flow must back the deferred view");
	load_flow_dag(&mut Transaction::Admin(txn), flow.id).unwrap()
}

fn declare_span(txn: &mut AdminTransaction, node: OperatorId) {
	// Writes a span onto a node DDL saw with no settings row at all.
	create_operator_settings(
		txn,
		node,
		&OperatorSettings {
			ttl: Some(OperatorTtl {
				duration: Duration::from_seconds(1).unwrap(),
			}),
			join: None,
		},
	)
	.unwrap();
}

fn node_of(dag: &FlowDag, matches: impl Fn(&OperatorDef) -> bool) -> OperatorId {
	dag.topological_order()
		.unwrap()
		.into_iter()
		.find(|id| matches(&dag.get_operator(id).unwrap().ty))
		.expect("the dag must contain the node the test is about")
}

fn recheck(engine: &TestEngine, txn: &mut AdminTransaction, dag: &FlowDag) -> Option<String> {
	let catalog: Catalog = engine.catalog();
	match check_declared_spans(&catalog, &mut Transaction::Admin(txn), dag) {
		Ok(()) => None,
		Err(err) => Some(err.diagnostic().code),
	}
}

#[test]
fn a_span_that_appears_on_a_stateless_node_after_definition_is_refused() {
	// Left unchecked the catalog claims the node ages while the engine resolves its horizon to
	// Perpetual and never consults the span.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE sp");
	engine.admin("CREATE TABLE sp::t { id: int4, v: int4 }");
	engine.admin("CREATE DEFERRED VIEW sp::v { id: int4, v: int4 } AS { FROM sp::t map { id, v } }");

	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let dag = reload_dag(&engine, &mut txn, "v");
	let map = node_of(&dag, |ty| matches!(ty, OperatorDef::Map { .. }));
	declare_span(&mut txn, map);

	assert_eq!(
		recheck(&engine, &mut txn, &dag).as_deref(),
		Some("FLOW_045"),
		"a span on a node that holds no state must be refused when the flow is resolved again"
	);
}

#[test]
fn a_span_survives_the_recheck_when_the_operator_holds_state_to_age() {
	// Same drift and span on a node that can honour it, so a rule refusing every re-checked span
	// fails here instead of passing both.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE sp");
	engine.admin("CREATE TABLE sp::a { id: int4, v: int4 }");
	engine.admin("CREATE TABLE sp::b { id: int4, v: int4 }");
	engine.admin("CREATE DEFERRED VIEW sp::u { id: int4, v: int4 } AS { FROM sp::a append { FROM sp::b } }");

	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let dag = reload_dag(&engine, &mut txn, "u");
	let append = node_of(&dag, |ty| matches!(ty, OperatorDef::Append { .. }));
	declare_span(&mut txn, append);

	assert_eq!(
		recheck(&engine, &mut txn, &dag),
		None,
		"append keeps keyed state, so its span must still be honoured on a re-check"
	);
}
