// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// The second line of defence for declared retention spans, and the one nothing has ever exercised.
// FLOW_045's existing test passes because the GRAMMAR refuses `with { ttl }` on a stateless node, so
// the span walk itself is never reached; FLOW_044 has no grammar equivalent at all, because the
// grammar cannot see whether a guest operator declares Reclaim.
//
// Both diagnostics have to survive the route the grammar cannot see: a flow reloaded from the catalog
// whose settings no longer match what DDL saw. This file builds that state directly - the flow's own
// DAG comes back from the catalog and only the operator settings differ - and pins that the walk
// catches it. It is the same shape, and the same reason, as time_domain_recheck.rs.

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_catalog::{
	catalog::Catalog,
	store::operator_settings::create::create_operator_settings,
	vtable::system::operator_store::{OperatorInfo, OperatorStore},
};
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	row::{OperatorSettings, OperatorTtl},
};
use reifydb_engine::{flow::span::check_declared_spans, test_harness::TestEngine};
use reifydb_rql::flow::{flow::FlowDag, loader::load_flow_dag, node::FlowNodeType};
use reifydb_transaction::transaction::{Transaction, admin::AdminTransaction};
use reifydb_value::value::{duration::Duration, identity::IdentityId};

fn reclaim_bit() -> u32 {
	OperatorCapability::Reclaim.bit()
}

fn reload_dag(engine: &TestEngine, txn: &mut AdminTransaction, view: &str) -> FlowDag {
	let catalog = engine.catalog();
	let namespace = catalog.find_namespace_by_name(&mut Transaction::Admin(txn), "sp").unwrap().unwrap();
	let flow = catalog
		.find_flow_by_name(&mut Transaction::Admin(txn), namespace.id(), view)
		.unwrap()
		.expect("a flow must back the deferred view");
	load_flow_dag(&mut Transaction::Admin(txn), flow.id).unwrap()
}

// Writes a span onto a node the author never put one on. This is the drift the re-check exists for:
// DDL saw a node with no settings row, and the catalog it is resolved against has since moved.
fn declare_span(txn: &mut AdminTransaction, node: FlowNodeId) {
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

fn node_of(dag: &FlowDag, matches: impl Fn(&FlowNodeType) -> bool) -> FlowNodeId {
	dag.topological_order()
		.unwrap()
		.into_iter()
		.find(|id| matches(&dag.get_node(id).unwrap().ty))
		.expect("the dag must contain the node the test is about")
}

fn store_with(operator: &str, capabilities: u32) -> OperatorStore {
	let store = OperatorStore::new();
	store.add(OperatorInfo {
		operator: operator.to_string(),
		library_path: Default::default(),
		api: 1,
		capabilities,
		input_columns: vec![],
		output_columns: vec![],
	});
	store
}

fn recheck(
	engine: &TestEngine,
	operators: &OperatorStore,
	txn: &mut AdminTransaction,
	dag: &FlowDag,
) -> Option<String> {
	let catalog: Catalog = engine.catalog();
	match check_declared_spans(&catalog, operators, &mut Transaction::Admin(txn), dag) {
		Ok(()) => None,
		Err(err) => Some(err.diagnostic().code),
	}
}

#[test]
fn a_span_that_appears_on_a_stateless_node_after_definition_is_refused() {
	// Intent: FLOW_045 by the only route that can actually produce it. A map node cannot be given a
	// span through RQL - the grammar stops it - so the reachable failure is a settings row that
	// arrives some other way and comes back on restart. Left unchecked the catalog would claim the
	// node ages while the engine resolves its horizon to Perpetual and never consults the span.
	// Mutation: drop the consults_declared_span arm from check_declared_spans and this returns None,
	// while every grammar-level span test keeps passing. Widening that predicate to holds_state has the
	// same effect one node type over: a window would start accepting a drifted ttl it never reads,
	// because a window's horizon comes from its operator's seal span and nothing else.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE sp");
	engine.admin("CREATE TABLE sp::t { id: int4, v: int4 }");
	engine.admin("CREATE DEFERRED VIEW sp::v { id: int4, v: int4 } AS { FROM sp::t map { id, v } }");

	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let dag = reload_dag(&engine, &mut txn, "v");
	let map = node_of(&dag, |ty| matches!(ty, FlowNodeType::Map { .. }));
	declare_span(&mut txn, map);

	assert_eq!(
		recheck(&engine, &OperatorStore::new(), &mut txn, &dag).as_deref(),
		Some("FLOW_045"),
		"a span on a node that holds no state must be refused when the flow is resolved again"
	);
}

#[test]
fn a_span_survives_the_recheck_when_the_operator_still_declares_reclaim() {
	// The control. Same drift, same span, on a node that can honour it - so a rule that refused every
	// re-checked span, or every span it could not attribute, fails here instead of passing both.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE sp");
	engine.admin("CREATE TABLE sp::a { id: int4, v: int4 }");
	engine.admin("CREATE TABLE sp::b { id: int4, v: int4 }");
	engine.admin("CREATE DEFERRED VIEW sp::u { id: int4, v: int4 } AS { FROM sp::a append { FROM sp::b } }");

	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let dag = reload_dag(&engine, &mut txn, "u");
	let append = node_of(&dag, |ty| matches!(ty, FlowNodeType::Append { .. }));
	declare_span(&mut txn, append);

	assert_eq!(
		recheck(&engine, &OperatorStore::new(), &mut txn, &dag),
		None,
		"append keeps keyed state and reclaims, so its span must still be honoured on a re-check"
	);
}

#[test]
fn a_span_is_refused_when_the_operator_catalog_reports_no_reclaim() {
	// Intent: FLOW_044 by the reload route, and the reason the operator catalog has to be complete.
	// An apply node is the only place a view author can declare a span the operator has no code to
	// honour, because every built-in stateful operator declares Reclaim in its own impl. The
	// capability is read by NAME from the operator catalog, which is why a statically registered
	// operator that never reaches that catalog is not a cosmetic gap: it reads as "cannot reclaim".
	// Mutation: make the lookup default to reclaiming when the operator is absent and the first
	// assertion below flips to None, leaving state to grow unbounded behind a span the catalog
	// advertises as honoured.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE sp");
	engine.admin("CREATE TABLE sp::t { id: int4, v: int4 }");
	engine.admin("CREATE DEFERRED VIEW sp::a { id: int4, v: int4 } AS { FROM sp::t APPLY tally{} }");

	let mut txn = engine.begin_admin(IdentityId::system()).unwrap();
	let dag = reload_dag(&engine, &mut txn, "a");
	let apply = node_of(&dag, |ty| matches!(ty, FlowNodeType::Apply { .. }));
	declare_span(&mut txn, apply);

	// Fail closed. An operator the catalog has never heard of cannot run at all, so accepting its
	// span would be strictly worse than refusing it.
	assert_eq!(
		recheck(&engine, &OperatorStore::new(), &mut txn, &dag).as_deref(),
		Some("FLOW_044"),
		"an operator absent from the catalog must not have its span accepted"
	);

	// Registered, and honest about not reclaiming.
	assert_eq!(
		recheck(&engine, &store_with("tally", 0), &mut txn, &dag).as_deref(),
		Some("FLOW_044"),
		"an operator that declares no Reclaim must have its span refused"
	);

	// The control: the same span, the same node, one capability bit different.
	assert_eq!(
		recheck(&engine, &store_with("tally", reclaim_bit()), &mut txn, &dag),
		None,
		"an operator that declares Reclaim can honour the span, so it must be accepted"
	);
}
