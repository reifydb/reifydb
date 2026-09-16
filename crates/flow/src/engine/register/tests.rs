// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::TimeDomain,
	interface::catalog::flow::{FlowId, OperatorId},
};
use reifydb_rql::{
	expression::parse_expression,
	flow::{
		flow::FlowDag,
		operator::{FlowEdge, FlowNode, OperatorDef},
	},
};
use reifydb_runtime::context::RuntimeContext;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::identity::IdentityId;

use crate::{
	engine::FlowEngineInner,
	operator::{metrics::OperatorSampleRegistry, provider::EmptyOperatorProvider},
	transaction::substrate::FlowSubstrate,
};

const FLOW: FlowId = FlowId(1);
const SOURCE: OperatorId = OperatorId(1);
const FILTER: OperatorId = OperatorId(2);
const AGGREGATE: OperatorId = OperatorId(3);

#[test]
fn registering_a_filter_with_a_variant_inside_an_expression_returns_the_compile_error_instead_of_panicking() {
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE s");
	engine.admin("CREATE ENUM s::status { Active, Inactive }");
	engine.admin("CREATE TABLE s::t { a: int4 }");

	let mut txn = engine.begin_command(IdentityId::system()).expect("a command transaction must open");
	let catalog = engine.catalog();
	let namespace = catalog
		.find_namespace_by_name(&mut Transaction::Command(&mut txn), "s")
		.expect("the namespace lookup must succeed")
		.expect("the namespace must exist");
	let table = catalog
		.find_table_by_name(&mut Transaction::Command(&mut txn), namespace.id(), "t")
		.expect("the table lookup must succeed")
		.expect("the table must exist");

	let mut builder = FlowDag::builder(FLOW);
	builder.add_node(FlowNode::new(
		SOURCE,
		OperatorDef::SourceTable {
			table: table.id,
			time_domain: TimeDomain::None,
		},
	));
	builder.add_node(FlowNode::new(
		FILTER,
		OperatorDef::Filter {
			conditions: parse_expression("a == 1 + s::status::Active").expect("the condition must parse"),
		},
	));
	builder.add_edge(FlowEdge::new(1, SOURCE, FILTER)).expect("both edge ends must exist");

	let mut inner = FlowEngineInner::new(
		engine.catalog(),
		engine.executor().routines.clone(),
		RuntimeContext::with_clock(engine.clock().clone()),
		Arc::new(EmptyOperatorProvider),
		FlowSubstrate::with_dictionary(engine.inner().dictionary_allocators(), engine.inner().operator_state()),
		OperatorSampleRegistry::new(),
	);

	let diagnostic = inner
		.register(&mut txn, builder.build())
		.expect_err("a filter the flow cannot compile must fail registration")
		.diagnostic();
	assert_eq!(diagnostic.code, "CA_102", "the error must be the variant compile error: {diagnostic:?}");
	assert_eq!(diagnostic.fragment.text(), "Active", "the error must point at the variant: {diagnostic:?}");
}

#[test]
fn registering_an_aggregate_with_a_bad_percentile_literal_returns_the_create_error_instead_of_panicking() {
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE s");
	engine.admin("CREATE TABLE s::t { g: int4, latency: Option(float8) }");
	let create = engine.admin_err(
		"CREATE DEFERRED VIEW s::bad { g: int4, p: Option(float8) } AS { FROM s::t | aggregate { p: stats::approx_percentile(latency, 1.5, 0.01) } by { g } }",
	);

	let mut txn = engine.begin_command(IdentityId::system()).expect("a command transaction must open");
	let catalog = engine.catalog();
	let namespace = catalog
		.find_namespace_by_name(&mut Transaction::Command(&mut txn), "s")
		.expect("the namespace lookup must succeed")
		.expect("the namespace must exist");
	let table = catalog
		.find_table_by_name(&mut Transaction::Command(&mut txn), namespace.id(), "t")
		.expect("the table lookup must succeed")
		.expect("the table must exist");

	let mut builder = FlowDag::builder(FLOW);
	builder.add_node(FlowNode::new(
		SOURCE,
		OperatorDef::SourceTable {
			table: table.id,
			time_domain: TimeDomain::None,
		},
	));
	builder.add_node(FlowNode::new(
		AGGREGATE,
		OperatorDef::Aggregate {
			by: parse_expression("g").expect("the group key must parse"),
			map: parse_expression("p: stats::approx_percentile(latency, 1.5, 0.01)")
				.expect("the aggregation must parse"),
		},
	));
	builder.add_edge(FlowEdge::new(1, SOURCE, AGGREGATE)).expect("both edge ends must exist");

	let mut inner = FlowEngineInner::new(
		engine.catalog(),
		engine.executor().routines.clone(),
		RuntimeContext::with_clock(engine.clock().clone()),
		Arc::new(EmptyOperatorProvider),
		FlowSubstrate::with_dictionary(engine.inner().dictionary_allocators(), engine.inner().operator_state()),
		OperatorSampleRegistry::new(),
	);

	let diagnostic = inner
		.register(&mut txn, builder.build())
		.expect_err("an aggregate call the flow cannot rewrite must fail registration")
		.diagnostic();
	assert_eq!(diagnostic.code, "FLOW_060", "the error must be the percentile range error: {diagnostic:?}");
	assert!(
		create.contains(&diagnostic.code),
		"register must report the code create reports, create said: {create}"
	);
	assert!(
		diagnostic.message.contains("aggregate output 'p'"),
		"the error must name the output like create does: {diagnostic:?}"
	);
}
