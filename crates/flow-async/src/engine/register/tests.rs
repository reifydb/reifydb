// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{
	common::{JoinType, TimeDomain},
	flow::{
		dag::FlowDag,
		operator::{FlowNode, LookupObject, OperatorDef},
	},
	interface::catalog::{
		flow::{FlowEdge, FlowId, OperatorId},
		id::{TableId, ViewId},
	},
	operator_with::{AggregateWith, LookupWith},
};
use reifydb_rql::expression::parse_expression;
use reifydb_runtime::context::RuntimeContext;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::{Transaction, command::CommandTransaction};
use reifydb_value::value::{identity::IdentityId, value_type::ValueType};

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
	builder.add_edge(FlowEdge::new(1, builder.id(), SOURCE, FILTER)).expect("both edge ends must exist");

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
			with: AggregateWith {},
		},
	));
	builder.add_edge(FlowEdge::new(1, builder.id(), SOURCE, AGGREGATE)).expect("both edge ends must exist");

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

const LOOKUP: OperatorId = OperatorId(4);
const SECOND_SOURCE: OperatorId = OperatorId(5);

fn lookup_engine_inner(engine: &TestEngine) -> FlowEngineInner {
	FlowEngineInner::new(
		engine.catalog(),
		engine.executor().routines.clone(),
		RuntimeContext::with_clock(engine.clock().clone()),
		Arc::new(EmptyOperatorProvider),
		FlowSubstrate::with_dictionary(engine.inner().dictionary_allocators(), engine.inner().operator_state()),
		OperatorSampleRegistry::new(),
	)
}

fn table_id(engine: &TestEngine, txn: &mut CommandTransaction, name: &str) -> TableId {
	let catalog = engine.catalog();
	let namespace = catalog
		.find_namespace_by_name(&mut Transaction::Command(txn), "s")
		.expect("the namespace lookup must succeed")
		.expect("the namespace must exist");
	catalog.find_table_by_name(&mut Transaction::Command(txn), namespace.id(), name)
		.expect("the table lookup must succeed")
		.expect("the table must exist")
		.id
}

fn view_id(engine: &TestEngine, txn: &mut CommandTransaction, name: &str) -> ViewId {
	let catalog = engine.catalog();
	let namespace = catalog
		.find_namespace_by_name(&mut Transaction::Command(txn), "s")
		.expect("the namespace lookup must succeed")
		.expect("the namespace must exist");
	catalog.find_view_by_name(&mut Transaction::Command(txn), namespace.id(), name)
		.expect("the view lookup must succeed")
		.expect("the view must exist")
		.id()
}

fn lookup_flow(left: TableId, right: LookupObject, key: &str) -> FlowDag {
	let mut builder = FlowDag::builder(FLOW);
	builder.add_node(FlowNode::new(
		SOURCE,
		OperatorDef::SourceTable {
			table: left,
			time_domain: TimeDomain::None,
		},
	));
	builder.add_node(FlowNode::new(
		LOOKUP,
		OperatorDef::Lookup {
			join_type: JoinType::Inner,
			right,
			left: parse_expression(key).expect("the left key must parse"),
			alias: Some("r".to_string()),
			with: LookupWith::default(),
		},
	));
	builder.add_edge(FlowEdge::new(1, builder.id(), SOURCE, LOOKUP)).expect("both edge ends must exist");
	builder.build()
}

#[test]
fn a_lookup_whose_left_key_is_text_against_an_int4_partition_column_fails_registration_with_lookup_003() {
	// A text key can never cast to int4, so the flow must be refused instead of matching nothing at runtime.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE s");
	engine.admin("CREATE TABLE s::l { k: utf8, a: int4 }");
	engine.admin("CREATE TABLE s::r { k: int4, b: int4 } WITH { partition: { by: { k } } }");

	let mut txn = engine.begin_command(IdentityId::system()).expect("a command transaction must open");
	let left = table_id(&engine, &mut txn, "l");
	let right = table_id(&engine, &mut txn, "r");

	let diagnostic = lookup_engine_inner(&engine)
		.register(&mut txn, lookup_flow(left, LookupObject::Table(right), "k"))
		.expect_err("a text key against an int4 partition column must fail registration")
		.diagnostic();
	assert_eq!(diagnostic.code, "LOOKUP_003", "got: {diagnostic:?}");
	assert!(
		diagnostic.message.contains(&ValueType::Utf8.to_string())
			&& diagnostic.message.contains(&ValueType::Int4.to_string()),
		"the message must name both types: {diagnostic:?}"
	);
}

#[test]
fn a_lookup_whose_int8_left_key_meets_an_int4_partition_column_registers() {
	// G4: a number casts to the right column's type at runtime, so only an out-of-range value may fail later.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE s");
	engine.admin("CREATE TABLE s::l { k: int8, a: int4 }");
	engine.admin("CREATE TABLE s::r { k: int4, b: int4 } WITH { partition: { by: { k } } }");

	let mut txn = engine.begin_command(IdentityId::system()).expect("a command transaction must open");
	let left = table_id(&engine, &mut txn, "l");
	let right = table_id(&engine, &mut txn, "r");

	lookup_engine_inner(&engine)
		.register(&mut txn, lookup_flow(left, LookupObject::Table(right), "k"))
		.expect("an int8 key against an int4 partition column must register");
}

#[test]
fn a_lookup_whose_text_left_key_meets_a_dictionary_partition_column_registers() {
	// The flow carries plain text and the lookup maps it to the id itself, so a dictionary column is text here.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE s");
	engine.admin("CREATE DICTIONARY s::syms FOR utf8 AS uint2");
	engine.admin("CREATE TABLE s::l { mint: utf8, a: int4 }");
	engine.admin(
		"CREATE TABLE s::r { mint: utf8 with { dictionary: s::syms }, b: int4 } WITH { partition: { by: { mint } } }",
	);

	let mut txn = engine.begin_command(IdentityId::system()).expect("a command transaction must open");
	let left = table_id(&engine, &mut txn, "l");
	let right = table_id(&engine, &mut txn, "r");

	lookup_engine_inner(&engine)
		.register(&mut txn, lookup_flow(left, LookupObject::Table(right), "mint"))
		.expect("a text key against a dictionary text partition column must register");
}

#[test]
fn a_lookup_on_a_sorted_view_fails_registration_with_lookup_001() {
	// A sorted view keys its rows by sort value, so a partition read would find nothing.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE s");
	engine.admin("CREATE TABLE s::l { k: int4, a: int4 }");
	engine.admin("CREATE TABLE s::src { k: int4, b: int4 }");
	engine.admin(
		"CREATE DEFERRED VIEW s::sorted { k: int4, b: int4 } WITH { partition: { by: { k } } } AS { FROM s::src SORT { b } }",
	);

	let mut txn = engine.begin_command(IdentityId::system()).expect("a command transaction must open");
	let left = table_id(&engine, &mut txn, "l");
	let right = view_id(&engine, &mut txn, "sorted");

	let diagnostic = lookup_engine_inner(&engine)
		.register(&mut txn, lookup_flow(left, LookupObject::View(right), "k"))
		.expect_err("a sorted view must not be looked up")
		.diagnostic();
	assert_eq!(diagnostic.code, "LOOKUP_001", "got: {diagnostic:?}");
}

#[test]
fn a_lookup_on_a_ringbuffer_view_fails_registration_with_lookup_001() {
	// A ring buffer view is out of the MVP (MD2), even when partitioned like the using columns.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE s");
	engine.admin("CREATE TABLE s::l { k: int4, a: int4 }");
	engine.admin("CREATE TABLE s::src { k: int4, b: int4 }");
	engine.admin(
		"CREATE DEFERRED RINGBUFFER VIEW s::rb { k: int4, b: int4 } WITH { capacity: 10, partition: { by: { k } } } AS { FROM s::src }",
	);

	let mut txn = engine.begin_command(IdentityId::system()).expect("a command transaction must open");
	let left = table_id(&engine, &mut txn, "l");
	let right = view_id(&engine, &mut txn, "rb");

	let diagnostic = lookup_engine_inner(&engine)
		.register(&mut txn, lookup_flow(left, LookupObject::View(right), "k"))
		.expect_err("a ring buffer view must not be looked up")
		.diagnostic();
	assert_eq!(diagnostic.code, "LOOKUP_001", "got: {diagnostic:?}");
}

#[test]
fn a_lookup_with_two_inputs_fails_registration_instead_of_reading_the_second_as_a_side() {
	// The right side is read from storage and gets no edge, so a second input means a malformed graph.
	let engine = TestEngine::new();
	engine.admin("CREATE NAMESPACE s");
	engine.admin("CREATE TABLE s::l { k: int4, a: int4 }");
	engine.admin("CREATE TABLE s::r { k: int4, b: int4 } WITH { partition: { by: { k } } }");

	let mut txn = engine.begin_command(IdentityId::system()).expect("a command transaction must open");
	let left = table_id(&engine, &mut txn, "l");
	let right = table_id(&engine, &mut txn, "r");

	let mut builder = FlowDag::builder(FLOW);
	builder.add_node(FlowNode::new(
		SOURCE,
		OperatorDef::SourceTable {
			table: left,
			time_domain: TimeDomain::None,
		},
	));
	builder.add_node(FlowNode::new(
		SECOND_SOURCE,
		OperatorDef::SourceTable {
			table: right,
			time_domain: TimeDomain::None,
		},
	));
	builder.add_node(FlowNode::new(
		LOOKUP,
		OperatorDef::Lookup {
			join_type: JoinType::Inner,
			right: LookupObject::Table(right),
			left: parse_expression("k").expect("the left key must parse"),
			alias: Some("r".to_string()),
			with: LookupWith::default(),
		},
	));
	builder.add_edge(FlowEdge::new(1, builder.id(), SOURCE, LOOKUP)).expect("both edge ends must exist");
	builder.add_edge(FlowEdge::new(2, builder.id(), SECOND_SOURCE, LOOKUP)).expect("both edge ends must exist");

	let diagnostic = lookup_engine_inner(&engine)
		.register(&mut txn, builder.build())
		.expect_err("a lookup with two inputs must fail registration")
		.diagnostic();
	assert!(diagnostic.message.contains("Lookup"), "the error must name the lookup: {diagnostic:?}");
}
