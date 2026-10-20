// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{str::FromStr, sync::Arc};

use arrow_array::{RecordBatch, cast::AsArray};
use arrow_schema::{Schema, SchemaRef};
use reifydb::{WithSubsystem, embedded, testing::db::TestDb};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	value::{batch::batch, column::builder::ColumnBuilder},
};
use reifydb_flow::{aggregate::AggregateContext, context::FlowContext};
use reifydb_flow_async::operator::aggregation::core::Aggregation;
use reifydb_rql::expression::parse_expression;
use reifydb_test_harness::{assert::column_values, engine::TestEngine};
use reifydb_value::value::{
	Value,
	constraint::{precision::Precision, scale::Scale},
	decimal::Decimal,
	duration::Duration,
	value_type::{
		ValueType,
		field::{FieldType, from_field, to_field},
	},
};

fn grouped(engine: &TestEngine, by: &str, aggregation: &str) -> Aggregation {
	Aggregation::new(
		OperatorId(1),
		None,
		parse_expression(by).expect("by parses"),
		parse_expression(aggregation).expect("aggregation parses"),
		engine.executor().routines.clone(),
		engine.executor().runtime_context.clone(),
		AggregateContext::Grouped,
		Arc::new(FlowContext::default()),
	)
	.expect("the aggregation core must build")
}

fn rows() -> RecordBatch {
	let spec: Vec<(&str, ValueType, Vec<Value>)> = vec![
		(
			"a",
			ValueType::Option(Box::new(ValueType::Int4)),
			vec![Value::Int4(5), Value::none(), Value::Int4(20)],
		),
		("s", ValueType::Utf8, vec![Value::Utf8("x".into()), Value::Utf8("y".into()), Value::Utf8("z".into())]),
		(
			"d",
			ValueType::DECIMAL,
			vec![
				Value::Decimal(Decimal::from_str("1.5").unwrap()),
				Value::Decimal(Decimal::from_str("2.25").unwrap()),
				Value::Decimal(Decimal::from_str("-3").unwrap()),
			],
		),
	];
	let columns = spec
		.into_iter()
		.map(|(name, ty, values)| {
			let mut buffer = ColumnBuilder::with_capacity(ty, values.len());
			for value in values {
				buffer.push_value(value);
			}
			buffer.finish(name)
		})
		.collect();
	batch(columns).expect("the rows form a batch")
}

fn hashes(by: &str) -> Vec<u128> {
	let groups = grouped(&TestEngine::new(), by, "n: math::count(a)")
		.compute_groups(&rows())
		.expect("the by keys every row");
	groups.hashes.into_iter().map(u128::from).collect()
}

#[test]
fn a_computed_by_takes_the_lowered_path() {
	// A computed by must group between over a nullable column per row, or a none row fails the view.
	let groups = grouped(&TestEngine::new(), "a between 2 and 10", "n: math::count(a)")
		.compute_groups(&rows())
		.expect("the by keys every row");

	assert_eq!(groups.values.len(), 3);
}

#[test]
fn a_computed_by_keeps_the_stored_group_hashes() {
	// Views store groups under these hashes, so any change splits every existing group after an upgrade.
	assert_eq!(
		hashes("a + 1"),
		[
			0x11b13c29c2d0e570884c28cf03a07001,
			0xe564c1757894f2ff03fda838da92ba29,
			0x6c1c30f0eb50c3022b5d053fa08ccc14
		]
	);
	assert_eq!(
		hashes("s + 'x'"),
		[
			0xb7e6ed8e09fb667ea72883d44f9600db,
			0x642a0d973caaff682fba9d2b6cf6bcf1,
			0x76a50a697099c2369aba482ca051cb0b
		]
	);
	assert_eq!(
		hashes("a"),
		[
			0xcc7ba8669734077647b6e85d5127ed46,
			0x1dc4be59b01488e546df12d4d7a0d663,
			0x659f54e23ae68b7e9cfe91fe2eb85978
		]
	);
	assert_eq!(
		hashes("d * 2"),
		[
			0xe89ee25b474698c7116c893fb3034e48,
			0x8d83cf103be599df109fc3142b0d86bc,
			0x66d5b3e1f51017fa4f4143b519a4a569
		]
	);
}

#[test]
fn a_computed_flow_slot_arg_takes_the_lowered_path() {
	// A computed slot input must answer between per row over a nullable column, or a none row fails the view.
	let core = grouped(&TestEngine::new(), "a", "n: math::count(a between 2 and 10)");

	let inputs = core.evaluate_slot_inputs(&rows()).expect("the slot input evaluates every row");

	assert_eq!(inputs.len(), 1);
	assert_eq!(inputs[0].1.as_boolean().iter().collect::<Vec<_>>(), [Some(true), None, Some(false)]);
}

#[test]
fn a_flow_output_keeps_its_type_and_value() {
	// Every node an output allows answers alike on both paths, so this pins what the view stores, not the path.
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db with flow"));
	db.admin("CREATE NAMESPACE app");
	db.admin("CREATE TABLE app::t { g: int4, a: Option(int4) }");
	db.admin(
		"CREATE DEFERRED VIEW app::v { g: int4, v: Option(int8) } AS { FROM app::t | aggregate { v: math::sum(a) + 1 } by { g } }",
	);

	db.command("INSERT app::t [{ g: 1, a: 5 }, { g: 1, a: none }, { g: 1, a: 20 }]");
	assert!(db.await_all_flows(Duration::from_seconds_const(5)), "the flow must process the insert");

	let frames = db.query("FROM app::v");
	let values: Vec<String> = column_values(&frames[0], "v").iter().map(|v| v.to_string()).collect();
	assert_eq!(values, ["26"]);
}

fn typed_parent() -> SchemaRef {
	Arc::new(Schema::new(vec![
		to_field("a", &FieldType::from(ValueType::Utf8)),
		to_field("n", &FieldType::from(ValueType::Int4)),
	]))
}

fn typed_grouped(engine: &TestEngine, by: &str, aggregation: &str) -> Aggregation {
	Aggregation::new(
		OperatorId(1),
		Some(typed_parent()),
		parse_expression(by).expect("by parses"),
		parse_expression(aggregation).expect("aggregation parses"),
		engine.executor().routines.clone(),
		engine.executor().runtime_context.clone(),
		AggregateContext::Grouped,
		Arc::new(FlowContext::default()),
	)
	.expect("the aggregation core must build")
}

#[test]
fn a_computed_group_key_and_a_min_over_a_computed_value_report_their_types() {
	// A computed group key or min reported as Any fails a lookup on it; RQL cannot name an inline by key.
	let engine = TestEngine::new();
	let aggregation = typed_grouped(&engine, "cast(a, utf8)", "m: math::min(cast(a, utf8))");
	let value_type = |index: usize| from_field(aggregation.output_schema.field(index)).unwrap().value_type;

	assert_eq!(value_type(0), Some(ValueType::Utf8), "the computed group key");
	assert_eq!(value_type(1), Some(ValueType::Option(Box::new(ValueType::Utf8))), "the min over a computed value");
}

#[test]
fn a_flow_sum_and_avg_report_the_types_the_flow_slots_emit() {
	// The flow sum widens each value, so the batch routine type Int4 would describe values that never arrive.
	let engine = TestEngine::new();
	let output_type = |aggregation: &str| {
		let aggregation = typed_grouped(&engine, "a", aggregation);
		from_field(aggregation.output_schema.field(1)).unwrap().value_type
	};
	let optional = |value_type: ValueType| Some(ValueType::Option(Box::new(value_type)));

	assert_eq!(output_type("s: math::sum(n)"), optional(ValueType::Int16), "a sum over int4 is widened to int16");
	assert_eq!(
		output_type("v: math::avg(n)"),
		optional(ValueType::decimal(Precision::MAX, Scale::new(6))),
		"an avg over int4 is a decimal at scale 6"
	);
}
