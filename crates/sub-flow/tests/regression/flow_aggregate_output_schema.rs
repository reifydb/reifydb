// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use reifydb_core::{
	common::{WindowKind, WindowSize},
	expression::Expression,
	interface::catalog::flow::OperatorId,
	value::{batch::views, column::builder::ColumnBuilder},
};
use reifydb_flow::context::FlowContext;
use reifydb_flow_async::operator::{
	HostOperator,
	aggregation::operator::AggregateOperator,
	window::operator::{WindowConfig, WindowOperator},
};
use reifydb_rql::expression::parse_expression;
use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::{duration::Duration, value_type::ValueType};

const OUTPUTS: [&str; 3] = [
	"n: math::count(latency)",
	"p: stats::approx_percentile(latency, 0.5, 0.01)",
	"sum99: stats::approx_percentile(latency, 0.99, 0.01) + stats::approx_percentile(latency, 0.5, 0.01)",
];

fn parent() -> SchemaRef {
	Arc::new(Schema::new(vec![
		ColumnBuilder::with_capacity(ValueType::Int4, 0).finish("a").0,
		ColumnBuilder::with_capacity(ValueType::Int4, 0).finish("b").0,
		ColumnBuilder::with_capacity(ValueType::Float8, 0).finish("latency").0,
	]))
}

fn expressions(sources: &[&str]) -> Vec<Expression> {
	sources.iter().flat_map(|source| parse_expression(source).expect("expression parses")).collect()
}

fn described(schema: Option<SchemaRef>) -> Vec<(String, ValueType)> {
	let columns = RecordBatch::new_empty(schema.expect("the operator must report its output schema"));
	views(&columns)
		.expect("the output schema reads")
		.iter()
		.map(|view| (view.field.name().to_string(), view.get_type()))
		.collect()
}

#[test]
fn a_flow_aggregate_output_schema_is_its_group_keys_then_its_outputs_never_its_input_or_slot_columns() {
	// A downstream natural join keys on these names, so an input column or an __aggregate slot here breaks it.
	let engine = TestEngine::new();
	let operator = AggregateOperator::new(
		Some(parent()),
		OperatorId(71),
		expressions(&["a"]),
		expressions(&OUTPUTS),
		engine.executor().routines.clone(),
		engine.executor().runtime_context.clone(),
	)
	.expect("the aggregate operator must build");

	assert_eq!(
		described(HostOperator::output_schema(&operator)),
		vec![
			("a".to_string(), ValueType::Int4),
			("n".to_string(), ValueType::Any),
			("p".to_string(), ValueType::Any),
			("sum99".to_string(), ValueType::Any),
		]
	);
}

#[test]
fn a_flow_window_output_schema_is_its_group_keys_then_its_outputs_including_the_window_bounds() {
	// window::start is an output like any aggregate, so leaving it out would hide a column a join can share.
	let engine = TestEngine::new();
	let operator = WindowOperator::new(WindowConfig {
		parent_schema: Some(parent()),
		operator: OperatorId(72),
		kind: WindowKind::Tumbling {
			size: WindowSize::Duration(Duration::from_seconds(60).expect("60 seconds is a duration")),
		},
		group_by: expressions(&["a"]),
		aggregations: expressions(&[
			OUTPUTS[0],
			OUTPUTS[1],
			OUTPUTS[2],
			"opened: window::start()",
			"closed: window::end()",
		]),
		runtime_context: engine.executor().runtime_context.clone(),
		routines: engine.executor().routines.clone(),
		lateness: None,
		immutable: None,
		ctx: Arc::new(FlowContext::default()),
	})
	.expect("the window operator must build");

	assert_eq!(
		described(HostOperator::output_schema(&operator)),
		vec![
			("a".to_string(), ValueType::Int4),
			("n".to_string(), ValueType::Any),
			("p".to_string(), ValueType::Any),
			("sum99".to_string(), ValueType::Any),
			("opened".to_string(), ValueType::Any),
			("closed".to_string(), ValueType::Any),
		]
	);
}
