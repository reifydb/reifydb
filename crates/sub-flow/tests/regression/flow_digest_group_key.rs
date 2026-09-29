// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_schema::FieldRef;
use reifydb_core::{
	common::{ChangeVersion, CommitVersion, WindowKind, WindowSize},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, ChangeOrigin, Diff},
	},
	value::{
		batch::batch,
		column::{builder::ColumnBuilder, factory},
	},
};
use reifydb_flow::context::FlowContext;
use reifydb_flow_async::operator::{
	HostOperator,
	aggregation::operator::AggregateOperator,
	host::TxnHostContext,
	window::operator::{WindowConfig, WindowOperator},
};
use reifydb_rql::expression::parse_expression;
use reifydb_test_harness::{engine::TestEngine, operator::transaction::FlowTxn};
use reifydb_value::{
	Result,
	value::{
		Value,
		datetime::DateTime,
		digest::Digest,
		duration::Duration,
		system_columns::{SystemColumn, with_system_column},
		value_type::ValueType,
	},
};

const SOURCE_OPERATOR: OperatorId = OperatorId(51);
const SUBJECT: OperatorId = OperatorId(52);

fn digest_type() -> ValueType {
	ValueType::Digest {
		inner: Box::new(ValueType::Float8),
		accuracy: 10_000,
	}
}

fn filled_key() -> (FieldRef, ArrayRef) {
	let mut buffer = ColumnBuilder::with_capacity(digest_type(), 2);
	for values in [[1.0, 2.0], [3.0, 4.0]] {
		let mut digest = Digest::new(ValueType::Float8, 10_000).unwrap();
		for value in values {
			digest.add_value(&Value::float8(value)).unwrap();
		}
		buffer.push_value(Value::Digest(Box::new(digest)));
	}
	buffer.finish("d")
}

fn all_none_key() -> (FieldRef, ArrayRef) {
	factory::none_typed("d", ValueType::Option(Box::new(digest_type())), 2)
}

fn keyed_by(key: (FieldRef, ArrayRef)) -> Change {
	let at = DateTime::from_millis(1_000_000);
	let system = [
		(SystemColumn::RowNumbers, factory::uint8("#rownum", [1u64, 2]).1),
		(SystemColumn::CreatedAt, factory::datetime("#created_at", [at; 2]).1),
		(SystemColumn::UpdatedAt, factory::datetime("#updated_at", [at; 2]).1),
		(SystemColumn::Time, factory::datetime("#time", [at; 2]).1),
	];
	let input = system.into_iter().fold(
		batch(vec![factory::int4("k", [1, 2]), key]).expect("user columns form a batch"),
		|columns, (column, array)| {
			with_system_column(columns, column, array).expect("a system column attaches")
		},
	);
	let mut diff = Diff::insert(input);
	diff.set_origin(Some(ChangeOrigin::Flow(SOURCE_OPERATOR)));
	Change::from_flow(SOURCE_OPERATOR, ChangeVersion::from(CommitVersion(1)), vec![diff], at)
}

fn apply(engine: &TestEngine, operator: &mut dyn HostOperator, change: Change) -> Result<Change> {
	let mut txn = engine.flow_txn().deferred();
	operator.apply(&mut TxnHostContext::new(&mut txn, SUBJECT), change)
}

fn refusal(result: Result<Change>) -> Option<String> {
	match result {
		Ok(output) => Some(format!("built groups from a digest key: {:?}", output.diffs)),
		Err(err) => {
			let diagnostic = err.diagnostic();
			if diagnostic.code == "AGGREGATE_008" && diagnostic.fragment.text() == "d" {
				None
			} else {
				Some(format!("refused with {} on '{}'", diagnostic.code, diagnostic.fragment.text()))
			}
		}
	}
}

fn window(engine: &TestEngine, kind: WindowKind) -> WindowOperator {
	WindowOperator::new(WindowConfig {
		parent_schema: None,
		operator: SUBJECT,
		kind,
		group_by: parse_expression("d").expect("group key parses"),
		aggregations: parse_expression("n: math::count(k)").expect("aggregation parses"),
		runtime_context: engine.executor().runtime_context.clone(),
		routines: engine.executor().routines.clone(),
		lateness: None,
		immutable: None,
		ctx: Arc::new(FlowContext::default()),
	})
	.expect("the window operator must build")
}

fn window_kinds() -> Vec<(&'static str, WindowKind)> {
	let minute = Duration::from_seconds(60).unwrap();
	vec![
		(
			"tumbling over time",
			WindowKind::Tumbling {
				size: WindowSize::Duration(minute),
			},
		),
		(
			"tumbling over count",
			WindowKind::Tumbling {
				size: WindowSize::Count(10),
			},
		),
		(
			"sliding",
			WindowKind::Sliding {
				size: WindowSize::Duration(minute),
				slide: WindowSize::Duration(Duration::from_seconds(20).unwrap()),
			},
		),
		(
			"rolling over time",
			WindowKind::Rolling {
				size: WindowSize::Duration(minute),
				lag: None,
				pane: None,
			},
		),
		(
			"rolling over count",
			WindowKind::Rolling {
				size: WindowSize::Count(10),
				lag: None,
				pane: None,
			},
		),
		(
			"session",
			WindowKind::Session {
				gap: minute,
			},
		),
	]
}

#[test]
fn a_flow_aggregate_by_an_all_none_digest_column_is_an_error_like_the_batch_group_by() {
	// A digest key column holding only none still has a digest type, so it must be refused before any group forms.
	let engine = TestEngine::new();
	let mut operator = AggregateOperator::new(
		None,
		SUBJECT,
		parse_expression("d").expect("group key parses"),
		parse_expression("n: math::count(k)").expect("aggregation parses"),
		engine.executor().routines.clone(),
		engine.executor().runtime_context.clone(),
	)
	.expect("the aggregate operator must build");

	let result = apply(&engine, &mut operator, keyed_by(all_none_key()));

	assert_eq!(refusal(result), None);
}

#[test]
fn a_flow_window_by_a_digest_column_is_an_error_like_the_batch_group_by_in_every_window_kind() {
	// Each window kind groups on its own apply path, so any one of them could still group by a digest.
	let mut failures = Vec::new();
	for (name, kind) in window_kinds() {
		for (key_name, key) in [("a digest", filled_key()), ("an all-none Option(digest)", all_none_key())] {
			let engine = TestEngine::new();
			let mut operator = window(&engine, kind.clone());
			if let Some(failure) = refusal(apply(&engine, &mut operator, keyed_by(key))) {
				failures.push(format!("{name} window by {key_name} key {failure}"));
			}
		}
	}

	assert!(failures.is_empty(), "{failures:#?}");
}
