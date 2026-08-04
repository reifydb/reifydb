// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Every operator that retracts a row must retract the row it previously published.
//!
//! No view-level check can see this: `Session` keeps only the `post`, and both the view sink and the
//! aggregation operator reconcile against their own stored state. Measured, not assumed - making
//! `MapOperator` publish `post` as its own `pre` passed all 32 iterations of `map_chaos`.
//!
//! It matters because chaindex `block_trade` builds its retraction from `pre` verbatim, downstream of
//! a join with no view boundary between them.
//!
//! Here rather than in `tests/chaos.rs` because `make test-workspace` does not pass `--features
//! chaos`, and this belongs in the default gate.

use std::sync::Arc;

use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{
		catalog::flow::OperatorId,
		change::{Change, Diff},
	},
	state::budget::OperatorStateBudgetHandle,
	value::column::columns::Columns,
};
use reifydb_routine::{
	function::default_native_functions, monoid::default_native_monoids, procedure::default_native_procedures,
	routine::registry::Routines,
};
use reifydb_rql::expression::parse_expression;
use reifydb_sub_flow::{
	context::FlowContext,
	operator::{
		OperatorCell,
		aggregation::operator::AggregateOperator,
		distinct::operator::DistinctOperator,
		extend::ExtendOperator,
		filter::FilterOperator,
		gate::GateOperator,
		map::MapOperator,
		scan::series::SourceSeriesOperator,
		take::TakeOperator,
		window::operator::{WindowConfig, WindowOperator},
	},
};
use reifydb_testing_flow::{generator, harness::Harness};
use reifydb_value::value::{Value, datetime::DateTime, duration::Duration, row_number::RowNumber};

const SOURCE: OperatorId = OperatorId(0);
const SUBJECT: OperatorId = OperatorId(1);

// Past the epoch, so a row that lost its stamp is visibly different from one that kept it.
const BASE_MS: u64 = 1_000_000;

fn routines() -> Routines {
	let b = Routines::builder();
	let b = default_native_functions(b);
	let b = default_native_procedures(b);
	default_native_monoids(b).configure()
}

fn source() -> OperatorCell {
	OperatorCell::new(SourceSeriesOperator::new(SOURCE))
}

fn row(number: u64, group: i32, value: i64) -> reifydb_core::row::Row {
	let at = DateTime::from_timestamp_millis(BASE_MS + number).expect("a row stamp is representable");
	generator::row(RowNumber(number), group, value, at)
}

fn values(columns: &Columns) -> Vec<Value> {
	columns.columns.iter().map(|column| column.get_value(0)).collect()
}

// Exactly one: several updates in answer to a single-row update is a shape this makes no claim
// about, and reading the first would hide it.
#[track_caller]
fn sole_update(out: &Change, who: &str) -> (Vec<Value>, Vec<Value>) {
	let mut found = None;
	for diff in out.diffs.iter() {
		if let Diff::Update {
			pre,
			post,
			..
		} = diff
		{
			assert!(found.is_none(), "{who} published more than one update: {:?}", out.diffs);
			found = Some((values(pre), values(post)));
		}
	}
	found.unwrap_or_else(|| panic!("{who} published no update at all: {:?}", out.diffs))
}

#[track_caller]
fn retracts_the_previous_row(who: &str, out: &Change, expected_pre: &[Value], expected_post: &[Value]) {
	// Guards the fixture, not the operator: a pre equal to post would satisfy the real assertion
	// without exercising anything.
	assert_ne!(expected_pre, expected_post, "{who}: the fixture must change something");

	let (pre, post) = sole_update(out, who);
	assert_eq!(pre, expected_pre, "{who}: the update must retract the row as previously published");
	assert_eq!(post, expected_post, "{who}: and publish the row as it now stands");
}

#[test]
fn an_aggregate_update_retracts_the_total_it_previously_published() {
	// A consumer nets `post` minus `pre`; a pre equal to post nets zero and the group's contribution
	// never moves.
	let mut harness = Harness::new(|runtime| {
		AggregateOperator::new(
			source(),
			SUBJECT,
			parse_expression("g").expect("group_by parses"),
			parse_expression("total: math::sum(v)").expect("aggregation parses"),
			routines(),
			runtime,
			None,
		)
	});

	harness.apply(generator::insert(vec![row(1, 1, 10), row(2, 1, 5)])).expect("seed applies");
	let out = harness.apply(generator::update(vec![(row(1, 1, 10), row(1, 1, 40))])).expect("update applies");

	retracts_the_previous_row(
		"aggregate",
		&out,
		&[Value::Int4(1), Value::Int16(15)],
		&[Value::Int4(1), Value::Int16(45)],
	);
}

#[test]
fn a_distinct_update_retracts_the_row_it_previously_published() {
	// Row 2 outranks row 1 on the key, so `pre` must be row 2's old content and not row 1's.
	let mut harness = Harness::new(|runtime| {
		DistinctOperator::new(
			source(),
			SUBJECT,
			parse_expression("g").expect("the distinct key parses"),
			routines(),
			runtime,
			Arc::new(FlowContext::default()),
			None,
		)
	});

	harness.apply(generator::insert(vec![row(1, 1, 10), row(2, 1, 20)])).expect("seed applies");
	let out = harness.apply(generator::update(vec![(row(2, 1, 20), row(2, 1, 30))])).expect("update applies");

	retracts_the_previous_row(
		"distinct",
		&out,
		&[Value::Int4(1), Value::Int8(20)],
		&[Value::Int4(1), Value::Int8(30)],
	);
}

#[test]
fn a_take_update_retracts_the_row_it_previously_published() {
	// Limit above the corpus, so nothing is evicted and `pre` is isolated from the eviction machinery.
	let mut harness = Harness::new(|_| TakeOperator::new(source(), SUBJECT, 8));

	harness.apply(generator::insert(vec![row(1, 1, 10)])).expect("seed applies");
	let out = harness.apply(generator::update(vec![(row(1, 1, 10), row(1, 1, 70))])).expect("update applies");

	retracts_the_previous_row("take", &out, &[Value::Int4(1), Value::Int8(10)], &[Value::Int4(1), Value::Int8(70)]);
}

#[test]
fn a_gate_update_retracts_the_row_it_previously_published() {
	// Both values clear the threshold, so this is an in-result update rather than a crossing.
	let mut harness = Harness::new(|runtime| {
		GateOperator::new(
			source(),
			SUBJECT,
			parse_expression("v > 50").expect("the gate condition parses"),
			routines(),
			runtime,
			OperatorStateBudgetHandle::default(),
			Arc::new(FlowContext::default()),
		)
	});

	harness.apply(generator::insert(vec![row(1, 1, 60)])).expect("seed applies");
	let out = harness.apply(generator::update(vec![(row(1, 1, 60), row(1, 1, 80))])).expect("update applies");

	retracts_the_previous_row("gate", &out, &[Value::Int4(1), Value::Int8(60)], &[Value::Int4(1), Value::Int8(80)]);
}

#[test]
fn a_filter_update_retracts_the_row_it_previously_published() {
	// Both values pass. Crossings are membership changes the chaos sweeps already cover; `pre` is what
	// they cannot see.
	let mut harness = Harness::new(|runtime| {
		FilterOperator::new(
			source(),
			SUBJECT,
			parse_expression("v > 50").expect("the predicate parses"),
			routines(),
			runtime,
			Arc::new(FlowContext::default()),
		)
	});

	harness.apply(generator::insert(vec![row(1, 1, 60)])).expect("seed applies");
	let out = harness.apply(generator::update(vec![(row(1, 1, 60), row(1, 1, 80))])).expect("update applies");

	retracts_the_previous_row("filter", &out, &[Value::Int4(1), Value::Int8(60)], &[Value::Int4(1), Value::Int8(80)]);
}

#[test]
fn a_map_update_retracts_the_projection_it_previously_published() {
	// The mutation that survived the whole single-operator sweep. The derived column must move too: a
	// map projecting only the post gives 160 on both halves.
	let mut harness = Harness::new(|runtime| {
		let mut exprs = parse_expression("g").expect("parses");
		exprs.extend(parse_expression("doubled: v * 2").expect("parses"));
		MapOperator::new(source(), SUBJECT, exprs, routines(), runtime, Arc::new(FlowContext::default()))
	});

	harness.apply(generator::insert(vec![row(1, 1, 60)])).expect("seed applies");
	let out = harness.apply(generator::update(vec![(row(1, 1, 60), row(1, 1, 80))])).expect("update applies");

	retracts_the_previous_row(
		"map",
		&out,
		&[Value::Int4(1), Value::Int16(120)],
		&[Value::Int4(1), Value::Int16(160)],
	);
}

#[test]
fn an_extend_update_retracts_the_row_it_previously_published() {
	// Payload and derived column must both move; a derived column computed only from `post` leaves 160
	// beside a payload of 60.
	let mut harness = Harness::new(|runtime| {
		ExtendOperator::new(
			source(),
			SUBJECT,
			parse_expression("doubled: v * 2").expect("parses"),
			routines(),
			runtime,
			Arc::new(FlowContext::default()),
		)
	});

	harness.apply(generator::insert(vec![row(1, 1, 60)])).expect("seed applies");
	let out = harness.apply(generator::update(vec![(row(1, 1, 60), row(1, 1, 80))])).expect("update applies");

	retracts_the_previous_row(
		"extend",
		&out,
		&[Value::Int4(1), Value::Int8(60), Value::Int16(120)],
		&[Value::Int4(1), Value::Int8(80), Value::Int16(160)],
	);
}

#[test]
fn a_window_update_retracts_the_total_it_previously_published() {
	// Both rows sit in one 60s window, so the update moves the total instead of opening a second.
	const SECS: i64 = 60;
	let mut harness = Harness::new(|runtime| {
		WindowOperator::new(WindowConfig {
			parent: source(),
			operator: SUBJECT,
			kind: WindowKind::Tumbling {
				size: WindowSize::Duration(Duration::from_seconds(SECS).expect("representable")),
			},
			group_by: parse_expression("g").expect("group_by parses"),
			aggregations: parse_expression("total: math::sum(v)").expect("aggregation parses"),
			runtime_context: runtime,
			routines: routines(),
			grace: Duration::default(),
			state_budget: OperatorStateBudgetHandle::default(),
			ctx: Arc::new(FlowContext::default()),
		})
	});

	harness.apply(generator::insert(vec![row(1, 1, 10), row(2, 1, 5)])).expect("seed applies");
	let out = harness.apply(generator::update(vec![(row(1, 1, 10), row(1, 1, 40))])).expect("update applies");

	retracts_the_previous_row(
		"window",
		&out,
		&[Value::Int4(1), Value::Int16(15)],
		&[Value::Int4(1), Value::Int16(45)],
	);
}

// The join reads which side a diff belongs to off the diff's own origin, so its changes have to be
// tagged before they mean anything. Its own module to keep that machinery away from the rest.
mod join {
	use std::sync::Arc;

	use reifydb_core::{
		common::{CommitVersion, JoinType},
		interface::{
			catalog::flow::OperatorId,
			change::{Change, ChangeOrigin, Diff},
		},
		value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
	};
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_rql::expression::parse_expression;
	use reifydb_sub_flow::{
		context::FlowContext,
		operator::join::operator::{JoinOperator, JoinSideConfig},
	};
	use reifydb_testing_flow::harness::Harness;
	use reifydb_value::{
		fragment::Fragment,
		value::{
			Value, datetime::DateTime, row_number::RowNumber, system_columns::SystemColumns,
			value_type::ValueType,
		},
	};

	use super::values;

	const LEFT: OperatorId = OperatorId(1);
	const RIGHT: OperatorId = OperatorId(2);
	const JOIN: OperatorId = OperatorId(3);

	const LEFT_COLUMNS: [(&str, ValueType); 3] =
		[("lid", ValueType::Int8), ("k", ValueType::Int4), ("lv", ValueType::Int8)];
	const RIGHT_COLUMNS: [(&str, ValueType); 3] =
		[("rid", ValueType::Int8), ("k", ValueType::Int4), ("rv", ValueType::Int8)];

	// The left payload's position in the joined row. Computing it from the side widths is how the
	// first version of this silently asserted against the right side's key column instead.
	const LV: usize = 2;

	// Every cell, so a strategy cannot be added or re-routed without this noticing it is uncovered.
	const MATRIX: [(bool, bool, bool); 8] = [
		(false, false, false),
		(false, false, true),
		(false, true, false),
		(false, true, true),
		(true, false, false),
		(true, false, true),
		(true, true, false),
		(true, true, true),
	];

	fn schema(spec: &[(&str, ValueType)]) -> Columns {
		Columns::new(
			spec.iter()
				.map(|(name, ty)| {
					ColumnWithName::new(
						Fragment::internal(*name),
						ColumnBuffer::with_capacity(ty.clone(), 0),
					)
				})
				.collect(),
		)
	}

	fn row(spec: &[(&str, ValueType); 3], number: u64, key: i32, value: i64) -> Columns {
		let mut buffers: Vec<ColumnBuffer> =
			spec.iter().map(|(_, ty)| ColumnBuffer::with_capacity(ty.clone(), 1)).collect();
		buffers[0].push_value(Value::Int8(number as i64));
		buffers[1].push_value(Value::Int4(key));
		buffers[2].push_value(Value::Int8(value));
		let columns = spec
			.iter()
			.zip(buffers)
			.map(|((name, _), buffer)| ColumnWithName::new(Fragment::internal(*name), buffer))
			.collect();
		let at = DateTime::from_millis(1_000_000 + number);
		Columns::with_system(
			columns,
			SystemColumns::new(vec![RowNumber(number)], Vec::new(), vec![at], vec![at], vec![at]),
		)
	}

	fn tagged(mut diff: Diff, origin: OperatorId) -> Diff {
		diff.set_origin(Some(ChangeOrigin::Flow(origin)));
		diff
	}

	fn change(diffs: Vec<Diff>) -> Change {
		Change::from_flow(LEFT, CommitVersion(1), diffs, DateTime::default())
	}

	fn label(outer: bool, latest: bool, snapshot: bool) -> String {
		let mut out = String::from(match outer {
			true => "left",
			false => "inner",
		});
		if latest {
			out.push_str("+latest");
		}
		if snapshot {
			out.push_str("+snapshot");
		}
		out
	}

	fn build(engine: &TestEngine, outer: bool, latest: bool, snapshot: bool) -> JoinOperator {
		JoinOperator::new(
			JoinSideConfig {
				operator: LEFT,
				exprs: parse_expression("k").expect("left key parses"),
				schema: schema(&LEFT_COLUMNS),
			},
			JoinSideConfig {
				operator: RIGHT,
				exprs: parse_expression("k").expect("right key parses"),
				schema: schema(&RIGHT_COLUMNS),
			},
			JOIN,
			match outer {
				true => JoinType::Left,
				false => JoinType::Inner,
			},
			None,
			engine.executor(),
			snapshot,
			false,
			latest,
			None,
			None,
			Arc::new(FlowContext::default()),
		)
	}

	// Seeds a right slot, joins a left row, then updates the left row. Returns what the join first
	// published and the change the update produced.
	fn drive(outer: bool, latest: bool, snapshot: bool) -> (Vec<Value>, Change) {
		let mut harness = Harness::with_engine(|engine, _| build(engine, outer, latest, snapshot));
		harness.apply(change(vec![tagged(Diff::insert(row(&RIGHT_COLUMNS, 100, 1, 10)), RIGHT)]))
			.expect("the right side seeds");
		let inserted = harness
			.apply(change(vec![tagged(Diff::insert(row(&LEFT_COLUMNS, 1, 1, 7)), LEFT)]))
			.expect("the left row joins");
		let published =
			values(inserted.diffs.iter().filter_map(|diff| diff.post()).next().expect("a matched insert"));
		let updated = harness
			.apply(change(vec![tagged(
				Diff::update(row(&LEFT_COLUMNS, 1, 1, 7), row(&LEFT_COLUMNS, 1, 1, 8)),
				LEFT,
			)]))
			.expect("the left row updates");
		(published, updated)
	}

	// A left update reaches the output as an Update in some strategies and as Remove-then-Insert in
	// others. Both encode the same change, so the property is stated over whichever the strategy
	// chose.
	fn retraction_and_publication(out: &Change, who: &str) -> (Vec<Value>, Vec<Value>) {
		let mut retracted = None;
		let mut published = None;
		for diff in out.diffs.iter() {
			if let Some(pre) = diff.pre().map(values) {
				assert!(retracted.is_none(), "{who} retracted twice: {:?}", out.diffs);
				retracted = Some(pre);
			}
			if let Some(post) = diff.post().map(values) {
				assert!(published.is_none(), "{who} published twice: {:?}", out.diffs);
				published = Some(post);
			}
		}
		(
			retracted.unwrap_or_else(|| panic!("{who} retracted nothing: {:?}", out.diffs)),
			published.unwrap_or_else(|| panic!("{who} published nothing: {:?}", out.diffs)),
		)
	}

	#[test]
	fn every_join_strategy_retracts_the_joined_row_it_previously_published() {
		// The right side is held still, so this isolates the left half. That a retraction carries the
		// right value the emission used even after the right side moves is the harder property, held by
		// snapshot_join_retraction.rs.
		for (outer, latest, snapshot) in MATRIX {
			let who = label(outer, latest, snapshot);
			let (published, updated) = drive(outer, latest, snapshot);
			assert_eq!(published[LV], Value::Int8(7), "{who}: fixture - the left payload must sit at {LV}");

			let mut expected = published.clone();
			expected[LV] = Value::Int8(8);

			let (retracted, republished) = retraction_and_publication(&updated, &who);
			assert_eq!(retracted, published, "{who}: the retraction must carry the row previously published");
			assert_eq!(republished, expected, "{who}: and the publication must carry the updated row");
		}
	}

	#[test]
	fn no_join_strategy_publishes_a_row_less_diff() {
		// Regression. `Emitted::published` gated on `Columns::is_empty`, which asks whether there are
		// any COLUMNS, not any rows - and `retain_rows` with an empty index list returns the full column
		// set with zero rows in it. Every publish where all identities were fresh therefore carried a
		// second, row-less Update, and `snapshot` without `latest` was where it reached the output.
		//
		// Invisible to the chaos matrix, which drives those cells and passes: folding a row-less diff
		// changes nothing a view comparison looks at.
		//
		// The insert is checked as well as the update because the same partition runs on both, and only
		// the update path happened to surface it.
        for (outer, latest, snapshot) in MATRIX {
            let who = label(outer, latest, snapshot);
            let mut harness = Harness::with_engine(|engine, _| build(engine, outer, latest, snapshot));

            harness.apply(change(vec![tagged(Diff::insert(row(&RIGHT_COLUMNS, 100, 1, 10)), RIGHT)]))
                .expect("the right side seeds");
            let inserted = harness
                .apply(change(vec![tagged(Diff::insert(row(&LEFT_COLUMNS, 1, 1, 7)), LEFT)]))
                .expect("the left row joins");
            let updated = harness
                .apply(change(vec![tagged(
                    Diff::update(row(&LEFT_COLUMNS, 1, 1, 7), row(&LEFT_COLUMNS, 1, 1, 8)),
                    LEFT,
                )]))
                .expect("the left row updates");

            for (stage, out) in [("insert", &inserted), ("update", &updated)] {
                for diff in out.diffs.iter() {
                    let rows = diff.pre().map(|c| c.row_count()).unwrap_or(0)
                        + diff.post().map(|c| c.row_count()).unwrap_or(0);
                    assert!(rows > 0, "{who}: the {stage} published a row-less diff: {diff:?}");
                }
            }
        }
    }
}
