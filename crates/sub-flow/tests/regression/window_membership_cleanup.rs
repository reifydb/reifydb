// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A window must not keep a membership record for a row it no longer holds.
//!
//! The record answers "which windows is this row in", and nothing recomputes it - so a stale one
//! outlives its row and is trusted later. For a session window that is a wrong answer, because the
//! assignment depends on arrival history and the row has since left; `window_session_*` covers that.
//! For a fixed grid it is only a leak, which no value comparison can see: the sweeps stay green with
//! the cleanup removed. Hence a footprint bound rather than an oracle.
//!
//! Stated as growth rather than an exact count because accumulators and meta share the keyspace, and
//! pinning their sizes here would make this fail for reasons it makes no claim about.

use std::sync::Arc;

use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::catalog::flow::OperatorId,
};
use reifydb_routine::{
	function::default_native_functions, monoid::default_native_monoids, procedure::default_native_procedures,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::parse_expression;
use reifydb_sub_flow::{
	context::FlowContext,
	operator::{
		OperatorCell,
		scan::series::SourceSeriesOperator,
		window::operator::{WindowConfig, WindowOperator},
	},
};
use reifydb_testing_flow::{generator, harness::Harness};
use reifydb_value::{
	factory::time::at_millis,
	value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
};

const SOURCE: OperatorId = OperatorId(0);
const SUBJECT: OperatorId = OperatorId(1);
const BASE_MS: u64 = 1_000_000;

// One group and one window, so every row shares a single accumulator and the only thing that can
// grow with the row count is the per-row membership record.
const GROUP: i32 = 1;
const WINDOW_SECS: i64 = 3_600;

fn routines() -> Routines {
	let b = Routines::builder();
	let b = default_native_functions(b);
	let b = default_native_procedures(b);
	default_native_monoids(b).configure()
}

fn harness(kind: WindowKind) -> Harness<WindowOperator> {
	Harness::new(move |runtime| {
		WindowOperator::new(WindowConfig {
			parent: OperatorCell::new(SourceSeriesOperator::new(SOURCE)),
			operator: SUBJECT,
			kind: kind.clone(),
			group_by: parse_expression("g").expect("group_by parses"),
			aggregations: parse_expression("total: math::sum(v)").expect("aggregation parses"),
			runtime_context: runtime,
			routines: routines(),
			grace: Duration::default(),
			ctx: Arc::new(FlowContext::default()),
		})
	})
}

fn row(number: u64) -> reifydb_core::row::Row {
	let at = DateTime::from_epoch_millis(BASE_MS + number).expect("a row stamp is representable");
	generator::row(RowNumber(number), GROUP, 1, at)
}

/// Inserts `count` rows into one window and removes every one of them, then reports what state is
/// left behind.
fn rows_left_after_churn(kind: WindowKind, count: u64) -> usize {
	let mut h = harness(kind);
	let rows: Vec<_> = (1..=count).map(row).collect();

	h.apply(generator::insert(rows.clone())).expect("the inserts apply");
	for r in rows {
		h.apply(generator::remove(vec![r])).expect("the removes apply");
	}

	let footprint = h.footprint().expect("the footprint reads");
	footprint.data_rows + footprint.node_scoped_data_rows + footprint.identity_rows
}

#[test]
fn a_count_tumbling_window_keeps_no_state_per_removed_row() {
	// Count-based, not duration: a duration window derives its window from the coordinate and never
	// records membership at all, so the same test over one would pass with the cleanup deleted.
	// Measured - it did.
	//
	// Ten times the rows through the same window must not leave ten times the state: every one of
	// them was withdrawn, so what remains describes the window, not its history.
	let kind = WindowKind::Tumbling {
		size: WindowSize::Count(1_000),
	};

	let few = rows_left_after_churn(kind.clone(), 2);
	let many = rows_left_after_churn(kind, 20);

	assert_eq!(
		few, many,
		"state left behind grew from {few} to {many} rows when the corpus grew tenfold; a removed row is \
		 still holding a membership record"
	);
}

#[test]
fn a_sliding_window_keeps_no_state_per_removed_row() {
	// The sliding engine records one membership entry per window a row joined, so a leak here is
	// wider than tumbling's rather than merely a copy of it.
	let kind = WindowKind::Sliding {
		size: WindowSize::Duration(Duration::from_seconds(WINDOW_SECS).expect("representable")),
		slide: WindowSize::Duration(Duration::from_seconds(WINDOW_SECS / 2).expect("representable")),
	};

	let few = rows_left_after_churn(kind.clone(), 2);
	let many = rows_left_after_churn(kind, 20);

	assert_eq!(
		few, many,
		"state left behind grew from {few} to {many} rows when the corpus grew tenfold; a removed row is \
		 still holding a membership record"
	);
}

#[test]
fn a_session_window_keeps_no_state_per_removed_row() {
	// The kind where the stale record was also a wrong answer. Kept here so the leak is pinned
	// independently of the value comparison in `window_session_*`, which a future oracle change
	// could stop exercising.
	let kind = WindowKind::Session {
		gap: Duration::from_seconds(WINDOW_SECS).expect("representable"),
	};

	let few = rows_left_after_churn(kind.clone(), 2);
	let many = rows_left_after_churn(kind, 20);

	assert_eq!(
		few, many,
		"state left behind grew from {few} to {many} rows when the corpus grew tenfold; a removed row is \
		 still holding a membership record"
	);
}

// The exact shape the session sweep found, pinned as a fixed sequence. The seed that surfaced it
// (12750666829617941778) is not a durable handle: it names a position in a generated corpus, so any
// change to the workload or its parameters points it somewhere else entirely.

fn valued(number: u64, value: i64, ms: u64) -> reifydb_core::row::Row {
	generator::row(RowNumber(number), GROUP, value, at_millis(ms))
}

#[test]
fn a_session_a_row_has_left_is_not_resurrected_by_a_later_update() {
	// Four steps, and the last one is the claim:
	//
	//   1. row 1 at 17890 opens session 0
	//   2. row 2 at 18381 is more than the gap ahead, so session 0 closes and 1 opens
	//   3. row 1 is withdrawn, emptying session 0, and re-arrives at 17890 - now refused, because it sits more than
	//      a gap before the open session's start
	//   4. row 1 is updated
	//
	// After 3 row 1 belongs to no session at all, so 4 must publish nothing. It used to find row 1's
	// membership record from step 1 - never cleaned up - treat the row as still filed in session 0,
	// and amend that closed and empty session into existence carrying the update's new value.
	const GAP_MS: i64 = 200;
	let mut h = harness(WindowKind::Session {
		gap: Duration::from_milliseconds(GAP_MS).expect("representable"),
	});

	h.apply(generator::insert(vec![valued(1, 94, 17_890)])).expect("the first row opens a session");
	h.apply(generator::insert(vec![valued(2, 97, 18_381)])).expect("the second row rotates the session");

	h.apply(generator::remove(vec![valued(1, 94, 17_890)])).expect("the first row is withdrawn");
	let refused = h.apply(generator::insert(vec![valued(1, 72, 17_890)])).expect("the first row re-arrives");
	assert!(
		refused.diffs.is_empty(),
		"fixture: the re-arrival must be refused, or the update below is not the case under test: {:?}",
		refused.diffs
	);

	let out = h
		.apply(generator::update(vec![(valued(1, 72, 17_890), valued(1, 33, 17_890))]))
		.expect("the update applies");

	assert!(
		out.diffs.is_empty(),
		"an update to a row that belongs to no session must publish nothing, but it republished: {:?}",
		out.diffs
	);
}
