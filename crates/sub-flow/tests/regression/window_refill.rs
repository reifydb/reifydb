// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb_core::{common::WindowKind, interface::catalog::flow::OperatorId, value::column::columns::Columns};
use reifydb_flow::{
	context::FlowContext,
	operator::window::operator::{WindowConfig, WindowOperator},
};
use reifydb_routine::{
	function::default_in_process_functions, monoid::default_in_process_monoids,
	procedure::default_in_process_procedures,
};
use reifydb_routine_abi::registry::Routines;
use reifydb_rql::expression::parse_expression;
use reifydb_testing_flow::{generator, harness::Harness};
use reifydb_value::{
	factory::time::at_millis,
	value::{diff_type::DiffType, duration::Duration, row_number::RowNumber},
};

const SUBJECT: OperatorId = OperatorId(1);
const GAP_MS: i64 = 200;

fn routines() -> Routines {
	let b = Routines::builder();
	let b = default_in_process_functions(b);
	let b = default_in_process_procedures(b);
	default_in_process_monoids(b).configure()
}

fn session_harness(lateness: Option<Duration>) -> Harness<WindowOperator> {
	Harness::new(move |runtime| {
		WindowOperator::new(WindowConfig {
			parent_schema: Some(Columns::empty()),
			operator: SUBJECT,
			kind: WindowKind::Session {
				gap: Duration::from_milliseconds(GAP_MS).expect("representable"),
			},
			group_by: parse_expression("g").expect("group_by parses"),
			aggregations: parse_expression("total: math::sum(v)").expect("aggregation parses"),
			runtime_context: runtime,
			routines: routines(),
			lateness,
			immutable: None,
			ctx: Arc::new(FlowContext::default()),
		})
		.expect("the window operator must build")
	})
}

fn valued(number: u64, group: i32, value: i64, ms: u64) -> reifydb_core::row::Row {
	generator::row(RowNumber(number), group, value, at_millis(ms as i64))
}

#[test]
fn a_refilled_native_session_publishes_an_insert() {
	// Downstream already dropped the removed session, so an update retracting it corrupts every consumer.
	let mut h = session_harness(None);
	h.apply(generator::insert(vec![valued(1, 1, 5, 10_000)])).expect("the first row opens a session");
	let out = h.apply(generator::remove(vec![valued(1, 1, 5, 10_000)])).expect("the first row is withdrawn");
	let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
	assert_eq!(kinds, vec![DiffType::Remove], "precondition: the emptied session publishes its removal");

	let out = h.apply(generator::insert(vec![valued(2, 1, 7, 10_050)])).expect("the refill applies");

	let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
	assert_eq!(kinds, vec![DiffType::Insert], "a refilled session publishes once, as an insert: {:?}", out.diffs);
}

#[test]
fn an_emptied_native_session_publishes_nothing_on_seal() {
	// A second removal of a session downstream already dropped is a retraction of nothing.
	let mut h = session_harness(Some(Duration::from_milliseconds(10).expect("representable")));
	h.apply(generator::insert(vec![valued(1, 1, 5, 10_000)])).expect("the first row opens a session");
	let out = h.apply(generator::remove(vec![valued(1, 1, 5, 10_000)])).expect("the first row is withdrawn");
	let kinds: Vec<DiffType> = out.diffs.iter().map(|d| d.kind()).collect();
	assert_eq!(kinds, vec![DiffType::Remove], "precondition: the emptied session publishes its removal");

	let emitted = h.settle_timers(100_000).expect("the timers settle");

	assert!(
		emitted.iter().all(|change| change.diffs.is_empty()),
		"sealing an emptied session must publish nothing: {emitted:?}"
	);
}
