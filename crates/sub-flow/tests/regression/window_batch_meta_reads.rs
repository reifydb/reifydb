// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A windowed pass reads each group's batch meta once, not twice.
//!
//! The pass loads the group's high water before it folds and writes the advanced one back after. The
//! write derived its own pre-image by reading the key a second time, and read-modify-write hid it: the
//! emitted rows and the stored high water are identical either way, so only a lookup count can see it.
//!
//! The stored meta carries nothing but the high water the load already returned, and every group that
//! persists was loaded in the same pass, so the second read could never observe anything the first did
//! not.

use std::sync::Arc;

use reifydb_core::{
	common::{WindowKind, WindowSize},
	interface::{catalog::flow::OperatorId, change::Change},
	key::operator_state::KeyspaceId,
	value::column::columns::Columns,
};
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
use reifydb_store_operator::store::OperatorStore;
use reifydb_testing_flow::{generator, harness::Harness};
use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

const SUBJECT: OperatorId = OperatorId(1);
const BASE_MS: u64 = 1_000_000;
const GROUP: i32 = 1;

fn routines() -> Routines {
	let b = Routines::builder();
	let b = default_in_process_functions(b);
	let b = default_in_process_procedures(b);
	default_in_process_monoids(b).configure()
}

fn row(number: u64) -> reifydb_core::row::Row {
	let at = DateTime::from_epoch_millis(BASE_MS + number).expect("a row stamp is representable");
	generator::row(RowNumber(number), GROUP, 1, at)
}

fn window_meta_lookups(store: &OperatorStore) -> u64 {
	store.point_keyspace_metrics()
		.into_iter()
		.filter(|m| m.slot == KeyspaceId::WINDOW_META)
		.map(|m| m.counters.hits + m.counters.misses)
		.sum()
}

#[test]
fn a_tumbling_pass_reads_each_groups_batch_meta_once() {
	let mut captured: Option<OperatorStore> = None;
	let mut harness = Harness::with_engine(|engine, runtime| {
		captured = Some(engine.inner().operator_state());
		WindowOperator::new(WindowConfig {
			parent_schema: Some(Columns::empty()),
			operator: SUBJECT,
			kind: WindowKind::Tumbling {
				size: WindowSize::Count(1),
			},
			group_by: parse_expression("g").expect("group_by parses"),
			aggregations: parse_expression("total: math::sum(v)").expect("aggregation parses"),
			runtime_context: runtime,
			routines: routines(),
			lateness: None,
			immutable: None,
			ctx: Arc::new(FlowContext::default()),
		})
	});
	let store = captured.expect("the harness hands its engine to the builder");

	// The seed writes the meta row and makes it durable: while it is still buffered the store answers from
	// the buffer, the tier is never consulted, and the count reads zero whatever the operator does.
	let _seeded: Change = harness.apply(generator::insert(vec![row(1)])).expect("seed applies");
	assert!(store.flush_pending_blocking(), "the seed must be durable or the tier is never consulted at all");

	let before = window_meta_lookups(&store);
	let second = harness.apply(generator::insert(vec![row(2)])).expect("the second batch applies");

	// The meta is only written when the batch observes a new slot, and a batch that writes nothing reads
	// once whatever the write path does. A window this row merely joins emits an update; one it opens emits
	// an insert, so this is the same condition the persist guard tests, read off the output.
	assert!(
		second.diffs.iter().any(|diff| diff.pre().is_none() && diff.post().is_some()),
		"the second batch must open a new window, or no meta is written and the count below proves nothing"
	);

	assert_eq!(
		window_meta_lookups(&store) - before,
		1,
		"the load must classify the write it precedes, or every group reads its batch meta twice"
	);
}
