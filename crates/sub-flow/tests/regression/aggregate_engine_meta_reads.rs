// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! An aggregation pass reads each group's engine meta once, not twice.
//!
//! The emit decision reads the prior meta and then writes it back. A write derives its own pre-image by
//! reading the key again unless the read that preceded it claimed what it saw, so the second read is
//! invisible in every output-shaped assertion: same diffs, same state, twice the lookups.
//!
//! Engine meta sits on a keyspace the point tier refuses to cache, so that second lookup is a durable read
//! no cache absorbs. Counted per keyspace, because one aggregation pass reads several others.

use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change},
	key::operator::state::KeyspaceId,
	value::column::columns::Columns,
};
use reifydb_flow::operator::aggregation::operator::AggregateOperator;
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

fn row(number: u64, value: i64) -> reifydb_core::row::Row {
	let at = DateTime::from_epoch_millis(BASE_MS + number).expect("a row stamp is representable");
	generator::row(RowNumber(number), GROUP, value, at)
}

fn engine_meta_lookups(store: &OperatorStore) -> u64 {
	store.point_keyspace_metrics()
		.into_iter()
		.filter(|m| m.bucket == KeyspaceId::ENGINE_META)
		.map(|m| m.counters.hits + m.counters.misses)
		.sum()
}

#[test]
fn an_aggregation_pass_reads_each_groups_engine_meta_once() {
	let mut captured: Option<OperatorStore> = None;
	let mut harness = Harness::with_engine(|engine, runtime| {
		captured = Some(engine.inner().operator_state());
		AggregateOperator::new(
			Some(Columns::empty()),
			SUBJECT,
			parse_expression("g").expect("group_by parses"),
			parse_expression("total: math::sum(v)").expect("aggregation parses"),
			routines(),
			runtime,
		)
	});
	let store = captured.expect("the harness hands its engine to the builder");

	// The seed both creates the meta row and makes it durable: while it is still buffered the store answers
	// from the buffer, the tier is never consulted, and the count would read zero whatever the operator does.
	let seeded: Change = harness.apply(generator::insert(vec![row(1, 10)])).expect("seed applies");
	assert!(!seeded.diffs.is_empty(), "the seed must emit, or no meta row is ever written");
	assert!(store.flush_pending_blocking(), "the seed must be durable or the tier is never consulted at all");

	let before = engine_meta_lookups(&store);
	harness.apply(generator::insert(vec![row(2, 5)])).expect("the second batch applies");

	assert_eq!(
		engine_meta_lookups(&store) - before,
		1,
		"the read the emit decision already paid must classify the write, or every group reads its meta twice"
	);
}
