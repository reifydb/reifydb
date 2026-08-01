// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_abi::{flow::diff::DiffType, operator::capabilities::OperatorCapability};
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::catalog::flow::FlowNodeId,
	key::operator_group_state::{Keyspace, OperatorGroupStateKey},
};
use reifydb_sdk::{
	config::Config,
	error::Result as SdkResult,
	operator::{
		OperatorLogic, OperatorMetadata,
		column::operator::OperatorColumn,
		context::{OperatorContext, StateApi},
		view::{ChangeView, ColumnsView, DiffView, RowView},
	},
	row,
};
use reifydb_testing_flow::{generator, harness::Harness};
use reifydb_value::value::{
	constraint::TypeConstraint, datetime::DateTime, duration::Duration, row_number::RowNumber,
	value_type::ValueType,
};

const NODE: FlowNodeId = FlowNodeId(1);
const TTL_SECS: i64 = 60;
const SPAN_MS: u64 = TTL_SECS as u64 * 1_000;
const ARRIVAL_MS: u64 = 1_000;
// A group is due once the cutoff clears the whole bucket it was stamped in, so one grid width past
// the span is the first sweep that reaches an arrival at ARRIVAL_MS.
const SWEEP_MS: u64 = ARRIVAL_MS + SPAN_MS + SPAN_MS / 16;

struct ProbeRow {
	group: i64,
	is_new: i64,
}

row!(ProbeRow {
	group: i64,
	is_new: i64
});

const PROBE_OUTPUT_COLUMNS: &[OperatorColumn] = &[
	OperatorColumn {
		name: "group",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "The interned group the row was keyed on",
	},
	OperatorColumn {
		name: "is_new",
		type_constraint: TypeConstraint::unconstrained(ValueType::Int8),
		description: "1 if the row number was minted on this apply rather than found",
	},
];

/// A keyed guest holding group-scoped state that reports whether each apply minted its row number
/// or found one already there. `is_new` is the only signal surviving a sweep, and it decides
/// whether the operator publishes an Insert or an Update over a row the sink is still holding.
struct KeyedCounter;

impl OperatorMetadata for KeyedCounter {
	const NAME: &'static str = "keyed_counter";
	const API: u32 = 1;
	const VERSION: &'static str = "0.0.1";
	const DESCRIPTION: &'static str = "Counts per interned group and reports row-number provenance";
	const INPUT_COLUMNS: &'static [OperatorColumn] = &[];
	const OUTPUT_COLUMNS: &'static [OperatorColumn] = PROBE_OUTPUT_COLUMNS;
	const CAPABILITIES: &'static [OperatorCapability] = OperatorCapability::STANDARD_WITH_RECLAIM;
}

impl OperatorLogic for KeyedCounter {
	fn create(_node: FlowNodeId, _config: &Config) -> SdkResult<Self> {
		Ok(KeyedCounter)
	}

	fn apply(&mut self, ctx: &mut impl OperatorContext, change: impl ChangeView) -> SdkResult<()> {
		let mut rows: Vec<ProbeRow> = Vec::new();
		let mut numbers: Vec<RowNumber> = Vec::new();
		for di in 0..change.diff_count() {
			let Some(diff) = change.diff(di) else {
				continue;
			};
			if diff.kind() == DiffType::Remove {
				continue;
			}
			let Some(columns) = diff.post() else {
				continue;
			};
			for i in 0..columns.row_count() {
				let Some(row) = columns.row(i) else {
					continue;
				};
				let Some(group_value) = row.i32("g") else {
					continue;
				};
				let group = ctx.intern_group(&EncodedKey::new(group_value.to_be_bytes().to_vec()))?;

				let state = OperatorGroupStateKey::inner_encoded(group, Keyspace::FIRST_CUSTOM, b"count");
				let count = ctx.state().get::<i64>(&state)?.unwrap_or(0) + 1;
				ctx.state().set::<i64>(&state, &count)?;

				let (number, is_new) =
					ctx.get_or_create_row_number(group, &EncodedKey::new(Vec::new()))?;
				rows.push(ProbeRow {
					group: group_value as i64,
					is_new: i64::from(is_new),
				});
				numbers.push(number);
			}
		}
		if rows.is_empty() {
			return Ok(());
		}
		ctx.emit_insert(&rows, &numbers)
	}
}

fn ttl() -> Duration {
	Duration::from_seconds(TTL_SECS).expect("60s is representable")
}

fn at(ms: u64) -> DateTime {
	DateTime::from_timestamp_millis(ms).expect("representable")
}

fn arrival(group: i32, value: i64, ms: u64) -> reifydb_core::interface::change::Change {
	generator::insert(vec![generator::row(RowNumber(value as u64), group, value, at(ms))])
}

fn is_new_of(change: &reifydb_core::interface::change::Change) -> Vec<i64> {
	change.diffs
		.iter()
		.filter_map(|diff| diff.post())
		.flat_map(|columns| match columns.iter().find(|column| column.name().text() == "is_new") {
			Some(column) => (0..column.data().len())
				.map(|i| match column.data().get_value(i) {
					reifydb_value::value::Value::Int8(v) => v,
					other => panic!("is_new must be Int8, got {other:?}"),
				})
				.collect::<Vec<_>>(),
			None => Vec::new(),
		})
		.collect()
}

#[test]
fn a_guest_whose_data_was_reclaimed_still_finds_the_row_number_it_published_under() {
	// With no sink row ttl declared the sweep runs the data phase but never the identity phase, so a
	// guest wakes with its accumulator gone and the mapping naming its published row still there.
	// `is_new` false is the only thing between it and a second row published over a live one.
	let mut harness = Harness::guest(KeyedCounter, NODE, OperatorCapability::STANDARD_WITH_RECLAIM, Some(ttl()))
		.with_activity_grid();

	let out = harness.apply(arrival(1, 1, ARRIVAL_MS)).expect("first arrival");
	assert_eq!(is_new_of(&out), vec![1], "the first sighting of a key mints its row number");

	let reclaimed = harness.reclaim(SWEEP_MS).expect("sweep");
	assert!(!reclaimed.data.is_empty(), "the sweep must have reached the group, or this asserts nothing");
	assert!(reclaimed.identity.is_empty(), "no sink row ttl was declared, so identity must not be swept");

	let out = harness.apply(arrival(1, 2, SWEEP_MS)).expect("second arrival");
	assert_eq!(
		is_new_of(&out),
		vec![0],
		"the mapping outlived the data, so the guest must find its row number rather than mint a second"
	);
}

#[test]
fn a_guest_that_declares_no_retention_is_never_swept() {
	// An operator that advertises Reclaim without sealing on anything of its own draws all its
	// retention from the view's `with { ttl: .. }`. Without one the node is not merely slower to
	// reclaim - it is skipped outright and counted perpetual while its state grows.
	let mut harness = Harness::guest(KeyedCounter, NODE, OperatorCapability::STANDARD_WITH_RECLAIM, None)
		.with_activity_grid();

	harness.apply(arrival(1, 1, ARRIVAL_MS)).expect("first arrival");

	assert!(
		harness.activity_grid().event_grid().is_none(),
		"with no scale the node grids as undeclared, which is the condition the driver skips on"
	);
	let reclaimed = harness.reclaim(SWEEP_MS).expect("sweep");
	assert!(reclaimed.data.is_empty(), "a node with no retention scale has no cutoff to sweep against");

	let out = harness.apply(arrival(1, 2, SWEEP_MS)).expect("second arrival");
	assert_eq!(is_new_of(&out), vec![0], "and its state is still there, which is the leak this names");
}
