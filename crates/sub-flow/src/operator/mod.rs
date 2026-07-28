// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Deref, sync::Arc};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::catalog::flow::FlowNodeId, key::operator_state::GroupSet, metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_flow::transaction::{FlowTransaction, timer::Timer};
use reifydb_sdk::operator::Tick;
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};

pub mod append;
pub mod apply;
#[cfg(reifydb_target = "native")]
pub mod context;
pub mod distinct;
pub mod extend;
#[cfg(reifydb_target = "native")]
pub mod ffi;
pub mod filter;
pub mod gate;
pub mod guard;
pub mod join;
pub mod map;
pub mod metrics;
#[cfg(reifydb_target = "native")]
pub mod native;
pub mod scan;
pub mod sink;
pub mod sort;
pub mod stateful;
pub mod store;
pub mod take;
pub mod window;

use append::AppendOperator;
use apply::ApplyOperator;
use distinct::operator::DistinctOperator;
use extend::ExtendOperator;
use filter::FilterOperator;
use gate::GateOperator;
use guard::{enforce_apply_capabilities, enforce_tick_capability};
use join::operator::JoinOperator;
use map::MapOperator;
use reifydb_core::interface::change::Change;
use reifydb_flow::operator::{BoxedOperator, Operator};
use scan::{
	ringbuffer::SourceRingBufferOperator, series::SourceSeriesOperator, table::SourceTableOperator,
	view::SourceViewOperator,
};
use sink::{
	ringbuffer_view::SinkRingBufferViewOperator, series_view::SinkSeriesViewOperator, view::SinkTableViewOperator,
};
use sort::SortOperator;
use take::TakeOperator;
use window::{aggregate::AggregateOperator, operator::WindowOperator};

#[derive(Clone)]
pub struct OperatorCell(Arc<Operators>);

impl OperatorCell {
	#[allow(clippy::arc_with_non_send_sync)]
	pub fn new(operators: Operators) -> Self {
		Self(Arc::new(operators))
	}
}

impl Deref for OperatorCell {
	type Target = Operators;

	fn deref(&self) -> &Operators {
		&self.0
	}
}

// SAFETY: a flow and all of its operators are only ever accessed by a single thread at any one
// time. Flows that execute in parallel on the rayon commit pool own disjoint operator sets
// (operators are keyed by FlowNodeId and never shared between flows), so no Operators value is ever
// reachable from two threads simultaneously. The inner Arc is only cloned and dereferenced from the
// owning thread, so asserting Send and Sync over the !Sync Operators it holds is sound.
unsafe impl Send for OperatorCell {}
unsafe impl Sync for OperatorCell {}

pub enum Operators {
	SourceTable(SourceTableOperator),
	SourceView(SourceViewOperator),
	SourceRingBuffer(SourceRingBufferOperator),
	SourceSeries(SourceSeriesOperator),
	Filter(FilterOperator),
	Gate(GateOperator),
	Map(MapOperator),
	Extend(ExtendOperator),
	Join(JoinOperator),
	Sort(SortOperator),
	Take(TakeOperator),
	Distinct(DistinctOperator),
	Append(AppendOperator),
	Apply(ApplyOperator),
	SinkTableView(SinkTableViewOperator),
	SinkRingBufferView(SinkRingBufferViewOperator),
	SinkSeriesView(SinkSeriesViewOperator),
	Window(Box<WindowOperator>),
	Aggregate(AggregateOperator),
}

impl Operators {
	pub fn id(&self) -> FlowNodeId {
		match self {
			Operators::Filter(op) => op.id(),
			Operators::Gate(op) => op.id(),
			Operators::Map(op) => op.id(),
			Operators::Extend(op) => op.id(),
			Operators::Join(op) => op.id(),
			Operators::Sort(op) => op.id(),
			Operators::Take(op) => op.id(),
			Operators::Distinct(op) => op.id(),
			Operators::Append(op) => op.id(),
			Operators::Apply(op) => op.id(),
			Operators::SinkTableView(op) => op.id(),
			Operators::SinkRingBufferView(op) => op.id(),
			Operators::SinkSeriesView(op) => op.id(),
			Operators::Window(op) => op.id(),
			Operators::Aggregate(op) => op.id(),
			Operators::SourceTable(op) => op.id(),
			Operators::SourceView(op) => op.id(),
			Operators::SourceRingBuffer(op) => op.id(),
			Operators::SourceSeries(op) => op.id(),
		}
	}

	pub fn capabilities(&self) -> &[OperatorCapability] {
		match self {
			Operators::Filter(op) => op.capabilities(),
			Operators::Gate(op) => op.capabilities(),
			Operators::Map(op) => op.capabilities(),
			Operators::Extend(op) => op.capabilities(),
			Operators::Join(op) => op.capabilities(),
			Operators::Sort(op) => op.capabilities(),
			Operators::Take(op) => op.capabilities(),
			Operators::Distinct(op) => op.capabilities(),
			Operators::Append(op) => op.capabilities(),
			Operators::Apply(op) => op.capabilities(),
			Operators::SinkTableView(op) => op.capabilities(),
			Operators::SinkRingBufferView(op) => op.capabilities(),
			Operators::SinkSeriesView(op) => op.capabilities(),
			Operators::Window(op) => op.capabilities(),
			Operators::Aggregate(op) => op.capabilities(),
			Operators::SourceTable(op) => op.capabilities(),
			Operators::SourceView(op) => op.capabilities(),
			Operators::SourceRingBuffer(op) => op.capabilities(),
			Operators::SourceSeries(op) => op.capabilities(),
		}
	}

	pub fn ticks(&self) -> Option<Duration> {
		match self {
			Operators::Filter(op) => op.ticks(),
			Operators::Gate(op) => op.ticks(),
			Operators::Map(op) => op.ticks(),
			Operators::Extend(op) => op.ticks(),
			Operators::Join(op) => op.ticks(),
			Operators::Sort(op) => op.ticks(),
			Operators::Take(op) => op.ticks(),
			Operators::Distinct(op) => op.ticks(),
			Operators::Append(op) => op.ticks(),
			Operators::Apply(op) => op.ticks(),
			Operators::SinkTableView(op) => op.ticks(),
			Operators::SinkRingBufferView(op) => op.ticks(),
			Operators::SinkSeriesView(op) => op.ticks(),
			Operators::Window(op) => op.ticks(),
			Operators::Aggregate(op) => op.ticks(),
			Operators::SourceTable(op) => op.ticks(),
			Operators::SourceView(op) => op.ticks(),
			Operators::SourceRingBuffer(op) => op.ticks(),
			Operators::SourceSeries(op) => op.ticks(),
		}
	}

	pub fn seal_after_ms(&self) -> Option<u64> {
		match self {
			Operators::Filter(op) => op.seal_after_ms(),
			Operators::Gate(op) => op.seal_after_ms(),
			Operators::Map(op) => op.seal_after_ms(),
			Operators::Extend(op) => op.seal_after_ms(),
			Operators::Join(op) => op.seal_after_ms(),
			Operators::Sort(op) => op.seal_after_ms(),
			Operators::Take(op) => op.seal_after_ms(),
			Operators::Distinct(op) => op.seal_after_ms(),
			Operators::Append(op) => op.seal_after_ms(),
			Operators::Apply(op) => op.seal_after_ms(),
			Operators::SinkTableView(op) => op.seal_after_ms(),
			Operators::SinkRingBufferView(op) => op.seal_after_ms(),
			Operators::SinkSeriesView(op) => op.seal_after_ms(),
			Operators::Window(op) => op.seal_after_ms(),
			Operators::Aggregate(op) => op.seal_after_ms(),
			Operators::SourceTable(op) => op.seal_after_ms(),
			Operators::SourceView(op) => op.seal_after_ms(),
			Operators::SourceRingBuffer(op) => op.seal_after_ms(),
			Operators::SourceSeries(op) => op.seal_after_ms(),
		}
	}

	pub fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		enforce_apply_capabilities(self.id(), self.capabilities(), &change);
		match self {
			Operators::Filter(op) => op.apply(txn, change),
			Operators::Gate(op) => op.apply(txn, change),
			Operators::Map(op) => op.apply(txn, change),
			Operators::Extend(op) => op.apply(txn, change),
			Operators::Join(op) => op.apply(txn, change),
			Operators::Sort(op) => op.apply(txn, change),
			Operators::Take(op) => op.apply(txn, change),
			Operators::Distinct(op) => op.apply(txn, change),
			Operators::Append(op) => op.apply(txn, change),
			Operators::Apply(op) => {
				let inherited = max_input_time(&change);
				let mut out = op.apply(txn, change)?;
				stamp_output_time(&mut out, inherited);
				Ok(out)
			}
			Operators::SinkTableView(op) => op.apply(txn, change),
			Operators::SinkRingBufferView(op) => op.apply(txn, change),
			Operators::SinkSeriesView(op) => op.apply(txn, change),
			Operators::Window(op) => op.apply(txn, change),
			Operators::Aggregate(op) => op.apply(txn, change),
			Operators::SourceTable(op) => op.apply(txn, change),
			Operators::SourceView(op) => op.apply(txn, change),
			Operators::SourceRingBuffer(op) => op.apply(txn, change),
			Operators::SourceSeries(op) => op.apply(txn, change),
		}
	}

	pub fn on_timer(&self, _txn: &mut FlowTransaction, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}

	pub fn tick(&self, txn: &mut FlowTransaction, tick: Tick) -> Result<Option<Change>> {
		match self {
			Operators::Window(op) => {
				enforce_tick_capability(op.id(), op.capabilities());
				op.tick(txn, tick)
			}
			Operators::Apply(op) => {
				enforce_tick_capability(op.id(), op.capabilities());
				op.tick(txn, tick)
			}
			Operators::Distinct(op) => {
				enforce_tick_capability(op.id(), op.capabilities());
				op.tick(txn, tick)
			}
			Operators::Join(op) => {
				enforce_tick_capability(op.id(), op.capabilities());
				op.tick(txn, tick)
			}
			Operators::Append(op) => {
				enforce_tick_capability(op.id(), op.capabilities());
				op.tick(txn, tick)
			}
			Operators::SinkRingBufferView(op) => {
				enforce_tick_capability(op.id(), op.capabilities());
				op.tick(txn, tick)
			}
			_ => Ok(None),
		}
	}

	pub fn invalidate_groups(&self, groups: &GroupSet) {
		if groups.is_empty() || !self.capabilities().contains(&OperatorCapability::Reclaim) {
			return;
		}
		match self {
			Operators::Window(op) => op.invalidate_groups(groups),
			Operators::Aggregate(op) => op.invalidate_groups(groups),
			Operators::Join(op) => op.invalidate_groups(groups),
			Operators::Distinct(op) => op.invalidate_groups(groups),
			Operators::Apply(op) => op.invalidate_groups(groups),
			Operators::Gate(op) => op.invalidate_groups(groups),
			Operators::Append(op) => op.invalidate_groups(groups),
			_ => {}
		}
	}

	pub fn sample(&self) -> Option<OperatorSample> {
		match self {
			Operators::Window(op) => op.sample(),
			Operators::Aggregate(op) => op.sample(),
			Operators::Join(op) => op.sample(),
			Operators::Distinct(op) => op.sample(),
			Operators::Apply(op) => op.sample(),
			_ => None,
		}
	}

	pub fn output_schema(&self) -> Option<Columns> {
		match self {
			Operators::SourceTable(op) => Some(op.output_schema()),
			Operators::SourceView(op) => Some(op.output_schema()),
			Operators::SourceRingBuffer(op) => Some(op.output_schema()),
			Operators::SourceSeries(_) => Some(Columns::empty()),
			Operators::Filter(op) => op.output_schema(),
			Operators::Gate(op) => op.output_schema(),
			Operators::Map(op) => op.output_schema(),
			Operators::Extend(op) => op.output_schema(),
			Operators::Sort(op) => op.output_schema(),
			Operators::Take(op) => op.output_schema(),
			Operators::Distinct(op) => op.output_schema(),
			Operators::Append(op) => op.output_schema(),
			Operators::Window(op) => op.core.parent.output_schema(),
			Operators::Aggregate(op) => op.output_schema(),
			Operators::Apply(op) => op.output_schema(),
			Operators::Join(_) => None,
			Operators::SinkTableView(_) => None,
			Operators::SinkRingBufferView(_) => None,
			Operators::SinkSeriesView(_) => None,
		}
	}
}

pub(crate) fn max_input_time(change: &Change) -> Option<DateTime> {
	change.diffs
		.iter()
		.filter_map(|diff| diff.post().or_else(|| diff.pre()))
		.flat_map(|columns| columns.time().iter().copied())
		.max()
}

fn stamp_output_time(change: &mut Change, inherited: Option<DateTime>) {
	let Some(inherited) = inherited else {
		return;
	};
	for diff in change.diffs.iter_mut() {
		for columns in diff.columns_mut() {
			let rows = columns.row_count();
			columns.system.set_time(vec![inherited; rows]);
		}
	}
}

#[cfg(test)]
mod substrate_stamping_tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::{
			catalog::flow::FlowNodeId,
			change::{Diff, Diffs},
		},
		value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
	};
	use reifydb_value::{
		fragment::Fragment,
		value::{row_number::RowNumber, system_columns::SystemColumns},
	};

	use super::*;

	fn at(millis: i64) -> DateTime {
		DateTime::from_timestamp(millis).unwrap()
	}

	fn columns(times: &[DateTime]) -> Columns {
		let n = times.len();
		Columns::with_system(
			vec![ColumnWithName::new(
				Fragment::internal("v"),
				ColumnBuffer::int4((0..n as i32).collect::<Vec<_>>()),
			)],
			SystemColumns::new(
				(1..=n as u64).map(RowNumber).collect(),
				Vec::new(),
				vec![at(0); n],
				vec![at(0); n],
				times.to_vec(),
			),
		)
	}

	fn change(diffs: Diffs) -> Change {
		Change::from_flow(FlowNodeId(1), CommitVersion(1), diffs, at(0))
	}

	#[test]
	// Intent: a custom operator's output carries the MAX #time of what it consumed. The operator
	// itself is never consulted - the substrate computes this from the input and overwrites whatever
	// the operator produced. That is what lets chaindex's operator population stay oblivious to
	// #time and still not break the clock.
	// Mutation: take min, or the first input's time, and this returns the wrong instant.
	fn the_substrate_stamps_output_with_the_max_input_time() {
		let mut diffs = Diffs::new();
		diffs.push(Diff::insert(columns(&[at(1_000), at(9_000), at(5_000)])));

		assert_eq!(max_input_time(&change(diffs)), Some(at(9_000)));
	}

	#[test]
	// Intent: THE protection. An operator that emits whatever stamp it likes - here a wildly wrong
	// one - has that stamp replaced by the substrate. An operator able to stamp above its inputs
	// would advance the flow watermark and seal state early; below them it would create permanent
	// lateness. Both are invisible at runtime, which is why this is enforced rather than trusted.
	// Mutation: skip stamp_output_time and the operator's own value survives.
	fn an_operator_cannot_influence_its_own_output_time() {
		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[at(999_999), at(0)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at(4_000)));

		let stamped = out.diffs[0].post().unwrap();
		assert_eq!(
			stamped.time().to_vec(),
			vec![at(4_000), at(4_000)],
			"every output row takes the substrate's stamp, not the operator's"
		);
	}

	#[test]
	// Intent: the stamp covers BOTH sides of an update. A pre image left at the operator's own
	// stamp would make a downstream retention decision see two different times for one row.
	fn both_sides_of_an_update_are_stamped() {
		let mut produced = Diffs::new();
		produced.push(Diff::update(columns(&[at(1)]), columns(&[at(2)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at(7_000)));

		assert_eq!(out.diffs[0].pre().unwrap().time().to_vec(), vec![at(7_000)]);
		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at(7_000)]);
	}

	#[test]
	// Intent: an operator that emits MORE rows than it consumed still has every row stamped, so a
	// fan-out operator cannot leak an unstamped row into the flow.
	fn a_fan_out_operator_has_every_emitted_row_stamped() {
		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[at(1), at(2), at(3), at(4), at(5)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at(8_000)));

		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at(8_000); 5]);
	}

	#[test]
	// Intent: with no input rows there is nothing to inherit, so the substrate must leave the output
	// alone rather than stamping an epoch time that would read as 1970 and be evicted at once.
	fn an_empty_input_leaves_the_output_untouched() {
		let empty = change(Diffs::new());
		assert_eq!(max_input_time(&empty), None);

		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[at(3_000)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, None);

		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at(3_000)]);
	}

	#[test]
	// Intent: the max is taken across ALL diffs in the batch, not just the first. An operator fed a
	// batch of several diffs must inherit the latest instant anywhere in it.
	fn the_max_spans_every_diff_in_the_batch() {
		let mut diffs = Diffs::new();
		diffs.push(Diff::insert(columns(&[at(1_000)])));
		diffs.push(Diff::insert(columns(&[at(12_000)])));
		diffs.push(Diff::insert(columns(&[at(3_000)])));

		assert_eq!(max_input_time(&change(diffs)), Some(at(12_000)));
	}
}
