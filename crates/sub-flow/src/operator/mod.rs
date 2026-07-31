// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Deref, sync::Arc};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	interface::catalog::flow::FlowNodeId, key::operator_state::GroupSet, metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_flow::{
	operator::Reclaimable,
	timer::Timer,
	transaction::FlowTransaction,
	window::{ledger::read_sealed_through, policy::SealPolicy},
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};

pub(crate) fn scale_from_millis(span: Option<u64>) -> Option<Duration> {
	span.filter(|millis| *millis > 0)
		.and_then(|millis| i64::try_from(millis).ok())
		.and_then(|millis| Duration::from_milliseconds(millis).ok())
}

pub(crate) fn sealed_or_idle(
	txn: &mut FlowTransaction,
	node: FlowNodeId,
	watermark: DateTime,
	scale: Option<Duration>,
) -> Result<Reclaimable> {
	let Some(scale) = scale else {
		return Ok(Reclaimable::default());
	};
	if let Some(sealed) = read_sealed_through(txn, node)? {
		return Ok(SealPolicy::of(scale).sealed_anchor(sealed.at()).map(Reclaimable::data).unwrap_or_default());
	}
	Ok(Reclaimable::data(watermark.saturating_sub(scale)))
}

pub mod aggregation;
pub mod append;
pub mod apply;
#[cfg(reifydb_target = "native")]
pub mod context;
pub mod distinct;
pub mod drops;
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

use aggregation::operator::AggregateOperator;
use append::AppendOperator;
use apply::ApplyOperator;
use distinct::operator::DistinctOperator;
use extend::ExtendOperator;
use filter::FilterOperator;
use gate::GateOperator;
use guard::enforce_apply_capabilities;
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
use window::operator::WindowOperator;

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
	pub fn retention_scale(&self) -> Option<Duration> {
		match self {
			Operators::Filter(op) => op.retention_scale(),
			Operators::Gate(op) => op.retention_scale(),
			Operators::Map(op) => op.retention_scale(),
			Operators::Extend(op) => op.retention_scale(),
			Operators::Join(op) => op.retention_scale(),
			Operators::Sort(op) => op.retention_scale(),
			Operators::Take(op) => op.retention_scale(),
			Operators::Distinct(op) => op.retention_scale(),
			Operators::Append(op) => op.retention_scale(),
			Operators::Apply(op) => op.retention_scale(),
			Operators::SinkTableView(op) => op.retention_scale(),
			Operators::SinkRingBufferView(op) => op.retention_scale(),
			Operators::SinkSeriesView(op) => op.retention_scale(),
			Operators::Window(op) => op.retention_scale(),
			Operators::Aggregate(op) => op.retention_scale(),
			Operators::SourceTable(op) => op.retention_scale(),
			Operators::SourceView(op) => op.retention_scale(),
			Operators::SourceRingBuffer(op) => op.retention_scale(),
			Operators::SourceSeries(op) => op.retention_scale(),
		}
	}

	pub fn reclaimable_through(&self, txn: &mut FlowTransaction, watermark: DateTime) -> Result<Reclaimable> {
		match self {
			Operators::Filter(op) => op.reclaimable_through(txn, watermark),
			Operators::Gate(op) => op.reclaimable_through(txn, watermark),
			Operators::Map(op) => op.reclaimable_through(txn, watermark),
			Operators::Extend(op) => op.reclaimable_through(txn, watermark),
			Operators::Join(op) => op.reclaimable_through(txn, watermark),
			Operators::Sort(op) => op.reclaimable_through(txn, watermark),
			Operators::Take(op) => op.reclaimable_through(txn, watermark),
			Operators::Distinct(op) => op.reclaimable_through(txn, watermark),
			Operators::Append(op) => op.reclaimable_through(txn, watermark),
			Operators::Apply(op) => op.reclaimable_through(txn, watermark),
			Operators::SinkTableView(op) => op.reclaimable_through(txn, watermark),
			Operators::SinkRingBufferView(op) => op.reclaimable_through(txn, watermark),
			Operators::SinkSeriesView(op) => op.reclaimable_through(txn, watermark),
			Operators::Window(op) => op.reclaimable_through(txn, watermark),
			Operators::Aggregate(op) => op.reclaimable_through(txn, watermark),
			Operators::SourceTable(op) => op.reclaimable_through(txn, watermark),
			Operators::SourceView(op) => op.reclaimable_through(txn, watermark),
			Operators::SourceRingBuffer(op) => op.reclaimable_through(txn, watermark),
			Operators::SourceSeries(op) => op.reclaimable_through(txn, watermark),
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

	pub fn on_timer(&self, txn: &mut FlowTransaction, timer: Timer) -> Result<Option<Change>> {
		match self {
			Operators::Filter(op) => op.on_timer(txn, timer),
			Operators::Gate(op) => op.on_timer(txn, timer),
			Operators::Map(op) => op.on_timer(txn, timer),
			Operators::Extend(op) => op.on_timer(txn, timer),
			Operators::Join(op) => op.on_timer(txn, timer),
			Operators::Sort(op) => op.on_timer(txn, timer),
			Operators::Take(op) => op.on_timer(txn, timer),
			Operators::Distinct(op) => op.on_timer(txn, timer),
			Operators::Append(op) => op.on_timer(txn, timer),
			Operators::Apply(op) => {
				let at = timer.at;
				let mut out = op.on_timer(txn, timer)?;
				if let Some(change) = out.as_mut() {
					stamp_output_time(change, Some(at));
				}
				Ok(out)
			}
			Operators::SinkTableView(op) => op.on_timer(txn, timer),
			Operators::SinkRingBufferView(op) => op.on_timer(txn, timer),
			Operators::SinkSeriesView(op) => op.on_timer(txn, timer),
			Operators::Window(op) => op.on_timer(txn, timer),
			Operators::Aggregate(op) => op.on_timer(txn, timer),
			Operators::SourceTable(op) => op.on_timer(txn, timer),
			Operators::SourceView(op) => op.on_timer(txn, timer),
			Operators::SourceRingBuffer(op) => op.on_timer(txn, timer),
			Operators::SourceSeries(op) => op.on_timer(txn, timer),
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
			let stamped: Vec<DateTime> = columns
				.time()
				.iter()
				.map(|own| {
					if own.is_epoch() || *own > inherited {
						inherited
					} else {
						*own
					}
				})
				.collect();
			columns.system.set_time(stamped);
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
	// A custom operator's output carries the MAX #time of what it consumed. The operator
	// itself is never consulted - the substrate computes this from the input and overwrites whatever
	// the operator produced. That is what lets chaindex's operator population stay oblivious to
	// #time and still not break the clock.
	fn the_substrate_stamps_output_with_the_max_input_time() {
		let mut diffs = Diffs::new();
		diffs.push(Diff::insert(columns(&[at(1_000), at(9_000), at(5_000)])));

		assert_eq!(max_input_time(&change(diffs)), Some(at(9_000)));
	}

	#[test]
	// THE protection. An operator able to stamp ABOVE its inputs would advance the flow
	// watermark and seal another node's state early, invisibly. That half is absolute and is what
	// this test pins: at(999_999) is replaced. The second row is at(0), the epoch, which is what an
	// unstamped row carries - also replaced, because the substrate must not leave a row reading as
	// 1970 for retention to evict at once. Only a row the operator deliberately stamped at or below
	// its inputs survives, which is the one-sided relaxation windowing needs.
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
	// The stamp covers BOTH sides of an update. A pre image left above the inherited instant
	// would let an operator advance the watermark through the pre side alone, and would make a
	// downstream retention decision see two different times for one row.
	// Both sides sit ABOVE the inherited instant, which is the direction the clamp still enforces
	// absolutely; feeding values below it would prove nothing about the pre side being visited.
	fn both_sides_of_an_update_are_stamped() {
		let mut produced = Diffs::new();
		produced.push(Diff::update(columns(&[at(9_000)]), columns(&[at(10_000)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at(7_000)));

		assert_eq!(out.diffs[0].pre().unwrap().time().to_vec(), vec![at(7_000)]);
		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at(7_000)]);
	}

	#[test]
	// An operator that emits MORE rows than it consumed still has every row visited, so a
	// fan-out operator cannot leak a row stamped above its inputs into the flow. Every row is above
	// the inherited instant so the assertion holds for all five regardless of position - a clamp
	// that only visited the first row would still pass if the others were left below.
	fn a_fan_out_operator_has_every_emitted_row_stamped() {
		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[at(9_000), at(10_000), at(11_000), at(12_000), at(13_000)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at(8_000)));

		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at(8_000); 5]);
	}

	#[test]
	// With no input rows there is nothing to inherit, so the substrate must leave the output
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
	// The surviving half of the invariant, isolated. Stamping above inputs is what advances
	// the flow watermark and seals another node's state early, and no relaxation of the clamp may
	// ever reach it. One nanosecond above is enough - the comparison is strict.
	fn an_operator_stamping_above_its_inputs_is_still_overwritten() {
		let inherited = at(5_000);
		let one_nano_above = DateTime::from_nanos(inherited.to_nanos() + 1);

		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[one_nano_above])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(inherited));

		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![inherited]);
	}

	#[test]
	// The relaxed half, and the whole reason the clamp exists. A window stamps its output
	// with the bucket START, which is by construction at or below every event it consumed. Before
	// the clamp the apply wrapper overwrote it, so a guest window could not be replay-stable and
	// anything reading its #time saw the batch's max instead. Equality must survive too - a bucket
	// start can coincide exactly with its only event.
	fn an_operator_stamping_at_or_below_its_inputs_keeps_its_stamp() {
		let inherited = at(5_000);

		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[at(1_000), inherited])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(inherited));

		assert_eq!(
			out.diffs[0].post().unwrap().time().to_vec(),
			vec![at(1_000), inherited],
			"below survives, and equal counts as below"
		);
	}

	#[test]
	// An unstamped row is not a row stamped at 1970. At this layer #time has no none: a
	// freshly built Columns carries DateTime::default(), the epoch. A clamp that only compared
	// against the inherited instant would find epoch below it and KEEP it, and every operator that
	// never touches #time would emit rows that retention evicts on sight. The epoch branch is what
	// distinguishes "deliberately stamped early" from "never stamped".
	fn a_row_with_no_time_is_stamped_from_the_inherited_instant() {
		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[DateTime::default()])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at(6_000)));

		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at(6_000)]);
	}

	#[test]
	// The clamp works on BOTH paths a guest window emits from, with no special case. On the
	// apply path `inherited` is max_input_time, and a bucket start is at or below every event in the
	// bucket. On the timer path `inherited` is timer.at, and a Seal timer fires at bucket end plus
	// grace plus one, strictly after the start. So the same rule carries the bucket start through
	// both. This is what lets the chaindex trending-* operators read #time instead of a window_start
	// data column.
	fn a_window_row_carries_its_window_start_through_the_apply_wrapper() {
		let window_start = at(60_000);
		let newest_event_in_bucket = at(119_000);
		let seal_fires_at = at(120_001);

		let mut on_apply = change({
			let mut d = Diffs::new();
			d.push(Diff::insert(columns(&[window_start])));
			d
		});
		stamp_output_time(&mut on_apply, Some(newest_event_in_bucket));
		assert_eq!(on_apply.diffs[0].post().unwrap().time().to_vec(), vec![window_start]);

		let mut on_timer = change({
			let mut d = Diffs::new();
			d.push(Diff::insert(columns(&[window_start])));
			d
		});
		stamp_output_time(&mut on_timer, Some(seal_fires_at));
		assert_eq!(on_timer.diffs[0].post().unwrap().time().to_vec(), vec![window_start]);
	}

	#[test]
	// The max is taken across ALL diffs in the batch, not just the first. An operator fed a
	// batch of several diffs must inherit the latest instant anywhere in it.
	fn the_max_spans_every_diff_in_the_batch() {
		let mut diffs = Diffs::new();
		diffs.push(Diff::insert(columns(&[at(1_000)])));
		diffs.push(Diff::insert(columns(&[at(12_000)])));
		diffs.push(Diff::insert(columns(&[at(3_000)])));

		assert_eq!(max_input_time(&change(diffs)), Some(at(12_000)));
	}
}
