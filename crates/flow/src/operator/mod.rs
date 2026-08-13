// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{catalog::flow::OperatorId, change::Change, flow::OperatorCapability},
	metrics::heap::OperatorSample,
	value::column::columns::Columns,
};
use reifydb_value::{
	Result,
	value::{datetime::DateTime, duration::Duration},
};

use crate::{operator::host::HostContext, timer::Timer};

#[cfg(all(reifydb_target = "host", not(reifydb_dst)))]
pub fn scale_from_millis(span: Option<u64>) -> Option<Duration> {
	span.filter(|millis| *millis > 0)
		.and_then(|millis| i64::try_from(millis).ok())
		.and_then(|millis| Duration::from_milliseconds(millis).ok())
}

pub mod aggregation;
pub mod append;
pub mod apply;
pub mod distinct;
pub mod drops;
pub mod extend;
pub mod filter;
pub mod gate;
pub mod guard;
pub mod host;
pub mod join;
pub mod map;
pub mod metrics;
pub mod provider;
pub mod scan;
pub mod sink;
pub mod sort;
pub mod stateful;
pub mod take;
pub mod window;

pub trait HostOperator: Send {
	fn id(&self) -> OperatorId;

	fn capabilities(&self) -> &[OperatorCapability];

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change>;

	fn on_timer(&mut self, _host: &mut dyn HostContext, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}

	fn seal_span(&self) -> Option<Duration> {
		None
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn output_schema(&self) -> Option<Columns> {
		None
	}
}

pub type BoxedHostOperator = Box<dyn HostOperator>;

pub fn max_input_time(change: &Change) -> Option<DateTime> {
	change.diffs
		.iter()
		.filter_map(|diff| diff.post().or_else(|| diff.pre()))
		.flat_map(|columns| columns.time().iter().copied())
		.max()
}

pub(crate) fn stamp_output_time(change: &mut Change, inherited: Option<DateTime>) {
	let Some(inherited) = inherited else {
		return;
	};
	for diff in change.diffs.iter_mut() {
		for columns in diff.columns_mut() {
			let stamped: Vec<DateTime> = columns.time().iter().map(|own| (*own).min(inherited)).collect();
			columns.system.set_time(stamped);
		}
	}
}

#[cfg(test)]
mod substrate_stamping_tests {
	use reifydb_core::{
		common::CommitVersion,
		interface::{
			catalog::flow::OperatorId,
			change::{Diff, Diffs},
		},
		value::column::{ColumnWithName, buffer::ColumnBuffer, columns::Columns},
	};
	use reifydb_value::{
		factory::time::at_millis,
		fragment::Fragment,
		value::{row_number::RowNumber, system_columns::SystemColumns},
	};

	use super::*;

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
				vec![at_millis(0); n],
				vec![at_millis(0); n],
				times.to_vec(),
			),
		)
	}

	fn untimed_columns(n: usize) -> Columns {
		Columns::with_system(
			vec![ColumnWithName::new(
				Fragment::internal("v"),
				ColumnBuffer::int4((0..n as i32).collect::<Vec<_>>()),
			)],
			SystemColumns::new(
				(1..=n as u64).map(RowNumber).collect(),
				Vec::new(),
				vec![at_millis(0); n],
				vec![at_millis(0); n],
				Vec::new(),
			),
		)
	}

	fn change(diffs: Diffs) -> Change {
		Change::from_flow(OperatorId(1), CommitVersion(1), diffs, at_millis(0))
	}

	#[test]
	fn the_substrate_stamps_output_with_the_max_input_time() {
		// The substrate derives the stamp from the input, never from the operator, which is what
		// lets a guest operator stay oblivious to #time without breaking the clock.
		let mut diffs = Diffs::new();
		diffs.push(Diff::insert(columns(&[at_millis(1_000), at_millis(9_000), at_millis(5_000)])));

		assert_eq!(max_input_time(&change(diffs)), Some(at_millis(9_000)));
	}

	#[test]
	fn an_operator_cannot_influence_its_own_output_time() {
		// Stamping above the inputs would advance the flow watermark and seal another operator's
		// state early, so the clamp is one-directional: it pulls a row down to the inherited instant
		// and leaves a genuinely earlier row where it is. Clamping both directions would drag a
		// backfilled row forward into a window it does not belong to.
		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[at_millis(999_999), at_millis(1_000)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at_millis(4_000)));

		assert_eq!(
			out.diffs[0].post().unwrap().time().to_vec(),
			vec![at_millis(4_000), at_millis(1_000)],
			"a row above the inherited instant is pulled down; one below keeps its own"
		);
	}

	#[test]
	fn both_sides_of_an_update_are_stamped() {
		// A pre image left above the inherited instant would advance the watermark through the
		// pre side alone and make a retention decision see two times for one row.
		let mut produced = Diffs::new();
		produced.push(Diff::update(columns(&[at_millis(9_000)]), columns(&[at_millis(10_000)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at_millis(7_000)));

		assert_eq!(out.diffs[0].pre().unwrap().time().to_vec(), vec![at_millis(7_000)]);
		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at_millis(7_000)]);
	}

	#[test]
	fn a_fan_out_operator_has_every_emitted_row_stamped() {
		// Every row sits above the inherited instant, so a clamp that visited only the first
		// would still pass if the rest were left alone.
		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[
			at_millis(9_000),
			at_millis(10_000),
			at_millis(11_000),
			at_millis(12_000),
			at_millis(13_000),
		])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at_millis(8_000)));

		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at_millis(8_000); 5]);
	}

	#[test]
	fn an_empty_input_leaves_the_output_untouched() {
		// With nothing to inherit, stamping anyway would write an epoch time that reads as 1970
		// and is evicted on sight.
		let empty = change(Diffs::new());
		assert_eq!(max_input_time(&empty), None);

		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[at_millis(3_000)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, None);

		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at_millis(3_000)]);
	}

	#[test]
	fn an_operator_stamping_above_its_inputs_is_still_overwritten() {
		// No relaxation of the clamp may reach the above-inputs direction, and the comparison is
		// strict: one nanosecond over is enough.
		let inherited = at_millis(5_000);
		let one_nano_above = DateTime::from_nanos(inherited.to_nanos() + 1);

		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[one_nano_above])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(inherited));

		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![inherited]);
	}

	#[test]
	fn an_operator_stamping_at_or_below_its_inputs_keeps_its_stamp() {
		// A window stamps the bucket START, at or below every event it consumed; overwriting it
		// costs replay stability. Equality must survive - a bucket start can coincide with its
		// only event.
		let inherited = at_millis(5_000);

		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[at_millis(1_000), inherited])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(inherited));

		assert_eq!(
			out.diffs[0].post().unwrap().time().to_vec(),
			vec![at_millis(1_000), inherited],
			"below survives, and equal counts as below"
		);
	}

	#[test]
	fn a_row_stamped_at_the_epoch_keeps_its_own_instant() {
		// The epoch is an ordinary coordinate here, not a marker for "unstamped". Substituting the
		// inherited instant for it would silently re-date every row a source legitimately placed in
		// 1970, and the two cases are already distinguishable without inspecting the value.
		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[DateTime::default()])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at_millis(6_000)));

		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![DateTime::default()]);
	}

	#[test]
	fn a_time_less_batch_stays_time_less_through_stamping() {
		// A source with no time domain emits rows carrying no #time, and stamping must not invent
		// one for them. Filling the sidecar here would give a time-less object a clock it never
		// declared and let its rows start moving watermarks.
		let mut produced = Diffs::new();
		produced.push(Diff::insert(untimed_columns(3)));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at_millis(6_000)));

		assert!(out.diffs[0].post().unwrap().time().is_empty(), "#time must stay absent");
	}

	#[test]
	fn a_window_row_carries_its_window_start_through_the_apply_wrapper() {
		// Both paths a guest window emits from inherit an instant at or after the bucket start,
		// so one rule carries the start through without a special case - which is what lets a
		// consumer read #time instead of a window_start data column.
		let window_start = at_millis(60_000);
		let newest_event_in_bucket = at_millis(119_000);
		let seal_fires_at = at_millis(120_001);

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
	fn the_max_spans_every_diff_in_the_batch() {
		// An operator fed several diffs must inherit the latest instant anywhere in the batch,
		// not the first diff's.
		let mut diffs = Diffs::new();
		diffs.push(Diff::insert(columns(&[at_millis(1_000)])));
		diffs.push(Diff::insert(columns(&[at_millis(12_000)])));
		diffs.push(Diff::insert(columns(&[at_millis(3_000)])));

		assert_eq!(max_input_time(&change(diffs)), Some(at_millis(12_000)));
	}
}
