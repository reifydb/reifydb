// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_arith::aggregate::max;
use arrow_array::{Array, ArrayRef, RecordBatch, TimestampNanosecondArray, types::TimestampNanosecondType};
#[cfg(feature = "runtime")]
use arrow_schema::SchemaRef;
#[cfg(feature = "runtime")]
use reifydb_core::{
	common::CommitVersion,
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::OperatorSample,
};
use reifydb_core::{interface::change::Change, internal_err};
#[cfg(feature = "runtime")]
use reifydb_value::value::{container::temporal_array::datetimes, duration::Duration};
use reifydb_value::{
	Result,
	value::{
		column_view::ViewData,
		container::temporal_array::{DATETIME_TIMEZONE, datetime_to_native},
		datetime::DateTime,
		system_columns::{SystemColumn, column_view, with_system_column},
	},
};

#[cfg(feature = "runtime")]
use crate::{operator::host::HostContext, timer::Timer};

#[cfg(feature = "runtime")]
pub mod aggregation;
#[cfg(feature = "runtime")]
pub mod append;
#[cfg(feature = "runtime")]
pub mod apply;
#[cfg(feature = "runtime")]
pub mod distinct;
#[cfg(feature = "runtime")]
pub mod drops;
#[cfg(feature = "runtime")]
pub mod extend;
#[cfg(feature = "runtime")]
pub mod filter;
#[cfg(feature = "runtime")]
pub mod gate;
#[cfg(feature = "runtime")]
pub mod guard;
#[cfg(feature = "runtime")]
pub mod host;
#[cfg(feature = "runtime")]
pub mod join;
#[cfg(feature = "runtime")]
pub mod lookup;
#[cfg(feature = "runtime")]
pub mod map;
#[cfg(feature = "runtime")]
pub mod metrics;
#[cfg(feature = "runtime")]
pub mod provider;
#[cfg(feature = "runtime")]
pub mod scan;
#[cfg(feature = "runtime")]
pub mod sink;
#[cfg(feature = "runtime")]
pub mod sort;
pub mod state;
pub mod state_access;
#[cfg(feature = "runtime")]
pub mod take;
#[cfg(feature = "runtime")]
pub mod window;

#[cfg(feature = "runtime")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InputOrder {
	Declared,
	Reversed,
}

#[cfg(feature = "runtime")]
impl InputOrder {
	pub fn rank(self, position: usize, arity: usize) -> usize {
		match self {
			InputOrder::Declared => position,
			InputOrder::Reversed => arity.saturating_sub(position + 1),
		}
	}
}

#[cfg(feature = "runtime")]
pub trait HostOperator: Send {
	fn id(&self) -> OperatorId;

	fn capabilities(&self) -> &[OperatorCapability];

	fn input_order(&self) -> InputOrder {
		InputOrder::Declared
	}

	fn apply(&mut self, host: &mut dyn HostContext, change: Change) -> Result<Change>;

	fn on_timer(&mut self, _host: &mut dyn HostContext, _timer: Timer) -> Result<Option<Change>> {
		Ok(None)
	}

	fn seal_span(&self) -> Option<Duration> {
		None
	}

	fn retention(&self) -> Option<Duration> {
		None
	}

	fn sample(&self) -> Option<OperatorSample> {
		None
	}

	fn output_schema(&self) -> Option<SchemaRef> {
		None
	}

	fn oldest_read_version(&self, _host: &mut dyn HostContext) -> Result<Option<CommitVersion>> {
		Ok(None)
	}
}

#[cfg(feature = "runtime")]
pub type BoxedHostOperator = Box<dyn HostOperator>;

pub fn max_input_time(change: &Change) -> Result<Option<DateTime>> {
	let mut latest = None;
	for columns in change.diffs.iter().filter_map(|diff| diff.post().or_else(|| diff.pre())) {
		latest = latest.max(time_array(columns)?.and_then(max).map(DateTime::from_nanos));
	}
	Ok(latest)
}

#[cfg_attr(not(feature = "runtime"), allow(dead_code))]
pub(crate) fn stamp_output_time(change: &mut Change, inherited: Option<DateTime>) -> Result<()> {
	let Some(inherited) = inherited else {
		return Ok(());
	};
	let inherited = datetime_to_native(inherited);
	for diff in change.diffs.iter_mut() {
		for columns in diff.batches_mut() {
			let Some(times) = time_array(columns)?.filter(|times| !times.is_empty()) else {
				continue;
			};
			let stamped = times
				.unary::<_, TimestampNanosecondType>(|own| own.min(inherited))
				.with_timezone(DATETIME_TIMEZONE);
			*columns = with_system_column(columns.clone(), SystemColumn::Time, Arc::new(stamped))?;
		}
	}
	Ok(())
}

fn time_array(columns: &RecordBatch) -> Result<Option<&TimestampNanosecondArray>> {
	let Some(view) = column_view(columns, SystemColumn::Time.name())? else {
		return Ok(None);
	};
	match &view.data {
		ViewData::DateTime(array) => Ok(Some(*array)),
		_ => internal_err!("system column #time holds {}", view.base_type()),
	}
}

#[cfg(feature = "runtime")]
pub(crate) fn row_times(columns: &RecordBatch) -> Result<Vec<Option<DateTime>>> {
	let Some(view) = column_view(columns, SystemColumn::Time.name())? else {
		return Ok(Vec::new());
	};
	match &view.data {
		ViewData::DateTime(array) => Ok(datetimes(array)
			.iter()
			.enumerate()
			.map(|(row, time)| array.is_valid(row).then_some(*time))
			.collect()),
		_ => internal_err!("system column #time holds {}", view.base_type()),
	}
}

#[cfg(feature = "runtime")]
pub(crate) fn time_at(columns: &RecordBatch, row_idx: usize) -> Result<Option<DateTime>> {
	Ok(row_times(&columns.slice(row_idx, 1))?.first().copied().flatten())
}

#[cfg_attr(not(feature = "runtime"), allow(dead_code))]
pub(crate) fn time_column(times: impl IntoIterator<Item = Option<DateTime>>) -> ArrayRef {
	Arc::new(
		TimestampNanosecondArray::from_iter(times.into_iter().map(|time| time.map(datetime_to_native)))
			.with_timezone(DATETIME_TIMEZONE),
	)
}

#[cfg(test)]
mod substrate_stamping_tests {
	use arrow_array::UInt64Array;
	use reifydb_core::{
		common::{ChangeVersion, CommitVersion},
		interface::{
			catalog::flow::OperatorId,
			change::{Diff, Diffs},
		},
		value::{batch::batch, column::factory::int4},
	};
	use reifydb_value::{
		factory::time::at_millis,
		value::{
			container::temporal_array::datetime_array,
			system_columns::{system_column, time},
		},
	};

	use super::*;

	fn columns(times: &[DateTime]) -> RecordBatch {
		let timed = untimed_columns(times.len());
		with_system_column(timed, SystemColumn::Time, Arc::new(datetime_array(times.iter().copied()))).unwrap()
	}

	fn untimed_columns(n: usize) -> RecordBatch {
		let user = batch(vec![int4("v", 0..n as i32)]).unwrap();
		let system: [(SystemColumn, ArrayRef); 3] = [
			(SystemColumn::RowNumbers, Arc::new(UInt64Array::from_iter_values(1..=n as u64))),
			(SystemColumn::CreatedAt, Arc::new(datetime_array(vec![at_millis(0); n]))),
			(SystemColumn::UpdatedAt, Arc::new(datetime_array(vec![at_millis(0); n]))),
		];
		system.into_iter()
			.fold(user, |columns, (column, array)| with_system_column(columns, column, array).unwrap())
	}

	fn change(diffs: Diffs) -> Change {
		Change::from_flow(OperatorId(1), ChangeVersion::from(CommitVersion(1)), diffs, at_millis(0))
	}

	#[test]
	fn the_substrate_stamps_output_with_the_max_input_time() {
		// The substrate derives the stamp from the input, never from the operator, which is what
		// lets a guest operator stay oblivious to #time without breaking the clock.
		let mut diffs = Diffs::new();
		diffs.push(Diff::insert(columns(&[at_millis(1_000), at_millis(9_000), at_millis(5_000)])));

		assert_eq!(max_input_time(&change(diffs)).unwrap(), Some(at_millis(9_000)));
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

		stamp_output_time(&mut out, Some(at_millis(4_000))).unwrap();

		assert_eq!(
			time(out.diffs[0].post().unwrap()).unwrap().to_vec(),
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

		stamp_output_time(&mut out, Some(at_millis(7_000))).unwrap();

		assert_eq!(time(out.diffs[0].pre().unwrap()).unwrap().to_vec(), vec![at_millis(7_000)]);
		assert_eq!(time(out.diffs[0].post().unwrap()).unwrap().to_vec(), vec![at_millis(7_000)]);
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

		stamp_output_time(&mut out, Some(at_millis(8_000))).unwrap();

		assert_eq!(time(out.diffs[0].post().unwrap()).unwrap().to_vec(), vec![at_millis(8_000); 5]);
	}

	#[test]
	fn an_empty_input_leaves_the_output_untouched() {
		// With nothing to inherit, stamping anyway would write an epoch time that reads as 1970
		// and is evicted on sight.
		let empty = change(Diffs::new());
		assert_eq!(max_input_time(&empty).unwrap(), None);

		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[at_millis(3_000)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, None).unwrap();

		assert_eq!(time(out.diffs[0].post().unwrap()).unwrap().to_vec(), vec![at_millis(3_000)]);
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

		stamp_output_time(&mut out, Some(inherited)).unwrap();

		assert_eq!(time(out.diffs[0].post().unwrap()).unwrap().to_vec(), vec![inherited]);
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

		stamp_output_time(&mut out, Some(inherited)).unwrap();

		assert_eq!(
			time(out.diffs[0].post().unwrap()).unwrap().to_vec(),
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

		stamp_output_time(&mut out, Some(at_millis(6_000))).unwrap();

		assert_eq!(time(out.diffs[0].post().unwrap()).unwrap().to_vec(), vec![DateTime::default()]);
	}

	#[test]
	fn a_time_less_batch_stays_time_less_through_stamping() {
		// A source with no time domain emits rows carrying no #time, and stamping must not invent
		// one for them. Filling the sidecar here would give a time-less object a clock it never
		// declared and let its rows start moving watermarks.
		let mut produced = Diffs::new();
		produced.push(Diff::insert(untimed_columns(3)));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at_millis(6_000))).unwrap();

		assert!(
			system_column(out.diffs[0].post().unwrap(), SystemColumn::Time).is_none(),
			"#time must stay absent"
		);
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
		stamp_output_time(&mut on_apply, Some(newest_event_in_bucket)).unwrap();
		assert_eq!(time(on_apply.diffs[0].post().unwrap()).unwrap().to_vec(), vec![window_start]);

		let mut on_timer = change({
			let mut d = Diffs::new();
			d.push(Diff::insert(columns(&[window_start])));
			d
		});
		stamp_output_time(&mut on_timer, Some(seal_fires_at)).unwrap();
		assert_eq!(time(on_timer.diffs[0].post().unwrap()).unwrap().to_vec(), vec![window_start]);
	}

	#[test]
	fn the_max_spans_every_diff_in_the_batch() {
		// An operator fed several diffs must inherit the latest instant anywhere in the batch,
		// not the first diff's.
		let mut diffs = Diffs::new();
		diffs.push(Diff::insert(columns(&[at_millis(1_000)])));
		diffs.push(Diff::insert(columns(&[at_millis(12_000)])));
		diffs.push(Diff::insert(columns(&[at_millis(3_000)])));

		assert_eq!(max_input_time(&change(diffs)).unwrap(), Some(at_millis(12_000)));
	}
}
