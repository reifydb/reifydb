// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Deref, sync::Arc};

#[cfg(reifydb_target = "native")]
use reifydb_core::interface::catalog::flow::OperatorId;
use reifydb_flow::transaction::FlowTransaction;
#[cfg(reifydb_target = "native")]
use reifydb_flow::window::{ledger::read_sealed_through, policy::SealPolicy};
#[cfg(reifydb_target = "native")]
use reifydb_store_operator::FloorSpec;
#[cfg(reifydb_target = "native")]
use reifydb_value::value::duration::Duration;
use reifydb_value::{Result, value::datetime::DateTime};

#[cfg(reifydb_target = "native")]
pub(crate) fn scale_from_millis(span: Option<u64>) -> Option<Duration> {
	span.filter(|millis| *millis > 0)
		.and_then(|millis| i64::try_from(millis).ok())
		.and_then(|millis| Duration::from_milliseconds(millis).ok())
}

#[cfg(reifydb_target = "native")]
pub(crate) fn sealed_or_idle_floor(
	txn: &mut FlowTransaction,
	operator: OperatorId,
	watermark: DateTime,
	scale: Option<Duration>,
) -> Result<FloorSpec> {
	let Some(scale) = scale else {
		return Ok(FloorSpec::default());
	};
	if let Some(sealed) = read_sealed_through(txn, operator)? {
		return Ok(SealPolicy::of(scale)
			.sealed_anchor(sealed.at())
			.map(FloorSpec::data)
			.unwrap_or_default());
	}
	Ok(FloorSpec::data(watermark.saturating_sub(scale)))
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

use guard::enforce_apply_capabilities;
use reifydb_core::interface::change::Change;
use reifydb_flow::operator::Operator;

#[derive(Clone)]
pub struct OperatorCell(Arc<dyn Operator + Send>);

impl OperatorCell {
	#[allow(clippy::arc_with_non_send_sync)]
	pub fn new(operator: impl Operator + 'static) -> Self {
		Self(Arc::new(operator))
	}

	pub fn apply(&self, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
		enforce_apply_capabilities(self.id(), self.capabilities(), &change);
		self.0.apply(txn, change)
	}
}

impl Deref for OperatorCell {
	type Target = dyn Operator + Send;

	fn deref(&self) -> &Self::Target {
		&*self.0
	}
}

// SAFETY: operators are keyed by OperatorId and never shared between flows, so an OperatorCell value
// is reachable from exactly one thread at a time and the inner Arc is only cloned or dereferenced
// from that owning thread. No aliasing of the !Sync interior can occur.
unsafe impl Send for OperatorCell {}
unsafe impl Sync for OperatorCell {}

pub(crate) fn max_input_time(change: &Change) -> Option<DateTime> {
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
			catalog::flow::OperatorId,
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
		Change::from_flow(OperatorId(1), CommitVersion(1), diffs, at(0))
	}

	#[test]
	fn the_substrate_stamps_output_with_the_max_input_time() {
		// The substrate derives the stamp from the input, never from the operator, which is what
		// lets a guest operator stay oblivious to #time without breaking the clock.
		let mut diffs = Diffs::new();
		diffs.push(Diff::insert(columns(&[at(1_000), at(9_000), at(5_000)])));

		assert_eq!(max_input_time(&change(diffs)), Some(at(9_000)));
	}

	#[test]
	fn an_operator_cannot_influence_its_own_output_time() {
		// An operator that stamped above its inputs would advance the flow watermark and seal
		// another operator's state early; the epoch row is replaced too, or an unstamped row would
		// read as 1970 and retention would evict it at once.
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
	fn both_sides_of_an_update_are_stamped() {
		// A pre image left above the inherited instant would advance the watermark through the
		// pre side alone and make a retention decision see two times for one row.
		let mut produced = Diffs::new();
		produced.push(Diff::update(columns(&[at(9_000)]), columns(&[at(10_000)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at(7_000)));

		assert_eq!(out.diffs[0].pre().unwrap().time().to_vec(), vec![at(7_000)]);
		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at(7_000)]);
	}

	#[test]
	fn a_fan_out_operator_has_every_emitted_row_stamped() {
		// Every row sits above the inherited instant, so a clamp that visited only the first
		// would still pass if the rest were left alone.
		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[at(9_000), at(10_000), at(11_000), at(12_000), at(13_000)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at(8_000)));

		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at(8_000); 5]);
	}

	#[test]
	fn an_empty_input_leaves_the_output_untouched() {
		// With nothing to inherit, stamping anyway would write an epoch time that reads as 1970
		// and is evicted on sight.
		let empty = change(Diffs::new());
		assert_eq!(max_input_time(&empty), None);

		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[at(3_000)])));
		let mut out = change(produced);

		stamp_output_time(&mut out, None);

		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at(3_000)]);
	}

	#[test]
	fn an_operator_stamping_above_its_inputs_is_still_overwritten() {
		// No relaxation of the clamp may reach the above-inputs direction, and the comparison is
		// strict: one nanosecond over is enough.
		let inherited = at(5_000);
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
	fn a_row_with_no_time_is_stamped_from_the_inherited_instant() {
		// #time has no none at this layer, so an unstamped row carries the epoch. Comparing only
		// against the inherited instant would keep it as "deliberately stamped early", and every
		// operator that never touches #time would emit rows retention evicts on sight.
		let mut produced = Diffs::new();
		produced.push(Diff::insert(columns(&[DateTime::default()])));
		let mut out = change(produced);

		stamp_output_time(&mut out, Some(at(6_000)));

		assert_eq!(out.diffs[0].post().unwrap().time().to_vec(), vec![at(6_000)]);
	}

	#[test]
	fn a_window_row_carries_its_window_start_through_the_apply_wrapper() {
		// Both paths a guest window emits from inherit an instant at or after the bucket start,
		// so one rule carries the start through without a special case - which is what lets a
		// consumer read #time instead of a window_start data column.
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
	fn the_max_spans_every_diff_in_the_batch() {
		// An operator fed several diffs must inherit the latest instant anywhere in the batch,
		// not the first diff's.
		let mut diffs = Diffs::new();
		diffs.push(Diff::insert(columns(&[at(1_000)])));
		diffs.push(Diff::insert(columns(&[at(12_000)])));
		diffs.push(Diff::insert(columns(&[at(3_000)])));

		assert_eq!(max_input_time(&change(diffs)), Some(at(12_000)));
	}
}
