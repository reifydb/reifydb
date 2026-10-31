// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use arrow_arith::aggregate::max;
#[cfg(feature = "runtime")]
use arrow_array::Array;
use arrow_array::{ArrayRef, RecordBatch, TimestampNanosecondArray};
#[cfg(feature = "runtime")]
use arrow_schema::SchemaRef;
use reifydb_core::{interface::change::Change, internal_err};
#[cfg(feature = "runtime")]
use reifydb_core::{
	interface::{catalog::flow::OperatorId, flow::OperatorCapability},
	metrics::heap::OperatorSample,
};
#[cfg(feature = "runtime")]
use reifydb_value::value::{container::temporal_array::datetimes, duration::Duration};
use reifydb_value::{
	Result,
	value::{
		column_view::ViewData,
		container::temporal_array::{datetime_array, datetime_timezone, datetime_to_native},
		datetime::DateTime,
		system_columns::{SystemColumn, column_view, system_column, with_system_column},
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
pub mod sink;
#[cfg(feature = "runtime")]
pub mod sort;
#[cfg(feature = "runtime")]
pub mod source;
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

	fn batches_timers(&self) -> bool {
		false
	}

	fn on_timers(&mut self, _host: &mut dyn HostContext, _timers: Vec<Timer>) -> Result<Vec<Option<Change>>> {
		internal_err!("operator {} does not batch timers", self.id().0)
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
pub(crate) fn stamp_output_created(change: &mut Change, at: DateTime) -> Result<()> {
	for diff in change.diffs.iter_mut() {
		for columns in diff.batches_mut() {
			let rows = columns.num_rows();
			for column in [SystemColumn::CreatedAt, SystemColumn::UpdatedAt] {
				if system_column(columns, column).is_none() {
					continue;
				}
				*columns = with_system_column(
					columns.clone(),
					column,
					Arc::new(datetime_array(vec![at; rows])),
				)?;
			}
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

#[cfg_attr(not(feature = "runtime"), allow(dead_code))]
pub(crate) fn time_column(times: impl IntoIterator<Item = Option<DateTime>>) -> ArrayRef {
	Arc::new(
		TimestampNanosecondArray::from_iter(times.into_iter().map(|time| time.map(datetime_to_native)))
			.with_timezone(datetime_timezone()),
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
	use reifydb_value::{factory::time::at_millis, value::container::temporal_array::datetime_array};

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
