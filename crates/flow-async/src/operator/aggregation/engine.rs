// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{HashMap, HashSet, hash_map::Entry};

use arrow_array::RecordBatch;
use reifydb_core::key::operator::state::GroupId;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, system_columns::require_row_numbers},
};
use tracing::instrument;

use super::{
	accumulator::{SlotContributions, WindowSlotKey},
	core::{Aggregation, SlotRow},
};
use crate::window::{
	accumulator::{AccumulatorEvents, AccumulatorOp},
	coord::DateTimeCoord,
	engine::tumbling::TumblingBuckets,
	span::WindowSpan,
};

pub(crate) type EngineBuckets = TumblingBuckets<Hash128, DateTimeCoord, SlotContributions>;

pub(crate) type WindowGroups = HashMap<(Hash128, u64), GroupId>;

type WindowKey = (Hash128, WindowSpan<DateTimeCoord>);

struct RoutedWindow {
	key: WindowKey,
	events: AccumulatorEvents<SlotContributions>,
	latest: Option<DateTimeCoord>,
}

#[derive(Default)]
pub(crate) struct Routing {
	index: HashMap<WindowKey, usize>,
	windows: Vec<RoutedWindow>,
	group_values: HashMap<Hash128, Vec<Value>>,
}

pub(crate) struct Routed {
	pub buckets: EngineBuckets,
	pub group_values: HashMap<Hash128, Vec<Value>>,
	pub arrival: Vec<WindowKey>,
	pub window_max_coord: HashMap<WindowKey, DateTimeCoord>,
}

impl Routing {
	pub(crate) fn push(
		&mut self,
		hash: Hash128,
		gvals: &[Value],
		span: WindowSpan<DateTimeCoord>,
		op: AccumulatorOp,
		contribution: (WindowSlotKey, SlotRow),
		event_coord: DateTimeCoord,
	) {
		let key = (hash, span);
		let index = match self.index.entry(key) {
			Entry::Occupied(slot) => *slot.get(),
			Entry::Vacant(slot) => {
				self.group_values.entry(hash).or_insert_with(|| gvals.to_vec());
				self.windows.push(RoutedWindow {
					key,
					events: AccumulatorEvents::new(),
					latest: None,
				});
				*slot.insert(self.windows.len() - 1)
			}
		};
		let window = &mut self.windows[index];
		if op == AccumulatorOp::Add {
			window.latest = Some(DateTimeCoord(
				window.latest.map_or_else(DateTime::default, |latest| latest.0).max(event_coord.0),
			));
		}
		window.events.push(op, contribution);
	}

	pub(crate) fn finish(self) -> Routed {
		let mut routed = Routed {
			buckets: EngineBuckets::new(),
			group_values: self.group_values,
			arrival: Vec::with_capacity(self.windows.len()),
			window_max_coord: HashMap::with_capacity(self.windows.len()),
		};
		for window in self.windows {
			routed.arrival.push(window.key);
			if let Some(latest) = window.latest {
				routed.window_max_coord.insert(window.key, latest);
			}
			routed.buckets.insert(window.key, window.events);
		}
		routed
	}
}

#[instrument(name = "flow::operator::aggregation::window_groups", level = "trace", skip_all, fields(windows = windows.len()))]
pub(crate) fn intern_window_groups(windows: &[(Hash128, u64)]) -> WindowGroups {
	windows.iter().map(|&(p, w)| ((p, w), GroupId::window(p, w))).collect()
}

pub(crate) fn intern_partition_groups(partitions: &[(Hash128, u64)]) -> WindowGroups {
	partitions.iter().map(|&(p, w)| ((p, w), GroupId::hashed(p))).collect()
}

pub(crate) fn group_of(groups: &WindowGroups, partition: Hash128, window_id: u64) -> GroupId {
	*groups.get(&(partition, window_id)).expect("every routed window is resolved before the engine runs")
}

pub(crate) fn slot_coord(event_coord: DateTimeCoord, row_number: u64) -> WindowSlotKey {
	WindowSlotKey::new(event_coord, row_number)
}

pub(crate) fn distinct_row_runs(pre: &RecordBatch, post: &RecordBatch) -> Result<Vec<(RecordBatch, RecordBatch)>> {
	let pre_numbers = require_row_numbers(pre)?;
	let post_numbers = require_row_numbers(post)?;
	let mut runs = Vec::new();
	let mut seen = HashSet::new();
	let mut start = 0;
	for (at, (before, after)) in pre_numbers.iter().zip(post_numbers).enumerate() {
		if seen.contains(before) || seen.contains(after) {
			runs.push((pre.slice(start, at - start), post.slice(start, at - start)));
			seen.clear();
			start = at;
		}
		seen.insert(*before);
		seen.insert(*after);
	}
	runs.push((pre.slice(start, pre.num_rows() - start), post.slice(start, post.num_rows() - start)));
	Ok(runs)
}

#[instrument(name = "flow::operator::aggregation::route", level = "trace", skip_all, fields(rows = columns.num_rows()))]
pub(crate) fn route_into_buckets<F>(
	core: &Aggregation,
	columns: &RecordBatch,
	is_add: bool,
	assign: F,
	routing: &mut Routing,
) -> Result<()>
where
	F: Fn(usize) -> (WindowSpan<DateTimeCoord>, DateTimeCoord),
{
	let row_count = columns.num_rows();
	if row_count == 0 {
		return Ok(());
	}
	let groups = core.compute_groups(columns)?;
	let slot_cols = core.evaluate_slot_inputs(columns)?;
	let views = core.slot_views(columns, &slot_cols)?;
	let row_numbers = require_row_numbers(columns)?;
	for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
		let (span, event_coord) = assign(row_idx);
		let coord = slot_coord(event_coord, row_numbers[row_idx].0);
		let contribution = (coord, views.contribution(row_idx, event_coord.0));
		let op = if is_add {
			AccumulatorOp::Add
		} else {
			AccumulatorOp::Remove
		};
		routing.push(*hash, gvals, span, op, contribution, event_coord);
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use reifydb_core::key::operator::state::GroupId;
	use reifydb_value::util::hash::Hash128;

	const PARTITION: Hash128 = Hash128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);

	#[test]
	fn a_partition_group_can_never_collide_with_a_window_group_of_the_same_partition() {
		// Only the leading window id field separates the two kinds; a collision would alias a partition's
		// session tracker onto a window's accumulators.
		let partition = GroupId::hashed(PARTITION);
		assert_eq!(partition.window_id(), None);
		for window_id in [0u64, 1, u64::MAX - 1] {
			let window = GroupId::window(PARTITION, window_id);
			assert_ne!(partition, window);
			assert_eq!(window.window_id(), Some(window_id));
		}
	}

	#[test]
	fn no_window_group_can_ever_be_the_root_group() {
		// Root owns the timer wheel, the expiry index and the reap queue, so a window landing on it would let
		// the reaper delete the operator's own scheduling state.
		for window_id in [0u64, 1, u64::MAX - 1] {
			for partition in [Hash128(0), Hash128(1), Hash128(u128::MAX)] {
				assert_ne!(GroupId::window(partition, window_id), GroupId::ROOT);
			}
		}
	}

	#[test]
	fn a_window_id_orders_its_groups_ahead_of_every_later_window() {
		// The reaper sweeps a window as one contiguous wire span, so all of window n must sort before any of
		// window n+1 whatever the partition hash.
		let earlier = GroupId::window(Hash128(u128::MAX), 7);
		let later = GroupId::window(Hash128(0), 8);
		assert!(earlier > later, "on the wire the raw order is inverted, so the older window sorts first");
	}
}

#[cfg(test)]
mod bucket_start_tests {
	use reifydb_value::value::duration::Duration;

	use super::*;
	use crate::operator::state::seal::coord::Coord;

	fn order(millis: i64) -> u64 {
		DateTime::from_epoch_millis(millis).unwrap().to_order()
	}

	#[test]
	fn a_bucket_stamps_the_same_time_regardless_of_what_arrived_in_it() {
		// The stamp is a pure function of the bucket, so two replays of one corpus agree. A
		// max-contributor stamp would vary with arrival and change retention decisions.
		let bucket = order(1_700_000_000_000);

		assert_eq!(
			<DateTimeCoord as Coord>::from_order(bucket),
			DateTimeCoord(DateTime::from_epoch_millis(1_700_000_000_000).unwrap())
		);
		assert_eq!(
			<DateTimeCoord as Coord>::from_order(bucket),
			<DateTimeCoord as Coord>::from_order(bucket),
			"the stamp depends on the bucket alone, so it cannot vary between two runs"
		);
	}

	#[test]
	fn adjacent_buckets_get_distinct_stamps_in_bucket_order() {
		// A chained rollup (1s -> 1m) collapses onto one instant unless distinct buckets get
		// distinct stamps.
		let first = <DateTimeCoord as Coord>::from_order(order(1_700_000_000_000));
		let second = <DateTimeCoord as Coord>::from_order(order(1_700_000_001_000));

		assert!(first < second, "bucket order must survive into #time");
		assert_eq!(second.0 - first.0, Duration::from_seconds(1).unwrap(), "a 1s bucket step is 1s in #time");
	}

	#[test]
	fn a_far_future_bucket_saturates_rather_than_wrapping() {
		// An unrepresentable bucket must still order above a real one; wrapping would make it
		// look ancient and be evicted at once.
		assert_eq!(<DateTimeCoord as Coord>::from_order(u64::MAX), DateTimeCoord(DateTime::MAX));
		assert!(<DateTimeCoord as Coord>::from_order(u64::MAX)
			> <DateTimeCoord as Coord>::from_order(1_700_000_000_000));
	}

	#[test]
	fn the_zero_bucket_maps_to_the_earliest_instant() {
		// An unset window_start must not be mistakeable for a real time far from zero.
		assert_eq!(<DateTimeCoord as Coord>::from_order(0), DateTimeCoord(DateTime::MIN));
	}
}
