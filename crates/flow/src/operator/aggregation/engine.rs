// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_codec::key::{encode_u64_asc, encode_u128_asc, encoded::EncodedKey};
use reifydb_core::{
	interface::change::{Change, Diff},
	key::operator_state::GroupId,
	value::column::columns::Columns,
};
use reifydb_rql::flow::aggregate::SlotKind;
use reifydb_value::{
	Result, reifydb_assertions,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, duration::Duration},
};
use tracing::instrument;

use super::{
	accumulator::{RowAccumulator, WindowSlotKey},
	core::Aggregation,
};
use crate::{
	operator::{
		host::HostContext,
		state::{seal::coord::Coord, store},
		state_access::{get_classified, put, remove},
	},
	window::{
		engine::{
			AccumulatorEvent, EmitKind, ExpiryAnchor,
			config::WindowEngineConfig,
			tumbling::{TumblingBuckets, TumblingEngine},
		},
		meta::{EngineMeta, EngineMetaKey},
		span::WindowSpan,
	},
};

pub(crate) type EngineBuckets = TumblingBuckets<Hash128, DateTime, (WindowSlotKey, Vec<Option<Value>>)>;

pub(crate) type WindowGroups = HashMap<(Hash128, u64), GroupId>;

const WINDOW_GROUP: u8 = 0x00;
const PARTITION_GROUP: u8 = 0x01;

pub(crate) fn window_group_key(partition: Hash128, window_id: u64) -> EncodedKey {
	let mut bytes = Vec::with_capacity(1 + 16 + 8);
	bytes.push(WINDOW_GROUP);
	bytes.extend_from_slice(&encode_u128_asc(partition.0));
	bytes.extend_from_slice(&encode_u64_asc(window_id));
	EncodedKey::new(bytes)
}

pub(crate) fn partition_group_key(partition: Hash128) -> EncodedKey {
	let mut bytes = Vec::with_capacity(1 + 16);
	bytes.push(PARTITION_GROUP);
	bytes.extend_from_slice(&encode_u128_asc(partition.0));
	EncodedKey::new(bytes)
}

#[instrument(name = "flow::operator::aggregation::window_groups", level = "trace", skip_all, fields(windows = windows.len()))]
pub(crate) fn intern_window_groups(windows: &[(Hash128, u64)]) -> WindowGroups {
	windows.iter().map(|&(p, w)| ((p, w), GroupId::of(&window_group_key(p, w)))).collect()
}

pub(crate) fn group_of(groups: &WindowGroups, partition: Hash128, window_id: u64) -> GroupId {
	*groups.get(&(partition, window_id)).expect("every routed window is resolved before the engine runs")
}

pub(crate) fn slot_coord(is_count: bool, event_ts: DateTime, row_number: u64) -> WindowSlotKey {
	let timestamp = if is_count {
		DateTime::default()
	} else {
		event_ts
	};
	WindowSlotKey::new(timestamp, row_number)
}

#[allow(clippy::too_many_arguments)]
#[instrument(name = "flow::operator::aggregation::route", level = "trace", skip_all, fields(rows = columns.row_count()))]
pub(crate) fn route_into_buckets<F>(
	core: &Aggregation,
	columns: &Columns,
	is_add: bool,
	assign: F,
	buckets: &mut EngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	arrival: &mut Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: &mut HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
) -> Result<()>
where
	F: Fn(usize) -> (WindowSpan<DateTime>, DateTime),
{
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(());
	}
	let groups = core.compute_groups(columns)?;
	let slot_cols = core.evaluate_slot_inputs(columns)?;
	for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
		let (span, event_ts) = assign(row_idx);
		let coord = slot_coord(false, event_ts, columns.row_numbers()[row_idx].0);
		let contribution = (coord, core.build_contribution(columns, &slot_cols, row_idx, event_ts));
		let key = (*hash, span);
		let event = if is_add {
			let entry = window_max_ts.entry(key).or_default();
			*entry = (*entry).max(event_ts);
			AccumulatorEvent::Add(contribution)
		} else {
			AccumulatorEvent::Remove(contribution)
		};
		if !buckets.contains_key(&key) {
			arrival.push(key);
		}
		buckets.entry(key).or_default().push(event);
		group_values.entry(*hash).or_insert_with(|| gvals.clone());
	}
	Ok(())
}

#[allow(clippy::too_many_arguments)]
#[instrument(name = "flow::operator::aggregation::finish", level = "trace", skip_all, fields(buckets = buckets.len()))]
pub(crate) fn finish_tumbling_engine(
	core: &mut Aggregation,
	host: &mut dyn HostContext,
	change: &Change,
	buckets: EngineBuckets,
	group_values: &HashMap<Hash128, Vec<Value>>,
	arrival: Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
	groups: &WindowGroups,
	kinds: &[SlotKind],
	engine_config: WindowEngineConfig,
	immutable: Option<Duration>,
	anchor: ExpiryAnchor,
	indexed_retractions: bool,
) -> Result<Vec<Diff>> {
	let mut engine = core
		.tumbling_engine_slot()
		.take()
		.unwrap_or_else(|| Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::new(engine_config)));
	let results = engine.apply(
		host,
		buckets,
		&arrival,
		|hash, window_start| (group_of(groups, *hash, window_start.to_order()), store::empty_key()),
		|| RowAccumulator::new(kinds, immutable),
	)?;
	let _ = indexed_retractions;
	reifydb_assertions! {
		let dropped = engine.dropped_retractions();
		assert!(
			!indexed_retractions || dropped == 0,
			"a retraction resolved through the row index must find the state it addresses; dropping \
			 it against an empty accumulator means the row index outlived the window it points at, \
			 so the withdrawal is lost and the window publishes its pre-retraction value forever \
			 (dropped={dropped})"
		);
	}

	for r in &results {
		let group = group_of(groups, r.group, r.span.start.to_order());
		let window_start = r.span.start.to_order();
		let prior_meta = get_classified::<_, EngineMeta>(host, &EngineMetaKey(group))?;
		let prior_last = prior_meta.as_ref().map(|m| m.last_event_time);
		let prior_index = prior_meta.is_some().then(|| anchor.of(window_start, prior_last)).flatten();
		match r.kind {
			EmitKind::Remove => {
				engine.reindex_window(
					host,
					&r.group,
					r.span.start,
					group,
					&store::empty_key(),
					prior_index,
					None,
				)?;
				remove(host, &EngineMetaKey(group))?;
			}
			EmitKind::Insert | EmitKind::Update => {
				let batch_max = window_max_ts.get(&(r.group, r.span)).map(|ts| ts.to_order());
				let last_event_time = prior_last.max(batch_max);
				let new_index = anchor.of(window_start, last_event_time);
				engine.reindex_window(
					host,
					&r.group,
					r.span.start,
					group,
					&store::empty_key(),
					prior_index,
					new_index,
				)?;
				let meta = EngineMeta {
					last_event_time: last_event_time.unwrap_or_default(),
				};
				put(host, &EngineMetaKey(group), meta)?;
			}
		}
	}
	*core.tumbling_engine_slot() = Some(engine);

	let ts = change.changed_at;
	let mut diffs = Vec::new();
	for r in results {
		let gvals = group_values.get(&r.group).cloned().unwrap_or_default();
		match r.kind {
			EmitKind::Insert => {
				let row = core.build_engine_row(&gvals, &r.value, r.row_number, ts, Some(r.span))?;
				diffs.push(Diff::insert(Columns::from_row(&row)));
			}
			EmitKind::Update => {
				let pre_vals: &[Value] = r.prior.as_deref().unwrap_or(&r.value);
				let pre = core.build_engine_row(&gvals, pre_vals, r.row_number, ts, Some(r.span))?;
				let post = core.build_engine_row(&gvals, &r.value, r.row_number, ts, Some(r.span))?;
				diffs.push(Diff::update(Columns::from_row(&pre), Columns::from_row(&post)));
			}
			EmitKind::Remove => {
				let pre_vals: &[Value] = r.prior.as_deref().unwrap_or(&r.value);
				let pre = core.build_engine_row(&gvals, pre_vals, r.row_number, ts, Some(r.span))?;
				diffs.push(Diff::remove(Columns::from_row(&pre)));
			}
		}
	}
	Ok(diffs)
}

#[cfg(test)]
mod tests {
	use reifydb_value::util::hash::Hash128;

	use super::{partition_group_key, window_group_key};

	const PARTITION: Hash128 = Hash128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef);

	#[test]
	fn a_partition_group_can_never_collide_with_a_window_group() {
		// Both kinds share one dictionary, and without the leading discriminator only the
		// (incidentally fixed) coordinate width separates them: a collision would alias a
		// partition's session tracker onto a window's accumulators.
		let partition = partition_group_key(PARTITION);
		for window_id in [0u64, 1, u64::MAX] {
			let window = window_group_key(PARTITION, window_id);
			assert_ne!(partition, window);
			assert_ne!(
				partition.as_bytes()[0],
				window.as_bytes()[0],
				"the discriminator, not the length, must be what separates the two kinds"
			);
		}
	}
}

#[cfg(test)]
mod bucket_start_tests {
	use super::*;

	fn order(millis: u64) -> u64 {
		DateTime::from_epoch_millis(millis).unwrap().to_order()
	}

	#[test]
	fn a_bucket_stamps_the_same_time_regardless_of_what_arrived_in_it() {
		// The stamp is a pure function of the bucket, so two replays of one corpus agree. A
		// max-contributor stamp would vary with arrival and change retention decisions.
		let bucket = order(1_700_000_000_000);

		assert_eq!(
			<DateTime as Coord>::from_order(bucket),
			DateTime::from_epoch_millis(1_700_000_000_000).unwrap()
		);
		assert_eq!(
			<DateTime as Coord>::from_order(bucket),
			<DateTime as Coord>::from_order(bucket),
			"the stamp depends on the bucket alone, so it cannot vary between two runs"
		);
	}

	#[test]
	fn adjacent_buckets_get_distinct_stamps_in_bucket_order() {
		// A chained rollup (1s -> 1m) collapses onto one instant unless distinct buckets get
		// distinct stamps.
		let first = <DateTime as Coord>::from_order(order(1_700_000_000_000));
		let second = <DateTime as Coord>::from_order(order(1_700_000_001_000));

		assert!(first < second, "bucket order must survive into #time");
		assert_eq!(second - first, Duration::from_seconds(1).unwrap(), "a 1s bucket step is 1s in #time");
	}

	#[test]
	fn a_far_future_bucket_saturates_rather_than_wrapping() {
		// An unrepresentable bucket must still order above a real one; wrapping would make it
		// look ancient and be evicted at once.
		assert_eq!(<DateTime as Coord>::from_order(u64::MAX), DateTime::MAX);
		assert!(<DateTime as Coord>::from_order(u64::MAX) > <DateTime as Coord>::from_order(1_700_000_000_000));
	}

	#[test]
	fn the_zero_bucket_maps_to_the_epoch() {
		// An unset window_start must not be mistakeable for a real time far from zero.
		assert_eq!(<DateTime as Coord>::from_order(0), DateTime::EPOCH);
	}
}
