// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use std::collections::{BTreeMap, HashMap, HashSet};

use reifydb_codec::key::encoded::IntoEncodedKey;
use reifydb_core::{
	common::TimeDomain,
	interface::change::{Change, Diff},
	value::column::columns::Columns,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, WindowStateKey,
			config::WindowEngineConfig,
			tumbling::{TumblingBuckets, TumblingEngine, reindex_window},
		},
		span::WindowSpan,
		store::WindowStore,
	},
};
use reifydb_engine::flow::aggregate::SlotKind;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, duration::Duration},
};
use serde::{Deserialize, Serialize};
use tracing::Span;

use super::{
	accumulator::{RowAccumulator, WindowSlotKey},
	aggregation::Aggregation,
	operator::WindowOperator,
	store::FlowWindowStore,
};
use crate::{
	operator::{stateful::row::RowNumberProvider, window::warn_when_expiry_capped},
	transaction::FlowTransaction,
};

type EngineBuckets = TumblingBuckets<Hash128, u64, (WindowSlotKey, Vec<Option<Value>>)>;

pub(super) fn slot_coord(is_count: bool, event_ts: u64, row_number: u64) -> WindowSlotKey {
	let timestamp = if is_count {
		DateTime::default()
	} else {
		DateTime::from_timestamp_millis(event_ts).unwrap_or_default()
	};
	WindowSlotKey::new(timestamp, row_number)
}

#[derive(Default, Serialize, Deserialize)]
struct EngineWindowMeta {
	group_hash: u128,
	window_start: u64,
	row_number: u64,
	last_event_time: u64,
	group_values: Vec<Value>,
}

#[allow(clippy::too_many_arguments)]
pub(super) fn route_into_buckets<F>(
	core: &Aggregation,
	columns: &Columns,
	is_add: bool,
	assign: F,
	buckets: &mut EngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	arrival: &mut Vec<(Hash128, WindowSpan<u64>)>,
	window_max_ts: &mut HashMap<(Hash128, WindowSpan<u64>), u64>,
) -> Result<()>
where
	F: Fn(usize) -> (WindowSpan<u64>, u64),
{
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(());
	}
	let groups = core.compute_groups(columns)?;
	let slot_cols = core.evaluate_slot_inputs(columns)?;
	for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
		let (span, event_ts) = assign(row_idx);
		let coord = slot_coord(false, event_ts, columns.row_numbers[row_idx].0);
		let contribution = (coord, core.build_contribution(columns, &slot_cols, row_idx));
		let key = (*hash, span);
		let event = if is_add {
			let entry = window_max_ts.entry(key).or_insert(0);
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
fn route_engine_columns(
	operator: &WindowOperator,
	columns: &Columns,
	is_add: bool,
	window_size_ms: u64,
	buckets: &mut EngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	arrival: &mut Vec<(Hash128, WindowSpan<u64>)>,
	window_max_ts: &mut HashMap<(Hash128, WindowSpan<u64>), u64>,
) -> Result<()> {
	let timestamps = operator.resolve_event_timestamps(columns, columns.row_count())?;
	route_into_buckets(
		&operator.core,
		columns,
		is_add,
		|row_idx| {
			let ts = timestamps[row_idx];
			(WindowSpan::for_slot(ts, window_size_ms), ts)
		},
		buckets,
		group_values,
		arrival,
		window_max_ts,
	)
}

#[allow(clippy::too_many_arguments)]
fn push_count_event(
	buckets: &mut EngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	arrival: &mut Vec<(Hash128, WindowSpan<u64>)>,
	window_max_ts: &mut HashMap<(Hash128, WindowSpan<u64>), u64>,
	hash: Hash128,
	gvals: &[Value],
	window_id: u64,
	coord: WindowSlotKey,
	event: AccumulatorEvent<Vec<Option<Value>>>,
	event_ts: u64,
) {
	let now = event_ts;
	let span = WindowSpan::new(window_id, window_id + 1);
	let key = (hash, span);
	let event = match event {
		AccumulatorEvent::Add(c) => AccumulatorEvent::Add((coord, c)),
		AccumulatorEvent::Remove(c) => AccumulatorEvent::Remove((coord, c)),
	};
	if matches!(event, AccumulatorEvent::Add(_)) {
		let entry = window_max_ts.entry(key).or_insert(0);
		*entry = (*entry).max(now);
	}
	if !buckets.contains_key(&key) {
		arrival.push(key);
	}
	buckets.entry(key).or_default().push(event);
	group_values.entry(hash).or_insert_with(|| gvals.to_vec());
}

fn route_count_tumbling(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	change: &Change,
	buckets: &mut EngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	arrival: &mut Vec<(Hash128, WindowSpan<u64>)>,
	window_max_ts: &mut HashMap<(Hash128, WindowSpan<u64>), u64>,
) -> Result<()> {
	let size = operator.size_count().unwrap_or(1).max(1);
	let now = operator.core.current_timestamp();
	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => {
				let groups = operator.core.compute_groups(post)?;
				let slot_cols = operator.core.evaluate_slot_inputs(post)?;
				for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
					let ordinal = operator.get_and_increment_global_count(txn, *hash)?;
					let window_id = ordinal / size;
					operator.store_row_index(txn, *hash, post.row_numbers[row_idx], window_id)?;
					let contribution = operator.core.build_contribution(post, &slot_cols, row_idx);
					let coord = slot_coord(true, now, post.row_numbers[row_idx].0);
					push_count_event(
						buckets,
						group_values,
						arrival,
						window_max_ts,
						*hash,
						gvals,
						window_id,
						coord,
						AccumulatorEvent::Add(contribution),
						now,
					);
				}
			}
			Diff::Remove {
				pre,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let slot_cols = operator.core.evaluate_slot_inputs(pre)?;
				for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
					let contribution = operator.core.build_contribution(pre, &slot_cols, row_idx);
					let coord = slot_coord(true, now, pre.row_numbers[row_idx].0);
					for window_id in
						operator.lookup_row_index(txn, *hash, pre.row_numbers[row_idx])?
					{
						push_count_event(
							buckets,
							group_values,
							arrival,
							window_max_ts,
							*hash,
							gvals,
							window_id,
							coord,
							AccumulatorEvent::Remove(contribution.clone()),
							now,
						);
					}
				}
			}
			Diff::Update {
				pre,
				post,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let pre_cols = operator.core.evaluate_slot_inputs(pre)?;
				let post_cols = operator.core.evaluate_slot_inputs(post)?;
				for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
					let row_number = pre.row_numbers[row_idx];
					let existing = operator.lookup_row_index(txn, *hash, row_number)?;
					if existing.is_empty() {
						let ordinal = operator.get_and_increment_global_count(txn, *hash)?;
						let window_id = ordinal / size;
						operator.store_row_index(
							txn,
							*hash,
							post.row_numbers[row_idx],
							window_id,
						)?;
						let contribution =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord = slot_coord(true, now, post.row_numbers[row_idx].0);
						push_count_event(
							buckets,
							group_values,
							arrival,
							window_max_ts,
							*hash,
							gvals,
							window_id,
							coord,
							AccumulatorEvent::Add(contribution),
							now,
						);
					} else {
						let pre_contrib =
							operator.core.build_contribution(pre, &pre_cols, row_idx);
						let post_contrib =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord = slot_coord(true, now, pre.row_numbers[row_idx].0);
						for window_id in existing {
							push_count_event(
								buckets,
								group_values,
								arrival,
								window_max_ts,
								*hash,
								gvals,
								window_id,
								coord,
								AccumulatorEvent::Remove(pre_contrib.clone()),
								now,
							);
							push_count_event(
								buckets,
								group_values,
								arrival,
								window_max_ts,
								*hash,
								gvals,
								window_id,
								coord,
								AccumulatorEvent::Add(post_contrib.clone()),
								now,
							);
						}
					}
				}
			}
		}
	}
	Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn finish_tumbling_engine(
	core: &Aggregation,
	txn: &mut FlowTransaction,
	row_numbers: &RowNumberProvider,
	change: &Change,
	buckets: EngineBuckets,
	group_values: &HashMap<Hash128, Vec<Value>>,
	arrival: Vec<(Hash128, WindowSpan<u64>)>,
	window_max_ts: HashMap<(Hash128, WindowSpan<u64>), u64>,
	kinds: &[SlotKind],
	engine_config: WindowEngineConfig,
	grace: Duration,
	index: bool,
) -> Result<Vec<Diff>> {
	let results = {
		let mut store = FlowWindowStore::new(txn, core.node, row_numbers);
		for (hash, span) in &arrival {
			let key = core.create_window_key(*hash, span.start);
			store.get_or_create_row_number(&key)?;
		}
		let mut engine = TumblingEngine::<Hash128, u64, RowAccumulator>::new(engine_config);
		let res = engine.apply(
			&mut store,
			buckets,
			|hash, window_start| core.create_window_key(*hash, window_start),
			|| RowAccumulator::new(kinds, grace),
		)?;
		engine.flush(&mut store)?;
		res
	};

	{
		let mut store = FlowWindowStore::new(txn, core.node, row_numbers);
		for r in &results {
			let ewm_key = core.create_engine_meta_key(r.group, r.span.start);
			let prior_last =
				store.state_get::<EngineWindowMeta>(&ewm_key)?.map(|m| m.last_event_time).unwrap_or(0);
			match r.kind {
				EmitKind::Remove => {
					if index {
						reindex_window(
							&mut store,
							&r.group,
							r.span.start,
							r.row_number,
							(prior_last > 0).then_some(prior_last),
							None,
						)?;
					}
					store.state_drop(&ewm_key)?;
				}
				EmitKind::Insert | EmitKind::Update => {
					let batch_max = window_max_ts.get(&(r.group, r.span)).copied().unwrap_or(0);
					let last_event_time = prior_last.max(batch_max);
					if index {
						reindex_window(
							&mut store,
							&r.group,
							r.span.start,
							r.row_number,
							(prior_last > 0).then_some(prior_last),
							(last_event_time > 0).then_some(last_event_time),
						)?;
					}
					let meta = EngineWindowMeta {
						group_hash: r.group.0,
						window_start: r.span.start,
						row_number: r.row_number.0,
						last_event_time,
						group_values: group_values.get(&r.group).cloned().unwrap_or_default(),
					};
					store.state_set(&ewm_key, &meta)?;
				}
			}
		}
	}

	let ts_nanos = change.changed_at.to_nanos();
	let mut diffs = Vec::new();
	for r in results {
		let gvals = group_values.get(&r.group).cloned().unwrap_or_default();
		match r.kind {
			EmitKind::Insert => {
				let row = core.build_engine_row(&gvals, &r.value, r.row_number, ts_nanos)?;
				diffs.push(Diff::insert(Columns::from_row(&row)));
			}
			EmitKind::Update => {
				let pre_vals: &[Value] = r.prior.as_deref().unwrap_or(&r.value);
				let pre = core.build_engine_row(&gvals, pre_vals, r.row_number, ts_nanos)?;
				let post = core.build_engine_row(&gvals, &r.value, r.row_number, ts_nanos)?;
				diffs.push(Diff::update(Columns::from_row(&pre), Columns::from_row(&post)));
			}
			EmitKind::Remove => {
				let pre_vals: &[Value] = r.prior.as_deref().unwrap_or(&r.value);
				let pre = core.build_engine_row(&gvals, pre_vals, r.row_number, ts_nanos)?;
				diffs.push(Diff::remove(Columns::from_row(&pre)));
			}
		}
	}
	Ok(diffs)
}

pub fn apply_tumbling_engine(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let window_size_ms = operator.size_duration().map(|d| d.milliseconds().unwrap_or(0) as u64).unwrap_or(0);
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<u64>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<u64>), u64> = HashMap::new();

	if operator.is_count_based() {
		route_count_tumbling(
			operator,
			txn,
			&change,
			&mut buckets,
			&mut group_values,
			&mut arrival,
			&mut window_max_ts,
		)?;
	} else {
		for diff in change.diffs.iter() {
			match diff {
				Diff::Insert {
					post,
					..
				} => route_engine_columns(
					operator,
					post,
					true,
					window_size_ms,
					&mut buckets,
					&mut group_values,
					&mut arrival,
					&mut window_max_ts,
				)?,
				Diff::Remove {
					pre,
					..
				} => route_engine_columns(
					operator,
					pre,
					false,
					window_size_ms,
					&mut buckets,
					&mut group_values,
					&mut arrival,
					&mut window_max_ts,
				)?,
				Diff::Update {
					pre,
					post,
					..
				} => {
					route_engine_columns(
						operator,
						pre,
						false,
						window_size_ms,
						&mut buckets,
						&mut group_values,
						&mut arrival,
						&mut window_max_ts,
					)?;
					route_engine_columns(
						operator,
						post,
						true,
						window_size_ms,
						&mut buckets,
						&mut group_values,
						&mut arrival,
						&mut window_max_ts,
					)?;
				}
			}
		}
	}

	if operator.kind.time() == TimeDomain::Event
		&& !operator.is_count_based()
		&& let Some(batch_max) = window_max_ts.values().copied().max()
	{
		operator.advance_event_watermark(txn, batch_max)?;
	}

	gate_sealed_buckets(
		operator,
		txn,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		window_size_ms + operator.grace_ms(),
	)?;

	let diffs = finish_tumbling_engine(
		&operator.core,
		txn,
		&operator.row_number_provider,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&kinds,
		operator.engine_config(),
		operator.grace(),
		!operator.is_count_based(),
	)?;
	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

fn sliding_insert_window_ids(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	hash: Hash128,
	event_ts: u64,
	is_count: bool,
	is_event: bool,
) -> Result<Vec<u64>> {
	let coord = if is_count {
		operator.get_and_increment_global_count(txn, hash)?
	} else if is_event {
		event_ts
	} else {
		operator.core.current_timestamp()
	};
	Ok(operator.get_sliding_window_ids(coord))
}

pub fn apply_sliding_engine(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let is_count = operator.is_count_based();
	let is_event = operator.ts.is_some();
	let window_size_ms = operator.size_duration().map(|d| d.milliseconds().unwrap_or(0) as u64).unwrap_or(0);

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<u64>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<u64>), u64> = HashMap::new();

	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => {
				let groups = operator.core.compute_groups(post)?;
				let timestamps = if is_count {
					Vec::new()
				} else {
					operator.resolve_event_timestamps(post, post.row_count())?
				};
				let slot_cols = operator.core.evaluate_slot_inputs(post)?;
				for row_idx in 0..post.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let event_ts = if is_count {
						0
					} else {
						timestamps[row_idx]
					};
					let window_ids = sliding_insert_window_ids(
						operator, txn, *hash, event_ts, is_count, is_event,
					)?;
					let contribution = operator.core.build_contribution(post, &slot_cols, row_idx);
					let coord = slot_coord(is_count, event_ts, post.row_numbers[row_idx].0);
					for wid in &window_ids {
						operator.store_row_index(txn, *hash, post.row_numbers[row_idx], *wid)?;
						push_count_event(
							&mut buckets,
							&mut group_values,
							&mut arrival,
							&mut window_max_ts,
							*hash,
							gvals,
							*wid,
							coord,
							AccumulatorEvent::Add(contribution.clone()),
							event_ts,
						);
					}
				}
			}
			Diff::Remove {
				pre,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let timestamps = if is_count {
					Vec::new()
				} else {
					operator.resolve_event_timestamps(pre, pre.row_count())?
				};
				let slot_cols = operator.core.evaluate_slot_inputs(pre)?;
				for row_idx in 0..pre.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let event_ts = if is_count {
						0
					} else {
						timestamps[row_idx]
					};
					let contribution = operator.core.build_contribution(pre, &slot_cols, row_idx);
					let coord = slot_coord(is_count, event_ts, pre.row_numbers[row_idx].0);
					for wid in operator.lookup_row_index(txn, *hash, pre.row_numbers[row_idx])? {
						push_count_event(
							&mut buckets,
							&mut group_values,
							&mut arrival,
							&mut window_max_ts,
							*hash,
							gvals,
							wid,
							coord,
							AccumulatorEvent::Remove(contribution.clone()),
							event_ts,
						);
					}
				}
			}
			Diff::Update {
				pre,
				post,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let timestamps = if is_count {
					Vec::new()
				} else {
					operator.resolve_event_timestamps(post, post.row_count())?
				};
				let pre_cols = operator.core.evaluate_slot_inputs(pre)?;
				let post_cols = operator.core.evaluate_slot_inputs(post)?;
				for row_idx in 0..pre.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let row_number = pre.row_numbers[row_idx];
					let event_ts = if is_count {
						0
					} else {
						timestamps[row_idx]
					};
					let existing = operator.lookup_row_index(txn, *hash, row_number)?;
					if existing.is_empty() {
						let window_ids = sliding_insert_window_ids(
							operator, txn, *hash, event_ts, is_count, is_event,
						)?;
						let contribution =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord = slot_coord(is_count, event_ts, row_number.0);
						for wid in &window_ids {
							operator.store_row_index(
								txn,
								*hash,
								post.row_numbers[row_idx],
								*wid,
							)?;
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								*wid,
								coord,
								AccumulatorEvent::Add(contribution.clone()),
								event_ts,
							);
						}
					} else {
						let pre_contrib =
							operator.core.build_contribution(pre, &pre_cols, row_idx);
						let post_contrib =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord = slot_coord(is_count, event_ts, row_number.0);
						for wid in existing {
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								wid,
								coord,
								AccumulatorEvent::Remove(pre_contrib.clone()),
								event_ts,
							);
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								wid,
								coord,
								AccumulatorEvent::Add(post_contrib.clone()),
								event_ts,
							);
						}
					}
				}
			}
		}
	}

	if operator.kind.time() == TimeDomain::Event
		&& !operator.is_count_based()
		&& let Some(batch_max) = window_max_ts.values().copied().max()
	{
		operator.advance_event_watermark(txn, batch_max)?;
	}

	gate_sealed_buckets(
		operator,
		txn,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		window_size_ms + operator.grace_ms(),
	)?;

	let diffs = finish_tumbling_engine(
		&operator.core,
		txn,
		&operator.row_number_provider,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&kinds,
		operator.engine_config(),
		operator.grace(),
		!operator.is_count_based(),
	)?;
	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

fn session_assign(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	hash: Hash128,
	event_ts: u64,
	gap_ms: u64,
	trackers: &mut HashMap<Hash128, (u64, u64, u64)>,
	closes: &mut Vec<(Hash128, u64)>,
) -> Result<Option<u64>> {
	let (mut session_id, last, start) = match trackers.get(&hash) {
		Some(&tracker) => tracker,
		None => {
			let tracker = operator.load_session_tracker(txn, hash)?;
			trackers.insert(hash, tracker);
			tracker
		}
	};
	if last == 0 {
		trackers.insert(hash, (session_id, event_ts, event_ts));
		return Ok(Some(session_id));
	}
	if event_ts > last && event_ts - last > gap_ms {
		closes.push((hash, session_id));
		session_id += 1;
		trackers.insert(hash, (session_id, event_ts, event_ts));
		return Ok(Some(session_id));
	}
	if event_ts < start && start - event_ts > gap_ms {
		return Ok(None);
	}
	trackers.insert(hash, (session_id, last.max(event_ts), start.min(event_ts)));
	Ok(Some(session_id))
}

pub fn apply_session_engine(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let gap_ms = operator.session_gap_ms();

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<u64>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<u64>), u64> = HashMap::new();
	let mut closes: Vec<(Hash128, u64)> = Vec::new();
	let mut trackers: HashMap<Hash128, (u64, u64, u64)> = HashMap::new();

	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => {
				let groups = operator.core.compute_groups(post)?;
				let timestamps = operator.resolve_event_timestamps(post, post.row_count())?;
				let slot_cols = operator.core.evaluate_slot_inputs(post)?;
				for row_idx in 0..post.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let event_ts = timestamps[row_idx];
					if let Some(session_id) = session_assign(
						operator,
						txn,
						*hash,
						event_ts,
						gap_ms,
						&mut trackers,
						&mut closes,
					)? {
						operator.store_row_index(
							txn,
							*hash,
							post.row_numbers[row_idx],
							session_id,
						)?;
						let contribution =
							operator.core.build_contribution(post, &slot_cols, row_idx);
						let coord = slot_coord(false, event_ts, post.row_numbers[row_idx].0);
						push_count_event(
							&mut buckets,
							&mut group_values,
							&mut arrival,
							&mut window_max_ts,
							*hash,
							gvals,
							session_id,
							coord,
							AccumulatorEvent::Add(contribution),
							event_ts,
						);
					}
				}
			}
			Diff::Remove {
				pre,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let timestamps = operator.resolve_event_timestamps(pre, pre.row_count())?;
				let slot_cols = operator.core.evaluate_slot_inputs(pre)?;
				for row_idx in 0..pre.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let event_ts = timestamps[row_idx];
					let contribution = operator.core.build_contribution(pre, &slot_cols, row_idx);
					let coord = slot_coord(false, event_ts, pre.row_numbers[row_idx].0);
					for session_id in
						operator.lookup_row_index(txn, *hash, pre.row_numbers[row_idx])?
					{
						push_count_event(
							&mut buckets,
							&mut group_values,
							&mut arrival,
							&mut window_max_ts,
							*hash,
							gvals,
							session_id,
							coord,
							AccumulatorEvent::Remove(contribution.clone()),
							event_ts,
						);
					}
				}
			}
			Diff::Update {
				pre,
				post,
				..
			} => {
				let groups = operator.core.compute_groups(pre)?;
				let timestamps = operator.resolve_event_timestamps(post, post.row_count())?;
				let pre_cols = operator.core.evaluate_slot_inputs(pre)?;
				let post_cols = operator.core.evaluate_slot_inputs(post)?;
				for row_idx in 0..pre.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let event_ts = timestamps[row_idx];
					let existing =
						operator.lookup_row_index(txn, *hash, pre.row_numbers[row_idx])?;
					if existing.is_empty() {
						if let Some(session_id) = session_assign(
							operator,
							txn,
							*hash,
							event_ts,
							gap_ms,
							&mut trackers,
							&mut closes,
						)? {
							operator.store_row_index(
								txn,
								*hash,
								post.row_numbers[row_idx],
								session_id,
							)?;
							let contribution = operator
								.core
								.build_contribution(post, &post_cols, row_idx);
							let coord = slot_coord(
								false,
								event_ts,
								post.row_numbers[row_idx].0,
							);
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								session_id,
								coord,
								AccumulatorEvent::Add(contribution),
								event_ts,
							);
						}
					} else {
						let pre_contrib =
							operator.core.build_contribution(pre, &pre_cols, row_idx);
						let post_contrib =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord = slot_coord(false, event_ts, pre.row_numbers[row_idx].0);
						for session_id in existing {
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								session_id,
								coord,
								AccumulatorEvent::Remove(pre_contrib.clone()),
								event_ts,
							);
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								session_id,
								coord,
								AccumulatorEvent::Add(post_contrib.clone()),
								event_ts,
							);
						}
					}
				}
			}
		}
	}

	for (hash, (session_id, last, start)) in &trackers {
		operator.save_session_tracker(txn, *hash, *session_id, *last, *start)?;
	}

	if operator.kind.time() == TimeDomain::Event
		&& !operator.is_count_based()
		&& let Some(batch_max) = window_max_ts.values().copied().max()
	{
		operator.advance_event_watermark(txn, batch_max)?;
	}

	gate_sealed_buckets(
		operator,
		txn,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		operator.session_gap_ms() + operator.grace_ms(),
	)?;

	let mut diffs = finish_tumbling_engine(
		&operator.core,
		txn,
		&operator.row_number_provider,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&kinds,
		operator.engine_config(),
		operator.grace(),
		!operator.is_count_based(),
	)?;

	let ts_nanos = change.changed_at.to_nanos();
	{
		let mut store = FlowWindowStore::new(txn, operator.core.node, &operator.row_number_provider);
		for (hash, session_id) in &closes {
			let key = operator.core.create_window_key(*hash, *session_id);
			let (row_number, _) = store.get_or_create_row_number(&key)?;
			let accumulator_key = WindowStateKey(row_number).into_encoded_key();
			let meta_key = operator.core.create_engine_meta_key(*hash, *session_id);
			let prior_last =
				store.state_get::<EngineWindowMeta>(&meta_key)?.map(|m| m.last_event_time).unwrap_or(0);
			reindex_window(
				&mut store,
				hash,
				*session_id,
				row_number,
				(prior_last > 0).then_some(prior_last),
				None,
			)?;
			if let Some(accumulator) = store.internal_get::<RowAccumulator>(&accumulator_key)?
				&& let Some(value) = accumulator.finalize()
			{
				let gvals = group_values.get(hash).cloned().unwrap_or_default();
				let row = operator.core.build_engine_row(&gvals, &value, row_number, ts_nanos)?;
				diffs.push(Diff::remove(Columns::from_row(&row)));
			}
			store.internal_drop(&accumulator_key)?;
			store.state_drop(&meta_key)?;
		}
	}

	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

fn gate_sealed_buckets(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	buckets: &mut EngineBuckets,
	arrival: &mut Vec<(Hash128, WindowSpan<u64>)>,
	window_max_ts: &HashMap<(Hash128, WindowSpan<u64>), u64>,
	cutoff_ms: u64,
) -> Result<()> {
	if cutoff_ms == 0 || operator.is_count_based() || operator.kind.time() != TimeDomain::Event {
		return Ok(());
	}
	let watermark = operator.load_expiry_watermark(txn)?;
	let mut sealed: Vec<(Hash128, WindowSpan<u64>)> = Vec::new();
	let mut dropped = 0u64;
	{
		let mut store = FlowWindowStore::new(txn, operator.core.node, &operator.row_number_provider);
		for (key, events) in buckets.iter() {
			let (hash, span) = key;
			let meta_key = operator.core.create_engine_meta_key(*hash, span.start);
			let prior_last =
				store.state_get::<EngineWindowMeta>(&meta_key)?.map(|m| m.last_event_time).unwrap_or(0);
			let batch_last = window_max_ts.get(key).copied().unwrap_or(0);
			let last = prior_last.max(batch_last);
			if last > 0 && watermark.saturating_sub(last) > cutoff_ms {
				dropped += events.len() as u64;
				sealed.push(*key);
			}
		}
	}
	if sealed.is_empty() {
		return Ok(());
	}
	for key in &sealed {
		buckets.remove(key);
	}
	let sealed: HashSet<(Hash128, WindowSpan<u64>)> = sealed.into_iter().collect();
	arrival.retain(|key| !sealed.contains(key));
	operator.note_sealed_drops(dropped);
	Ok(())
}

#[tracing::instrument(name = "flow::window::tick_expire", level = "debug", skip_all, fields(node = operator.core.node.0, expired = tracing::field::Empty))]
fn tick_expire_by_cutoff(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	current_timestamp: u64,
	cutoff_ms: u64,
) -> Result<Vec<Diff>> {
	if cutoff_ms == 0 {
		return Ok(Vec::new());
	}
	let ts_nanos = current_timestamp.saturating_mul(1_000_000);
	let effective_now = match operator.kind.time() {
		TimeDomain::Event => operator.load_event_watermark(txn)?,
		TimeDomain::Processing => current_timestamp,
	};
	if operator.kind.time() == TimeDomain::Event {
		operator.advance_expiry_watermark(txn, effective_now)?;
	}
	let threshold = effective_now.saturating_sub(cutoff_ms).saturating_sub(1);
	let expired = {
		let mut store = FlowWindowStore::new(txn, operator.core.node, &operator.row_number_provider);
		let mut engine = TumblingEngine::<Hash128, u64, RowAccumulator>::new(operator.engine_config());
		let res = engine.expire(&mut store, threshold)?;
		engine.flush(&mut store)?;
		res
	};
	warn_when_expiry_capped(operator, expired.len());
	Span::current().record("expired", expired.len());
	let mut diffs = Vec::new();
	let mut store = FlowWindowStore::new(txn, operator.core.node, &operator.row_number_provider);
	for window in expired {
		let ewm_key = operator.core.create_engine_meta_key(window.group, window.window_start);
		if let Some(value) = window.value {
			let gvals = store
				.state_get::<EngineWindowMeta>(&ewm_key)?
				.map(|m| m.group_values)
				.unwrap_or_default();
			let row = operator.core.build_engine_row(&gvals, &value, window.row_number, ts_nanos)?;
			diffs.push(Diff::remove(Columns::from_row(&row)));
		}
		store.state_drop(&ewm_key)?;
	}
	Ok(diffs)
}

pub fn tick_expire_session_engine(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	current_timestamp: u64,
) -> Result<Vec<Diff>> {
	tick_expire_by_cutoff(operator, txn, current_timestamp, operator.session_gap_ms() + operator.grace_ms())
}

pub fn tick_expire_engine_windows(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	current_timestamp: u64,
) -> Result<Vec<Diff>> {
	let window_size_ms = match operator.size_duration() {
		Some(d) => d.milliseconds().unwrap_or(0) as u64,
		None => return Ok(Vec::new()),
	};
	tick_expire_by_cutoff(operator, txn, current_timestamp, window_size_ms + operator.grace_ms())
}

#[cfg(test)]
mod tests {
	use reifydb_core::window::engine::{is_sealed, seal_horizon};

	#[test]
	fn gate_and_tick_expiry_agree_on_the_seal_boundary() {
		// The routing gate (gate_sealed_buckets: watermark - last_event_time > cutoff)
		// and the tick expire sweep (expiry index keyed by last_event_time, threshold
		// watermark - cutoff - 1, inclusive) must agree at every watermark, or a window
		// can be swept but then re-created by a late delta - the resurrection
		// divergence. Both are activity-based: spans carry synthetic ids for sliding
		// and session windows, so wall-clock math must never touch span bounds.
		let cutoff = 19u64;
		let last = 10u64;
		let gate_seals = |wm: u64| wm.saturating_sub(last) > cutoff;
		let tick_sweeps = |wm: u64| last <= wm.saturating_sub(cutoff).saturating_sub(1);
		for wm in 0..100u64 {
			assert_eq!(gate_seals(wm), tick_sweeps(wm), "gate and sweep diverge at watermark {wm}");
		}
		assert!(!gate_seals(last + cutoff), "watermark exactly cutoff past the last event is still mutable");
		assert!(gate_seals(last + cutoff + 1), "one past the cutoff is sealed");
	}

	#[test]
	fn seal_horizon_saturates_for_young_watermarks() {
		// A watermark smaller than seal_after must not wrap; nothing is sealed yet.
		assert_eq!(seal_horizon(3, 10), 0, "young watermark saturates to zero horizon");
		assert!(!is_sealed(0, seal_horizon(3, 10)), "anchor zero is not below a zero horizon");
		assert!(is_sealed(4, seal_horizon(20, 10)), "anchor below watermark - seal_after is sealed");
	}
}
