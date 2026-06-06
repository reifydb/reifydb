// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB
use std::collections::{BTreeMap, HashMap, HashSet};

use reifydb_core::{
	common::TimeDomain,
	encoded::key::{EncodedKey, IntoEncodedKey},
	interface::change::{Change, Diff},
	key::{EncodableKey, flow_node_state::FlowNodeStateKey},
	value::column::columns::Columns,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, LatePolicy,
			tumbling::{TumblingBuckets, TumblingEngine},
		},
		span::WindowSpan,
		store::WindowStore,
	},
};
use reifydb_engine::flow::aggregate::SlotKind;
use reifydb_runtime::hash::Hash128;
use reifydb_value::{
	Result,
	value::{Value, row_number::RowNumber},
};
use serde::{Deserialize, Serialize};

use super::{accumulator::RowAccumulator, aggregation::Aggregation, operator::WindowOperator, store::FlowWindowStore};
use crate::transaction::FlowTransaction;

type EngineBuckets = TumblingBuckets<Hash128, u64, Vec<Option<Value>>>;

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
		let contribution = core.build_contribution(columns, &slot_cols, row_idx);
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
	event: AccumulatorEvent<Vec<Option<Value>>>,
	event_ts: u64,
) {
	let now = event_ts;
	let span = WindowSpan::new(window_id, window_id + 1);
	let key = (hash, span);
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
					push_count_event(
						buckets,
						group_values,
						arrival,
						window_max_ts,
						*hash,
						gvals,
						window_id,
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
						push_count_event(
							buckets,
							group_values,
							arrival,
							window_max_ts,
							*hash,
							gvals,
							window_id,
							AccumulatorEvent::Add(contribution),
							now,
						);
					} else {
						let pre_contrib =
							operator.core.build_contribution(pre, &pre_cols, row_idx);
						let post_contrib =
							operator.core.build_contribution(post, &post_cols, row_idx);
						for window_id in existing {
							push_count_event(
								buckets,
								group_values,
								arrival,
								window_max_ts,
								*hash,
								gvals,
								window_id,
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
	change: &Change,
	buckets: EngineBuckets,
	group_values: &HashMap<Hash128, Vec<Value>>,
	arrival: Vec<(Hash128, WindowSpan<u64>)>,
	window_max_ts: HashMap<(Hash128, WindowSpan<u64>), u64>,
	kinds: &[SlotKind],
	late_policy: LatePolicy,
) -> Result<Vec<Diff>> {
	let results = {
		let mut store = FlowWindowStore::new(txn, core.node);
		for (hash, span) in &arrival {
			let key = core.create_window_key(*hash, span.start);
			store.get_or_create_row_number(&key)?;
		}
		let mut engine = TumblingEngine::<Hash128, u64, RowAccumulator>::with_late_policy(late_policy);
		let res = engine.apply(
			&mut store,
			buckets,
			|hash, window_start| core.create_window_key(*hash, window_start),
			|| RowAccumulator::new(kinds),
		)?;
		engine.flush(&mut store)?;
		res
	};

	{
		let mut store = FlowWindowStore::new(txn, core.node);
		for r in &results {
			let ewm_key = core.create_engine_meta_key(r.group, r.span.start);
			match r.kind {
				EmitKind::Remove => store.state_remove(&ewm_key)?,
				EmitKind::Insert | EmitKind::Update => {
					let batch_max = window_max_ts.get(&(r.group, r.span)).copied().unwrap_or(0);
					let prior_last = store
						.state_get::<EngineWindowMeta>(&ewm_key)?
						.map(|m| m.last_event_time)
						.unwrap_or(0);
					let meta = EngineWindowMeta {
						group_hash: r.group.0,
						window_start: r.span.start,
						row_number: r.row_number.0,
						last_event_time: prior_last.max(batch_max),
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

	gate_closed_buckets(operator, txn, &mut buckets, &mut arrival, &window_max_ts, window_size_ms)?;

	let diffs = finish_tumbling_engine(
		&operator.core,
		txn,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&kinds,
		operator.late_policy,
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
					for wid in operator.lookup_row_index(txn, *hash, pre.row_numbers[row_idx])? {
						push_count_event(
							&mut buckets,
							&mut group_values,
							&mut arrival,
							&mut window_max_ts,
							*hash,
							gvals,
							wid,
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
								AccumulatorEvent::Add(contribution.clone()),
								event_ts,
							);
						}
					} else {
						let pre_contrib =
							operator.core.build_contribution(pre, &pre_cols, row_idx);
						let post_contrib =
							operator.core.build_contribution(post, &post_cols, row_idx);
						for wid in existing {
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								wid,
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

	let window_size_ms = operator.size_duration().map(|d| d.milliseconds().unwrap_or(0) as u64).unwrap_or(0);
	gate_closed_buckets(operator, txn, &mut buckets, &mut arrival, &window_max_ts, window_size_ms)?;

	let diffs = finish_tumbling_engine(
		&operator.core,
		txn,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&kinds,
		operator.late_policy,
	)?;
	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

fn session_assign(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	hash: Hash128,
	event_ts: u64,
	gap_ms: u64,
	trackers: &mut HashMap<Hash128, (u64, u64)>,
	closes: &mut Vec<(Hash128, u64)>,
) -> Result<u64> {
	let (mut session_id, last) = match trackers.get(&hash) {
		Some(&tracker) => tracker,
		None => {
			let tracker = operator.load_session_tracker(txn, hash)?;
			trackers.insert(hash, tracker);
			tracker
		}
	};
	if last > 0 && event_ts.saturating_sub(last) > gap_ms {
		closes.push((hash, session_id));
		session_id += 1;
	}
	trackers.insert(hash, (session_id, event_ts));
	Ok(session_id)
}

pub fn apply_session_engine(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let gap_ms = operator.session_gap_ms();

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<u64>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<u64>), u64> = HashMap::new();
	let mut closes: Vec<(Hash128, u64)> = Vec::new();
	let mut trackers: HashMap<Hash128, (u64, u64)> = HashMap::new();

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
					let session_id = session_assign(
						operator,
						txn,
						*hash,
						event_ts,
						gap_ms,
						&mut trackers,
						&mut closes,
					)?;
					operator.store_row_index(txn, *hash, post.row_numbers[row_idx], session_id)?;
					let contribution = operator.core.build_contribution(post, &slot_cols, row_idx);
					push_count_event(
						&mut buckets,
						&mut group_values,
						&mut arrival,
						&mut window_max_ts,
						*hash,
						gvals,
						session_id,
						AccumulatorEvent::Add(contribution),
						event_ts,
					);
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
						let session_id = session_assign(
							operator,
							txn,
							*hash,
							event_ts,
							gap_ms,
							&mut trackers,
							&mut closes,
						)?;
						operator.store_row_index(
							txn,
							*hash,
							post.row_numbers[row_idx],
							session_id,
						)?;
						let contribution =
							operator.core.build_contribution(post, &post_cols, row_idx);
						push_count_event(
							&mut buckets,
							&mut group_values,
							&mut arrival,
							&mut window_max_ts,
							*hash,
							gvals,
							session_id,
							AccumulatorEvent::Add(contribution),
							event_ts,
						);
					} else {
						let pre_contrib =
							operator.core.build_contribution(pre, &pre_cols, row_idx);
						let post_contrib =
							operator.core.build_contribution(post, &post_cols, row_idx);
						for session_id in existing {
							push_count_event(
								&mut buckets,
								&mut group_values,
								&mut arrival,
								&mut window_max_ts,
								*hash,
								gvals,
								session_id,
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
								AccumulatorEvent::Add(post_contrib.clone()),
								event_ts,
							);
						}
					}
				}
			}
		}
	}

	for (hash, (session_id, last)) in &trackers {
		operator.save_session_tracker(txn, *hash, *session_id, *last)?;
	}

	if operator.kind.time() == TimeDomain::Event
		&& !operator.is_count_based()
		&& let Some(batch_max) = window_max_ts.values().copied().max()
	{
		operator.advance_event_watermark(txn, batch_max)?;
	}

	gate_closed_buckets(operator, txn, &mut buckets, &mut arrival, &window_max_ts, gap_ms)?;

	let mut diffs = finish_tumbling_engine(
		&operator.core,
		txn,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&kinds,
		operator.late_policy,
	)?;

	let ts_nanos = change.changed_at.to_nanos();
	{
		let mut store = FlowWindowStore::new(txn, operator.core.node);
		for (hash, session_id) in &closes {
			let key = operator.core.create_window_key(*hash, *session_id);
			let (row_number, _) = store.get_or_create_row_number(&key)?;
			let accumulator_key = row_number.into_encoded_key();
			if let Some(accumulator) = store.state_get::<RowAccumulator>(&accumulator_key)?
				&& let Some(value) = accumulator.finalize()
			{
				let gvals = group_values.get(hash).cloned().unwrap_or_default();
				let row = operator.core.build_engine_row(&gvals, &value, row_number, ts_nanos)?;
				diffs.push(Diff::remove(Columns::from_row(&row)));
			}
			store.state_remove(&accumulator_key)?;
			store.state_remove(&operator.core.create_engine_meta_key(*hash, *session_id))?;
		}
	}

	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

pub(super) fn window_closed(watermark: u64, last_event_time: u64, cutoff_ms: u64) -> bool {
	watermark.saturating_sub(last_event_time) > cutoff_ms
}

fn gate_closed_buckets(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	buckets: &mut EngineBuckets,
	arrival: &mut Vec<(Hash128, WindowSpan<u64>)>,
	window_max_ts: &HashMap<(Hash128, WindowSpan<u64>), u64>,
	cutoff_ms: u64,
) -> Result<()> {
	if cutoff_ms == 0 || operator.kind.time() != TimeDomain::Event || operator.is_count_based() {
		return Ok(());
	}
	let watermark = operator.load_event_watermark(txn)?;
	let mut closed: Vec<(Hash128, WindowSpan<u64>)> = Vec::new();
	{
		let mut store = FlowWindowStore::new(txn, operator.core.node);
		for key in buckets.keys() {
			let (hash, span) = key;
			let meta_key = operator.core.create_engine_meta_key(*hash, span.start);
			let prior_last =
				store.state_get::<EngineWindowMeta>(&meta_key)?.map(|m| m.last_event_time).unwrap_or(0);
			let batch_last = window_max_ts.get(key).copied().unwrap_or(0);
			if window_closed(watermark, prior_last.max(batch_last), cutoff_ms) {
				closed.push(*key);
			}
		}
	}
	if closed.is_empty() {
		return Ok(());
	}
	for key in &closed {
		buckets.remove(key);
	}
	let closed: HashSet<(Hash128, WindowSpan<u64>)> = closed.into_iter().collect();
	arrival.retain(|key| !closed.contains(key));
	Ok(())
}

fn tick_expire_by_cutoff(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	current_timestamp: u64,
	cutoff_ms: u64,
) -> Result<Vec<Diff>> {
	if cutoff_ms == 0 {
		return Ok(Vec::new());
	}
	let meta_keys = scan_meta_keys(operator, txn, b"ewm:")?;
	let ts_nanos = current_timestamp.saturating_mul(1_000_000);
	let effective_now = match operator.kind.time() {
		TimeDomain::Event => operator.load_event_watermark(txn)?,
		TimeDomain::Processing => current_timestamp,
	};
	let mut diffs = Vec::new();
	let mut store = FlowWindowStore::new(txn, operator.core.node);
	for meta_key in &meta_keys {
		let Some(meta) = store.state_get::<EngineWindowMeta>(meta_key)? else {
			continue;
		};
		if meta.last_event_time == 0 {
			continue;
		}
		if !window_closed(effective_now, meta.last_event_time, cutoff_ms) {
			continue;
		}
		let row_number = RowNumber(meta.row_number);
		let accumulator_key = row_number.into_encoded_key();
		if let Some(accumulator) = store.state_get::<RowAccumulator>(&accumulator_key)?
			&& let Some(value) = accumulator.finalize()
		{
			let row = operator.core.build_engine_row(&meta.group_values, &value, row_number, ts_nanos)?;
			diffs.push(Diff::remove(Columns::from_row(&row)));
		}
		store.state_remove(&accumulator_key)?;
		store.state_remove(meta_key)?;
	}
	Ok(diffs)
}

pub fn tick_expire_session_engine(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	current_timestamp: u64,
) -> Result<Vec<Diff>> {
	tick_expire_by_cutoff(operator, txn, current_timestamp, operator.session_gap_ms())
}

pub(super) fn scan_meta_keys(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	tag: &[u8],
) -> Result<Vec<EncodedKey>> {
	let all_state = txn.state_scan_all(operator.core.node)?;
	let prefix = FlowNodeStateKey::new(operator.core.node, vec![]).encode();
	let mut keys = Vec::new();
	for item in &all_state.items {
		let full_key = &item.key;
		if full_key.len() <= prefix.len() {
			continue;
		}
		let inner = &full_key[prefix.len()..];
		if inner.starts_with(tag) {
			keys.push(EncodedKey::new(inner));
		}
	}
	Ok(keys)
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
	tick_expire_by_cutoff(operator, txn, current_timestamp, window_size_ms)
}

#[cfg(test)]
mod tests {
	use super::window_closed;

	#[test]
	fn closed_only_strictly_beyond_cutoff() {
		// The routing gate and tick_expire_by_cutoff share this predicate; if they disagree
		// (e.g. one uses > and the other >=) a window can be expired but then re-created by a
		// later delta, which is exactly the resurrection divergence this fix removes.
		assert!(!window_closed(14, 14, 5), "watermark at last event time is open");
		assert!(!window_closed(19, 14, 5), "exactly cutoff behind is still open");
		assert!(window_closed(20, 14, 5), "one past the cutoff is closed");
	}

	#[test]
	fn out_of_order_event_does_not_underflow() {
		// Event time behind the watermark must not panic or wrap; it is simply not closed by
		// virtue of a saturating distance of zero.
		assert!(!window_closed(3, 10, 5), "event ahead of watermark saturates to open");
	}
}
