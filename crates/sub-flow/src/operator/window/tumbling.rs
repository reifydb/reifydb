// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use std::collections::{BTreeMap, HashMap, HashSet};

use reifydb_abi::operator::timer::TimerKind;
use reifydb_codec::{
	key::{encode_u64_asc, encode_u128_asc, encoded::EncodedKey},
	state::decode_state,
};
use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	key::operator_state::{GroupId, IntoStateKey},
	state::store::StateStore,
	value::column::columns::Columns,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, WindowStateKey,
			config::WindowEngineConfig,
			tumbling::{TumblingBuckets, TumblingEngine},
		},
		span::WindowSpan,
	},
};
use reifydb_engine::flow::aggregate::SlotKind;
use reifydb_flow::transaction::{FlowTransaction, timer::Timer};
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, duration::Duration},
};
use tracing::Span;

use super::{
	accumulator::{RowAccumulator, WindowSlotKey},
	aggregation::Aggregation,
	aux::{EngineMeta, EngineMetaKey},
	operator::WindowOperator,
};
use crate::operator::{stateful::utils, store::OperatorStateStore, window::warn_when_expiry_capped};

type EngineBuckets = TumblingBuckets<Hash128, u64, (WindowSlotKey, Vec<Option<Value>>)>;

pub(super) type WindowGroups = HashMap<(Hash128, u64), GroupId>;

const WINDOW_GROUP: u8 = 0x00;
const PARTITION_GROUP: u8 = 0x01;

pub(super) fn seal_instant(last_event_time: u64, cutoff_ms: u64) -> u64 {
	last_event_time.saturating_add(cutoff_ms).saturating_add(1)
}

pub(super) fn window_group_key(partition: Hash128, window_id: u64) -> EncodedKey {
	let mut bytes = Vec::with_capacity(1 + 16 + 8);
	bytes.push(WINDOW_GROUP);
	bytes.extend_from_slice(&encode_u128_asc(partition.0));
	bytes.extend_from_slice(&encode_u64_asc(window_id));
	EncodedKey::new(bytes)
}

pub(super) fn partition_group_key(partition: Hash128) -> EncodedKey {
	let mut bytes = Vec::with_capacity(1 + 16);
	bytes.push(PARTITION_GROUP);
	bytes.extend_from_slice(&encode_u128_asc(partition.0));
	EncodedKey::new(bytes)
}

pub(super) fn intern_window_groups(
	node: FlowNodeId,
	txn: &mut FlowTransaction,
	windows: &[(Hash128, u64)],
) -> Result<WindowGroups> {
	if windows.is_empty() {
		return Ok(WindowGroups::new());
	}
	let keys: Vec<EncodedKey> = windows.iter().map(|(p, w)| window_group_key(*p, *w)).collect();
	let interned = txn.intern_groups(node, &keys)?;
	Ok(windows.iter().copied().zip(interned.into_iter().map(|(id, _)| id)).collect())
}

pub(super) fn group_of(groups: &WindowGroups, partition: Hash128, window_id: u64) -> GroupId {
	*groups.get(&(partition, window_id)).expect("every routed window is interned before the engine runs")
}

pub(super) fn slot_coord(is_count: bool, event_ts: u64, row_number: u64) -> WindowSlotKey {
	let timestamp = if is_count {
		DateTime::default()
	} else {
		DateTime::from_timestamp_millis(event_ts).unwrap_or_default()
	};
	WindowSlotKey::new(timestamp, row_number)
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
		let coord = slot_coord(false, event_ts, columns.row_numbers()[row_idx].0);
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
					operator.store_row_index(txn, *hash, post.row_numbers()[row_idx], window_id)?;
					let contribution = operator.core.build_contribution(post, &slot_cols, row_idx);
					let coord = slot_coord(true, now, post.row_numbers()[row_idx].0);
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
					let coord = slot_coord(true, now, pre.row_numbers()[row_idx].0);
					for window_id in
						operator.lookup_row_index(txn, *hash, pre.row_numbers()[row_idx])?
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
					let row_number = pre.row_numbers()[row_idx];
					let existing = operator.lookup_row_index(txn, *hash, row_number)?;
					if existing.is_empty() {
						let ordinal = operator.get_and_increment_global_count(txn, *hash)?;
						let window_id = ordinal / size;
						operator.store_row_index(
							txn,
							*hash,
							post.row_numbers()[row_idx],
							window_id,
						)?;
						let contribution =
							operator.core.build_contribution(post, &post_cols, row_idx);
						let coord = slot_coord(true, now, post.row_numbers()[row_idx].0);
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
						let coord = slot_coord(true, now, pre.row_numbers()[row_idx].0);
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
	change: &Change,
	buckets: EngineBuckets,
	group_values: &HashMap<Hash128, Vec<Value>>,
	arrival: Vec<(Hash128, WindowSpan<u64>)>,
	window_max_ts: HashMap<(Hash128, WindowSpan<u64>), u64>,
	groups: &WindowGroups,
	kinds: &[SlotKind],
	engine_config: WindowEngineConfig,
	grace: Duration,
	index: bool,
) -> Result<Vec<Diff>> {
	let mut engine = core.tumbling_engine_slot().take().unwrap_or_else(|| {
		Box::new(TumblingEngine::<Hash128, u64, RowAccumulator>::group_scoped(engine_config))
	});
	let results = {
		let mut store = OperatorStateStore::new(txn, core.node);
		let res = engine.apply(
			&mut store,
			buckets,
			&arrival,
			|hash, window_start| (group_of(groups, *hash, window_start), utils::empty_key()),
			|| RowAccumulator::new(kinds, grace),
		)?;
		engine.flush(&mut store)?;
		res
	};

	{
		let mut store = OperatorStateStore::new(txn, core.node);
		for r in &results {
			let group = group_of(groups, r.group, r.span.start);
			let prior_last = core
				.engine_meta()
				.get(&mut store, &EngineMetaKey(group))?
				.map(|m| m.last_event_time)
				.unwrap_or(0);
			match r.kind {
				EmitKind::Remove => {
					if index {
						engine.reindex_window(
							&mut store,
							&r.group,
							r.span.start,
							group,
							r.row_number,
							(prior_last > 0).then_some(prior_last),
							None,
						)?;
					}
					core.engine_meta().remove(&mut store, &EngineMetaKey(group))?;
				}
				EmitKind::Insert | EmitKind::Update => {
					let batch_max = window_max_ts.get(&(r.group, r.span)).copied().unwrap_or(0);
					let last_event_time = prior_last.max(batch_max);
					if index {
						engine.reindex_window(
							&mut store,
							&r.group,
							r.span.start,
							group,
							r.row_number,
							(prior_last > 0).then_some(prior_last),
							(last_event_time > 0).then_some(last_event_time),
						)?;
					}
					let meta = EngineMeta {
						group_hash: r.group.0,
						window_start: r.span.start,
						row_number: r.row_number.0,
						last_event_time,
						group_values: group_values.get(&r.group).cloned().unwrap_or_default(),
					};
					core.engine_meta().put(&mut store, &EngineMetaKey(group), meta)?;
				}
			}
		}
	}
	*core.tumbling_engine_slot() = Some(engine);

	let ts_nanos = change.changed_at.to_nanos();
	let mut diffs = Vec::new();
	for r in results {
		let gvals = group_values.get(&r.group).cloned().unwrap_or_default();
		match r.kind {
			EmitKind::Insert => {
				let row = core.build_engine_row(
					&gvals,
					&r.value,
					r.row_number,
					ts_nanos,
					bucket_start_nanos(r.span.start),
				)?;
				diffs.push(Diff::insert(Columns::from_row(&row)));
			}
			EmitKind::Update => {
				let pre_vals: &[Value] = r.prior.as_deref().unwrap_or(&r.value);
				let pre = core.build_engine_row(
					&gvals,
					pre_vals,
					r.row_number,
					ts_nanos,
					bucket_start_nanos(r.span.start),
				)?;
				let post = core.build_engine_row(
					&gvals,
					&r.value,
					r.row_number,
					ts_nanos,
					bucket_start_nanos(r.span.start),
				)?;
				diffs.push(Diff::update(Columns::from_row(&pre), Columns::from_row(&post)));
			}
			EmitKind::Remove => {
				let pre_vals: &[Value] = r.prior.as_deref().unwrap_or(&r.value);
				let pre = core.build_engine_row(
					&gvals,
					pre_vals,
					r.row_number,
					ts_nanos,
					bucket_start_nanos(r.span.start),
				)?;
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

	gate_and_arm_seals(
		operator,
		txn,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		window_size_ms + operator.grace_ms(),
	)?;

	let groups = intern_batch(operator, txn, &arrival)?;

	let diffs = finish_tumbling_engine(
		&operator.core,
		txn,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&groups,
		&kinds,
		operator.engine_config(),
		operator.grace(),
		!operator.is_count_based(),
	)?;
	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

fn intern_batch(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	arrival: &[(Hash128, WindowSpan<u64>)],
) -> Result<WindowGroups> {
	let windows: Vec<(Hash128, u64)> = arrival.iter().map(|(hash, span)| (*hash, span.start)).collect();
	intern_window_groups(operator.core.node, txn, &windows)
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
	let is_event = operator.core.ctx.time.is_event();
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
					let coord = slot_coord(is_count, event_ts, post.row_numbers()[row_idx].0);
					for wid in &window_ids {
						operator.store_row_index(
							txn,
							*hash,
							post.row_numbers()[row_idx],
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
					let coord = slot_coord(is_count, event_ts, pre.row_numbers()[row_idx].0);
					for wid in operator.lookup_row_index(txn, *hash, pre.row_numbers()[row_idx])? {
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
					let row_number = pre.row_numbers()[row_idx];
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
								post.row_numbers()[row_idx],
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

	gate_and_arm_seals(
		operator,
		txn,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		window_size_ms + operator.grace_ms(),
	)?;

	let groups = intern_batch(operator, txn, &arrival)?;

	let diffs = finish_tumbling_engine(
		&operator.core,
		txn,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&groups,
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
							post.row_numbers()[row_idx],
							session_id,
						)?;
						let contribution =
							operator.core.build_contribution(post, &slot_cols, row_idx);
						let coord = slot_coord(false, event_ts, post.row_numbers()[row_idx].0);
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
					let coord = slot_coord(false, event_ts, pre.row_numbers()[row_idx].0);
					for session_id in
						operator.lookup_row_index(txn, *hash, pre.row_numbers()[row_idx])?
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
						operator.lookup_row_index(txn, *hash, pre.row_numbers()[row_idx])?;
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
								post.row_numbers()[row_idx],
								session_id,
							)?;
							let contribution = operator
								.core
								.build_contribution(post, &post_cols, row_idx);
							let coord = slot_coord(
								false,
								event_ts,
								post.row_numbers()[row_idx].0,
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
						let coord = slot_coord(false, event_ts, pre.row_numbers()[row_idx].0);
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

	gate_and_arm_seals(
		operator,
		txn,
		&mut buckets,
		&mut arrival,
		&window_max_ts,
		operator.session_gap_ms() + operator.grace_ms(),
	)?;

	let groups = intern_batch(operator, txn, &arrival)?;

	let mut diffs = finish_tumbling_engine(
		&operator.core,
		txn,
		&change,
		buckets,
		&group_values,
		arrival,
		window_max_ts,
		&groups,
		&kinds,
		operator.engine_config(),
		operator.grace(),
		!operator.is_count_based(),
	)?;

	let ts_nanos = change.changed_at.to_nanos();
	let mut disarm: Vec<(Hash128, u64, u64)> = Vec::new();
	{
		let node = operator.core.node;
		let mut closing: Vec<(Hash128, u64, GroupId)> = Vec::with_capacity(closes.len());
		for (hash, session_id) in &closes {
			if let Some(group) = txn.lookup_group(node, &window_group_key(*hash, *session_id))? {
				closing.push((*hash, *session_id, group));
			}
		}
		let mut engine = operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
			Box::new(TumblingEngine::<Hash128, u64, RowAccumulator>::group_scoped(operator.engine_config()))
		});
		let mut store = OperatorStateStore::new(txn, node);
		for (hash, session_id, group) in &closing {
			let (row_number, _) = store.get_or_create_row_number(*group, &utils::empty_key())?;
			let accumulator_key = WindowStateKey::new(*group, row_number).into_state_key();
			let meta = operator.core.engine_meta().get(&mut store, &EngineMetaKey(*group))?;
			let prior_last = meta.as_ref().map(|m| m.last_event_time).unwrap_or(0);
			let window_start = meta.map(|m| m.window_start).unwrap_or(0);
			if prior_last > 0 {
				disarm.push((*hash, *session_id, prior_last));
			}
			engine.reindex_window(
				&mut store,
				hash,
				*session_id,
				*group,
				row_number,
				(prior_last > 0).then_some(prior_last),
				None,
			)?;
			if let Some(accumulator) = store
				.state_get(&accumulator_key)?
				.map(|b| decode_state::<RowAccumulator>(&b))
				.transpose()? && let Some(value) = accumulator.finalize()
			{
				let gvals = group_values.get(hash).cloned().unwrap_or_default();
				let row = operator.core.build_engine_row(
					&gvals,
					&value,
					row_number,
					ts_nanos,
					bucket_start_nanos(window_start),
				)?;
				diffs.push(Diff::remove(Columns::from_row(&row)));
			}
			store.state_remove(&accumulator_key)?;
			operator.core.engine_meta().remove(&mut store, &EngineMetaKey(*group))?;
		}
		*operator.core.tumbling_engine_slot() = Some(engine);
	}

	if !operator.is_count_based() {
		let node = operator.core.node;
		let cutoff_ms = operator.session_gap_ms() + operator.grace_ms();
		for (hash, session_id, prior_last) in disarm {
			txn.disarm_timer(
				node,
				&Timer {
					at: DateTime::from_millis(seal_instant(prior_last, cutoff_ms)),
					kind: TimerKind::Seal,
					key: window_group_key(hash, session_id),
				},
			)?;
		}
	}

	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

fn gate_and_arm_seals(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	buckets: &mut EngineBuckets,
	arrival: &mut Vec<(Hash128, WindowSpan<u64>)>,
	window_max_ts: &HashMap<(Hash128, WindowSpan<u64>), u64>,
	cutoff_ms: u64,
) -> Result<()> {
	if cutoff_ms == 0 || operator.is_count_based() {
		return Ok(());
	}
	let ledger = operator.seal_ledger(txn)?;
	let node = operator.core.node;
	let mut known: Vec<Option<GroupId>> = Vec::with_capacity(buckets.len());
	for (hash, span) in buckets.keys() {
		known.push(txn.lookup_group(node, &window_group_key(*hash, span.start))?);
	}
	let mut sealed: Vec<(Hash128, WindowSpan<u64>)> = Vec::new();
	let mut rearm: Vec<(Hash128, u64, u64, u64)> = Vec::new();
	let mut dropped = 0u64;
	{
		let mut store = OperatorStateStore::new(txn, node);
		for ((key, events), group) in buckets.iter().zip(known) {
			let prior_last = match group {
				Some(group) => operator
					.core
					.engine_meta()
					.get(&mut store, &EngineMetaKey(group))?
					.map(|m| m.last_event_time)
					.unwrap_or(0),
				None => 0,
			};
			let batch_last = window_max_ts.get(key).copied().unwrap_or(0);
			let last = prior_last.max(batch_last);
			if last == 0 {
				continue;
			}
			if seal_instant(last, cutoff_ms) <= ledger {
				dropped += events.len() as u64;
				sealed.push(*key);
			} else {
				rearm.push((key.0, key.1.start, prior_last, last));
			}
		}
	}

	for (hash, window_start, prior_last, last) in rearm {
		let key = window_group_key(hash, window_start);
		if prior_last > 0 && seal_instant(prior_last, cutoff_ms) != seal_instant(last, cutoff_ms) {
			txn.disarm_timer(
				node,
				&Timer {
					at: DateTime::from_millis(seal_instant(prior_last, cutoff_ms)),
					kind: TimerKind::Seal,
					key: key.clone(),
				},
			)?;
		}
		txn.arm_timer(
			node,
			&Timer {
				at: DateTime::from_millis(seal_instant(last, cutoff_ms)),
				kind: TimerKind::Seal,
				key,
			},
		)?;
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

#[tracing::instrument(name = "flow::window::seal", level = "debug", skip_all, fields(node = operator.core.node.0, expired = tracing::field::Empty))]
fn seal_due_windows(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	at: u64,
	cutoff_ms: u64,
) -> Result<Vec<Diff>> {
	if cutoff_ms == 0 {
		return Ok(Vec::new());
	}
	operator.advance_seal_ledger(txn, at)?;
	let ts_nanos = at.saturating_mul(1_000_000);
	let threshold = at.saturating_sub(cutoff_ms).saturating_sub(1);
	let expired = {
		let mut store = OperatorStateStore::new(txn, operator.core.node);
		let mut engine = operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
			Box::new(TumblingEngine::<Hash128, u64, RowAccumulator>::group_scoped(operator.engine_config()))
		});
		let res = engine.expire(&mut store, threshold)?;
		engine.flush(&mut store)?;
		*operator.core.tumbling_engine_slot() = Some(engine);
		res
	};
	warn_when_expiry_capped(operator, expired.len());
	Span::current().record("expired", expired.len());
	let mut diffs = Vec::new();
	let mut store = OperatorStateStore::new(txn, operator.core.node);
	for window in expired {
		if let Some(value) = window.value {
			let meta = operator.core.engine_meta().get(&mut store, &EngineMetaKey(window.group_id))?;
			let gvals = meta.as_ref().map(|m| m.group_values.clone()).unwrap_or_default();
			let window_start = meta.map(|m| m.window_start).unwrap_or(0);
			let row = operator.core.build_engine_row(
				&gvals,
				&value,
				window.row_number,
				ts_nanos,
				bucket_start_nanos(window_start),
			)?;
			diffs.push(Diff::remove(Columns::from_row(&row)));
		}
		operator.core.engine_meta().remove(&mut store, &EngineMetaKey(window.group_id))?;
		if window.accumulator_present {
			store.remove_row_number(window.group_id, &utils::empty_key())?;
		}
	}
	Ok(diffs)
}

pub fn seal_session_engine(operator: &WindowOperator, txn: &mut FlowTransaction, at: u64) -> Result<Vec<Diff>> {
	seal_due_windows(operator, txn, at, operator.session_gap_ms() + operator.grace_ms())
}

pub fn seal_engine_windows(operator: &WindowOperator, txn: &mut FlowTransaction, at: u64) -> Result<Vec<Diff>> {
	let window_size_ms = match operator.size_duration() {
		Some(d) => d.milliseconds().unwrap_or(0) as u64,
		None => return Ok(Vec::new()),
	};
	seal_due_windows(operator, txn, at, window_size_ms + operator.grace_ms())
}

#[cfg(test)]
mod tests {
	use reifydb_core::window::engine::{is_sealed, seal_horizon};

	use super::seal_instant;

	#[test]
	fn the_armed_seal_instant_reproduces_the_pre_timer_boundary() {
		// One predicate now decides sealing: a bucket is sealed exactly when the instant it
		// armed its Seal timer at has been covered by the seal ledger. That instant and the
		// gate's test must be the same expression, or a bucket is dropped before its timer
		// fires (silent loss) or accepted after it fired (a window rebuilt from a late row
		// alone, emitting a wrong value over the correct one).
		// The boundary itself must not move from the pre-timer implementation, whose gate was
		// strict (watermark - last > cutoff). The wheel fires at watermark >= at, so
		// reproducing a strict boundary needs the +1 that seal_instant carries.
		// Sealing is activity-based, keyed on the last event in the window rather than on the
		// span end: sliding and session spans carry synthetic ids, so span bounds are not a
		// legitimate input here.
		// Mutation: drop the +1 from seal_instant and the equivalence breaks at exactly
		// last + cutoff, where a still-mutable window seals a millisecond early.
		let cutoff = 19u64;
		let last = 10u64;
		let sealed = |wm: u64| seal_instant(last, cutoff) <= wm;
		let pre_timer_gate = |wm: u64| wm.saturating_sub(last) > cutoff;

		for wm in 0..100u64 {
			assert_eq!(
				sealed(wm),
				pre_timer_gate(wm),
				"timer seal diverges from the gate at watermark {wm}"
			);
		}
		assert!(!sealed(last + cutoff), "watermark exactly cutoff past the last event is still mutable");
		assert!(sealed(last + cutoff + 1), "one past the cutoff is sealed");
	}

	#[test]
	fn seal_horizon_saturates_for_young_watermarks() {
		// A watermark smaller than seal_after must not wrap; nothing is sealed yet.
		assert_eq!(seal_horizon(3, 10), 0, "young watermark saturates to zero horizon");
		assert!(!is_sealed(0, seal_horizon(3, 10)), "anchor zero is not below a zero horizon");
		assert!(is_sealed(4, seal_horizon(20, 10)), "anchor below watermark - seal_after is sealed");
	}
}

fn bucket_start_nanos(window_start_ms: u64) -> u64 {
	window_start_ms.saturating_mul(1_000_000)
}

#[cfg(test)]
mod bucket_start_tests {
	use super::*;

	#[test]
	// Intent: THE replay-stability property. A bucketed window stamps #time with the bucket start,
	// which is a pure function of the bucket and therefore independent of which rows arrived, in
	// what order, or how many. Max-contributor would vary with arrival, so two replays of the same
	// corpus would produce different stamps and therefore different retention decisions - which is
	// exactly what decision 4 forbids.
	// Mutation: stamp with a contributor's event time instead and this stops being a function of
	// the window alone.
	fn a_bucket_stamps_the_same_time_regardless_of_what_arrived_in_it() {
		let bucket = 1_700_000_000_000u64;

		assert_eq!(bucket_start_nanos(bucket), 1_700_000_000_000_000_000);
		assert_eq!(
			bucket_start_nanos(bucket),
			bucket_start_nanos(bucket),
			"the stamp depends on the bucket alone, so it cannot vary between two runs"
		);
	}

	#[test]
	// Intent: distinct buckets must get distinct stamps, or a chained rollup (1s -> 1m) would
	// collapse every source bucket onto one instant and the downstream window could not separate
	// them.
	fn adjacent_buckets_get_distinct_stamps_in_bucket_order() {
		let first = bucket_start_nanos(1_700_000_000_000);
		let second = bucket_start_nanos(1_700_000_001_000);

		assert!(first < second, "bucket order must survive into #time");
		assert_eq!(second - first, 1_000_000_000, "a 1s bucket step is 1s in #time");
	}

	#[test]
	// Intent: the conversion is ms -> ns and must not overflow into a wrapped, tiny stamp for a
	// far-future bucket. A wrapped stamp would look ancient and be evicted immediately.
	fn a_far_future_bucket_saturates_rather_than_wrapping() {
		assert_eq!(bucket_start_nanos(u64::MAX), u64::MAX);
		assert!(bucket_start_nanos(u64::MAX) > bucket_start_nanos(1_700_000_000_000));
	}

	#[test]
	// Intent: the epoch bucket maps to the epoch instant, so an unset window_start cannot be
	// mistaken for a real time far from zero.
	fn the_zero_bucket_maps_to_the_epoch() {
		assert_eq!(bucket_start_nanos(0), 0);
	}
}
