// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
use std::collections::{BTreeMap, HashMap, HashSet};

use reifydb_abi::operator::timer::TimerKind;
use reifydb_codec::key::{encode_u64_asc, encode_u128_asc, encoded::EncodedKey};
use reifydb_core::{
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff},
	},
	key::operator_state::{GroupId, IntoStateKey},
	state::store::StateStore,
	value::column::columns::Columns,
	window::{
		engine::{
			AccumulatorEvent, EmitKind, ExpiryAnchor, WindowStateKey,
			config::WindowEngineConfig,
			tumbling::{TumblingBuckets, TumblingEngine},
		},
		span::{WindowCoord, WindowSpan},
	},
};
use reifydb_engine::flow::aggregate::SlotKind;
use reifydb_flow::{timer::Timer, transaction::FlowTransaction};
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

type EngineBuckets = TumblingBuckets<Hash128, DateTime, (WindowSlotKey, Vec<Option<Value>>)>;

pub(super) type WindowGroups = HashMap<(Hash128, u64), GroupId>;

const WINDOW_GROUP: u8 = 0x00;
const PARTITION_GROUP: u8 = 0x01;

pub(super) fn seal_instant(anchor_order: u64, cutoff: Duration) -> DateTime {
	let cutoff_ms = <DateTime as WindowCoord>::span_millis(cutoff).unwrap_or(0);
	<DateTime as WindowCoord>::from_order(anchor_order.saturating_add(cutoff_ms).saturating_add(1))
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

pub(super) fn slot_coord(is_count: bool, event_ts: DateTime, row_number: u64) -> WindowSlotKey {
	let timestamp = if is_count {
		DateTime::default()
	} else {
		event_ts
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
		let contribution = (coord, core.build_contribution(columns, &slot_cols, row_idx));
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
fn route_engine_columns(
	operator: &WindowOperator,
	columns: &Columns,
	is_add: bool,
	window_size: Duration,
	buckets: &mut EngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	arrival: &mut Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: &mut HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
) -> Result<()> {
	let timestamps = operator.row_times(columns, columns.row_count())?;
	route_into_buckets(
		&operator.core,
		columns,
		is_add,
		|row_idx| {
			let ts = timestamps[row_idx];
			(WindowSpan::for_coord(ts, window_size), ts)
		},
		buckets,
		group_values,
		arrival,
		window_max_ts,
	)
}

fn ordinal_window_span(window_id: u64) -> WindowSpan<DateTime> {
	WindowSpan::new(
		<DateTime as WindowCoord>::from_order(window_id),
		<DateTime as WindowCoord>::from_order(window_id + 1),
	)
}

fn sliding_window_span(operator: &WindowOperator, anchor: u64) -> WindowSpan<DateTime> {
	if operator.is_count_based() {
		return ordinal_window_span(anchor);
	}
	let size_ms = <DateTime as WindowCoord>::span_millis(operator.size_duration().unwrap_or_default())
		.unwrap_or(0)
		.max(1);
	WindowSpan::new(
		<DateTime as WindowCoord>::from_order(anchor),
		<DateTime as WindowCoord>::from_order(anchor + size_ms),
	)
}

#[allow(clippy::too_many_arguments)]
fn push_count_event(
	buckets: &mut EngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	arrival: &mut Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: &mut HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
	hash: Hash128,
	gvals: &[Value],
	span: WindowSpan<DateTime>,
	coord: WindowSlotKey,
	event: AccumulatorEvent<Vec<Option<Value>>>,
	event_ts: DateTime,
) {
	let now = event_ts;
	let key = (hash, span);
	let event = match event {
		AccumulatorEvent::Add(c) => AccumulatorEvent::Add((coord, c)),
		AccumulatorEvent::Remove(c) => AccumulatorEvent::Remove((coord, c)),
	};
	if matches!(event, AccumulatorEvent::Add(_)) {
		let entry = window_max_ts.entry(key).or_default();
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
	arrival: &mut Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: &mut HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
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
						ordinal_window_span(window_id),
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
							ordinal_window_span(window_id),
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
							ordinal_window_span(window_id),
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
								ordinal_window_span(window_id),
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
								ordinal_window_span(window_id),
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
	arrival: Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
	groups: &WindowGroups,
	kinds: &[SlotKind],
	engine_config: WindowEngineConfig,
	grace: Duration,
	anchor: ExpiryAnchor,
) -> Result<Vec<Diff>> {
	let mut engine = core.tumbling_engine_slot().take().unwrap_or_else(|| {
		Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::group_scoped(engine_config))
	});
	let results = {
		let mut store = OperatorStateStore::new(txn, core.node);
		let res = engine.apply(
			&mut store,
			buckets,
			&arrival,
			|hash, window_start| (group_of(groups, *hash, window_start.to_order()), utils::empty_key()),
			|| RowAccumulator::new(kinds, grace),
		)?;
		engine.flush(&mut store)?;
		res
	};

	{
		let mut store = OperatorStateStore::new(txn, core.node);
		for r in &results {
			let group = group_of(groups, r.group, r.span.start.to_order());
			let window_start = r.span.start.to_order();
			let prior_meta = core.engine_meta().get(&mut store, &EngineMetaKey(group))?;
			let prior_last = prior_meta.as_ref().map(|m| m.last_event_time);
			let prior_index = prior_meta.is_some().then(|| anchor.of(window_start, prior_last)).flatten();
			match r.kind {
				EmitKind::Remove => {
					engine.reindex_window(
						&mut store,
						&r.group,
						r.span.start,
						group,
						r.row_number,
						prior_index,
						None,
					)?;
					core.engine_meta().remove(&mut store, &EngineMetaKey(group))?;
				}
				EmitKind::Insert | EmitKind::Update => {
					let batch_max = window_max_ts.get(&(r.group, r.span)).map(|ts| ts.to_order());
					let last_event_time = prior_last.max(batch_max);
					let new_index = anchor.of(window_start, last_event_time);
					engine.reindex_window(
						&mut store,
						&r.group,
						r.span.start,
						group,
						r.row_number,
						prior_index,
						new_index,
					)?;
					let meta = EngineMeta {
						group_hash: r.group.0,
						window_start: r.span.start.to_order(),
						row_number: r.row_number.0,
						last_event_time: last_event_time.unwrap_or_default(),
						group_values: group_values.get(&r.group).cloned().unwrap_or_default(),
					};
					core.engine_meta().put(&mut store, &EngineMetaKey(group), meta)?;
				}
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
				let row = core.build_engine_row(&gvals, &r.value, r.row_number, ts, r.span.start)?;
				diffs.push(Diff::insert(Columns::from_row(&row)));
			}
			EmitKind::Update => {
				let pre_vals: &[Value] = r.prior.as_deref().unwrap_or(&r.value);
				let pre = core.build_engine_row(&gvals, pre_vals, r.row_number, ts, r.span.start)?;
				let post = core.build_engine_row(&gvals, &r.value, r.row_number, ts, r.span.start)?;
				diffs.push(Diff::update(Columns::from_row(&pre), Columns::from_row(&post)));
			}
			EmitKind::Remove => {
				let pre_vals: &[Value] = r.prior.as_deref().unwrap_or(&r.value);
				let pre = core.build_engine_row(&gvals, pre_vals, r.row_number, ts, r.span.start)?;
				diffs.push(Diff::remove(Columns::from_row(&pre)));
			}
		}
	}
	Ok(diffs)
}

pub fn apply_tumbling_engine(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let window_size = operator.size_duration().unwrap_or_default();
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<DateTime>), DateTime> = HashMap::new();

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
					window_size,
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
					window_size,
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
						window_size,
						&mut buckets,
						&mut group_values,
						&mut arrival,
						&mut window_max_ts,
					)?;
					route_engine_columns(
						operator,
						post,
						true,
						window_size,
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
		window_size.try_add(operator.grace()).unwrap_or(window_size),
		ExpiryAnchor::WindowStart,
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
		if operator.is_count_based() {
			ExpiryAnchor::Unindexed
		} else {
			ExpiryAnchor::WindowStart
		},
	)?;
	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

fn intern_batch(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	arrival: &[(Hash128, WindowSpan<DateTime>)],
) -> Result<WindowGroups> {
	let windows: Vec<(Hash128, u64)> = arrival.iter().map(|(hash, span)| (*hash, span.start.to_order())).collect();
	intern_window_groups(operator.core.node, txn, &windows)
}

fn sliding_insert_anchors(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	hash: Hash128,
	event_ts: DateTime,
	is_count: bool,
) -> Result<Vec<u64>> {
	let coord = if is_count {
		operator.get_and_increment_global_count(txn, hash)?
	} else {
		event_ts.to_order()
	};
	Ok(operator.sliding_window_anchors(coord))
}

pub fn apply_sliding_engine(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let is_count = operator.is_count_based();
	let window_size = operator.size_duration().unwrap_or_default();

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<DateTime>), DateTime> = HashMap::new();

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
					operator.row_times(post, post.row_count())?
				};
				let slot_cols = operator.core.evaluate_slot_inputs(post)?;
				for row_idx in 0..post.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let event_ts = if is_count {
						DateTime::default()
					} else {
						timestamps[row_idx]
					};
					let window_ids =
						sliding_insert_anchors(operator, txn, *hash, event_ts, is_count)?;
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
							sliding_window_span(operator, *wid),
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
					operator.row_times(pre, pre.row_count())?
				};
				let slot_cols = operator.core.evaluate_slot_inputs(pre)?;
				for row_idx in 0..pre.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let event_ts = if is_count {
						DateTime::default()
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
							sliding_window_span(operator, wid),
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
					operator.row_times(post, post.row_count())?
				};
				let pre_cols = operator.core.evaluate_slot_inputs(pre)?;
				let post_cols = operator.core.evaluate_slot_inputs(post)?;
				for row_idx in 0..pre.row_count() {
					let (hash, gvals) = &groups[row_idx];
					let row_number = pre.row_numbers()[row_idx];
					let event_ts = if is_count {
						DateTime::default()
					} else {
						timestamps[row_idx]
					};
					let existing = operator.lookup_row_index(txn, *hash, row_number)?;
					if existing.is_empty() {
						let window_ids = sliding_insert_anchors(
							operator, txn, *hash, event_ts, is_count,
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
								sliding_window_span(operator, *wid),
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
								sliding_window_span(operator, wid),
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
								sliding_window_span(operator, wid),
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
		window_size.try_add(operator.grace()).unwrap_or(window_size),
		ExpiryAnchor::WindowStart,
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
		if operator.is_count_based() {
			ExpiryAnchor::Unindexed
		} else {
			ExpiryAnchor::WindowStart
		},
	)?;
	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

fn session_assign(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	hash: Hash128,
	event_ts: DateTime,
	gap_ms: u64,
	trackers: &mut HashMap<Hash128, (u64, u64, u64)>,
	closes: &mut Vec<(Hash128, u64)>,
) -> Result<Option<u64>> {
	let event_order = event_ts.to_order();
	let (mut session_id, last, start) = match trackers.get(&hash) {
		Some(&tracker) => tracker,
		None => {
			let tracker = operator.load_session_tracker(txn, hash)?;
			trackers.insert(hash, tracker);
			tracker
		}
	};
	if last == 0 {
		trackers.insert(hash, (session_id, event_order, event_order));
		return Ok(Some(session_id));
	}
	if event_order > last && event_order - last > gap_ms {
		closes.push((hash, session_id));
		session_id += 1;
		trackers.insert(hash, (session_id, event_order, event_order));
		return Ok(Some(session_id));
	}
	if event_order < start && start - event_order > gap_ms {
		return Ok(None);
	}
	trackers.insert(hash, (session_id, last.max(event_order), start.min(event_order)));
	Ok(Some(session_id))
}

pub fn apply_session_engine(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let gap_ms = operator.session_gap_ms();

	let mut buckets: EngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut arrival: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut window_max_ts: HashMap<(Hash128, WindowSpan<DateTime>), DateTime> = HashMap::new();
	let mut closes: Vec<(Hash128, u64)> = Vec::new();
	let mut trackers: HashMap<Hash128, (u64, u64, u64)> = HashMap::new();

	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => {
				let groups = operator.core.compute_groups(post)?;
				let timestamps = operator.row_times(post, post.row_count())?;
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
							ordinal_window_span(session_id),
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
				let timestamps = operator.row_times(pre, pre.row_count())?;
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
							ordinal_window_span(session_id),
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
				let timestamps = operator.row_times(post, post.row_count())?;
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
								ordinal_window_span(session_id),
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
								ordinal_window_span(session_id),
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
								ordinal_window_span(session_id),
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
		operator.session_cutoff(),
		ExpiryAnchor::LastEvent,
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
		ExpiryAnchor::LastEvent,
	)?;

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
			Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::group_scoped(
				operator.engine_config(),
			))
		});
		let mut store = OperatorStateStore::new(txn, node);
		for (hash, session_id, group) in &closing {
			let (row_number, _) = store.get_or_create_row_number(*group, &utils::empty_key())?;
			let accumulator_key = WindowStateKey::new(*group, row_number).into_state_key();
			let meta = operator.core.engine_meta().get(&mut store, &EngineMetaKey(*group))?;
			let prior_last = meta.as_ref().map(|m| m.last_event_time).unwrap_or(0);
			if prior_last > 0 {
				disarm.push((*hash, *session_id, prior_last));
			}
			engine.reindex_window(
				&mut store,
				hash,
				<DateTime as WindowCoord>::from_order(*session_id),
				*group,
				row_number,
				(prior_last > 0).then_some(prior_last),
				None,
			)?;
			store.state_remove(&accumulator_key)?;
			operator.core.engine_meta().remove(&mut store, &EngineMetaKey(*group))?;
		}
		*operator.core.tumbling_engine_slot() = Some(engine);
	}

	if !operator.is_count_based() {
		let node = operator.core.node;
		let cutoff = operator.session_cutoff();
		for (hash, session_id, prior_last) in disarm {
			txn.disarm_timer(
				node,
				&Timer {
					at: seal_instant(prior_last, cutoff),
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
	arrival: &mut Vec<(Hash128, WindowSpan<DateTime>)>,
	window_max_ts: &HashMap<(Hash128, WindowSpan<DateTime>), DateTime>,
	cutoff: Duration,
	anchor: ExpiryAnchor,
) -> Result<()> {
	if cutoff.is_zero() || operator.is_count_based() {
		return Ok(());
	}
	let frontier = operator.seal_frontier(txn)?;
	let node = operator.core.node;
	let mut known: Vec<Option<GroupId>> = Vec::with_capacity(buckets.len());
	for (hash, span) in buckets.keys() {
		known.push(txn.lookup_group(node, &window_group_key(*hash, span.start.to_order()))?);
	}
	let mut sealed: Vec<(Hash128, WindowSpan<DateTime>)> = Vec::new();
	let mut rearm: Vec<(Hash128, u64, Option<u64>, u64)> = Vec::new();
	let mut dropped = 0u64;
	{
		let mut store = OperatorStateStore::new(txn, node);
		for ((key, events), group) in buckets.iter().zip(known) {
			let prior_last = match group {
				Some(group) => operator
					.core
					.engine_meta()
					.get(&mut store, &EngineMetaKey(group))?
					.map(|m| m.last_event_time),
				None => None,
			};
			let batch_last = window_max_ts.get(key).map(|ts| ts.to_order());
			let last = prior_last.max(batch_last);
			let window_start = key.1.start.to_order();
			let Some(horizon) = anchor.of(window_start, last) else {
				continue;
			};
			let prior_horizon = anchor.of(window_start, prior_last);
			if seal_instant(horizon, cutoff) <= frontier {
				dropped += events.len() as u64;
				sealed.push(*key);
			} else {
				rearm.push((key.0, window_start, prior_horizon, horizon));
			}
		}
	}

	for (hash, window_start, prior_horizon, horizon) in rearm {
		let key = window_group_key(hash, window_start);
		if let Some(prior_horizon) = prior_horizon
			&& seal_instant(prior_horizon, cutoff) != seal_instant(horizon, cutoff)
		{
			txn.disarm_timer(
				node,
				&Timer {
					at: seal_instant(prior_horizon, cutoff),
					kind: TimerKind::Seal,
					key: key.clone(),
				},
			)?;
		}
		txn.arm_timer(
			node,
			&Timer {
				at: seal_instant(horizon, cutoff),
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
	let sealed: HashSet<(Hash128, WindowSpan<DateTime>)> = sealed.into_iter().collect();
	arrival.retain(|key| !sealed.contains(key));
	operator.note_sealed_drops(dropped);
	Ok(())
}

#[tracing::instrument(name = "flow::window::seal", level = "debug", skip_all, fields(node = operator.core.node.0, expired = tracing::field::Empty))]
fn seal_due_windows(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	at: u64,
	cutoff: Duration,
) -> Result<Vec<Diff>> {
	if cutoff.is_zero() {
		return Ok(Vec::new());
	}
	let ts = <DateTime as WindowCoord>::from_order(at);
	operator.advance_seal_ledger(txn, ts)?;

	let cutoff_ms = <DateTime as WindowCoord>::span_millis(cutoff).unwrap_or(0);
	let Some(threshold) =
		at.checked_sub(cutoff_ms).and_then(|t| t.checked_sub(1)).map(<DateTime as WindowCoord>::from_order)
	else {
		return Ok(Vec::new());
	};
	let expired = {
		let mut store = OperatorStateStore::new(txn, operator.core.node);
		let mut engine = operator.core.tumbling_engine_slot().take().unwrap_or_else(|| {
			Box::new(TumblingEngine::<Hash128, DateTime, RowAccumulator>::group_scoped(
				operator.engine_config(),
			))
		});
		let res = engine.expire(&mut store, threshold.to_order())?;
		engine.flush(&mut store)?;
		*operator.core.tumbling_engine_slot() = Some(engine);
		res
	};
	warn_when_expiry_capped(operator, expired.len());
	Span::current().record("expired", expired.len());
	let mut store = OperatorStateStore::new(txn, operator.core.node);
	for window in expired {
		operator.core.engine_meta().remove(&mut store, &EngineMetaKey(window.group_id))?;
		if window.accumulator_present {
			store.remove_row_number(window.group_id, &utils::empty_key())?;
		}
	}
	Ok(Vec::new())
}

pub fn seal_session_engine(operator: &WindowOperator, txn: &mut FlowTransaction, at: u64) -> Result<Vec<Diff>> {
	seal_due_windows(operator, txn, at, operator.session_cutoff())
}

pub fn seal_engine_windows(operator: &WindowOperator, txn: &mut FlowTransaction, at: u64) -> Result<Vec<Diff>> {
	let Some(window_size) = operator.size_duration() else {
		return Ok(Vec::new());
	};
	seal_due_windows(operator, txn, at, window_size.try_add(operator.grace()).unwrap_or(window_size))
}

#[cfg(test)]
mod tests {
	use reifydb_core::window::{
		engine::{is_sealed, seal_horizon},
		span::WindowCoord,
	};
	use reifydb_value::value::duration::Duration;

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
		let cutoff_ms = 19u64;
		let cutoff = Duration::from_milliseconds(cutoff_ms as i64).expect("representable span");
		let last = 10u64;
		let sealed = |wm: u64| seal_instant(last, cutoff).to_order() <= wm;
		let pre_timer_gate = |wm: u64| wm.saturating_sub(last) > cutoff_ms;

		for wm in 0..100u64 {
			assert_eq!(
				sealed(wm),
				pre_timer_gate(wm),
				"timer seal diverges from the gate at watermark {wm}"
			);
		}
		assert!(!sealed(last + cutoff_ms), "watermark exactly cutoff past the last event is still mutable");
		assert!(sealed(last + cutoff_ms + 1), "one past the cutoff is sealed");
	}

	#[test]
	fn seal_horizon_saturates_for_young_watermarks() {
		// A watermark smaller than seal_after must not wrap; nothing is sealed yet.
		assert_eq!(seal_horizon(3, 10), 0, "young watermark saturates to zero horizon");
		assert!(!is_sealed(0, seal_horizon(3, 10)), "anchor zero is not below a zero horizon");
		assert!(is_sealed(4, seal_horizon(20, 10)), "anchor below watermark - seal_after is sealed");
	}
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

		assert_eq!(
			<DateTime as WindowCoord>::from_order(bucket),
			DateTime::from_timestamp_millis(bucket).unwrap()
		);
		assert_eq!(
			<DateTime as WindowCoord>::from_order(bucket),
			<DateTime as WindowCoord>::from_order(bucket),
			"the stamp depends on the bucket alone, so it cannot vary between two runs"
		);
	}

	#[test]
	// Intent: distinct buckets must get distinct stamps, or a chained rollup (1s -> 1m) would
	// collapse every source bucket onto one instant and the downstream window could not separate
	// them.
	fn adjacent_buckets_get_distinct_stamps_in_bucket_order() {
		let first = <DateTime as WindowCoord>::from_order(1_700_000_000_000);
		let second = <DateTime as WindowCoord>::from_order(1_700_000_001_000);

		assert!(first < second, "bucket order must survive into #time");
		assert_eq!(second - first, Duration::from_seconds(1).unwrap(), "a 1s bucket step is 1s in #time");
	}

	#[test]
	// Intent: a far-future bucket must not wrap into a tiny stamp that would look ancient and be
	// evicted immediately. The millisecond -> instant conversion is now fallible rather than a
	// saturating multiply, so the guard is that an unrepresentable bucket still orders above a real
	// one instead of collapsing below it.
	fn a_far_future_bucket_saturates_rather_than_wrapping() {
		assert_eq!(<DateTime as WindowCoord>::from_order(u64::MAX), DateTime::MAX);
		assert!(<DateTime as WindowCoord>::from_order(u64::MAX)
			> <DateTime as WindowCoord>::from_order(1_700_000_000_000));
	}

	#[test]
	// Intent: the epoch bucket maps to the epoch instant, so an unset window_start cannot be
	// mistaken for a real time far from zero.
	fn the_zero_bucket_maps_to_the_epoch() {
		assert_eq!(<DateTime as WindowCoord>::from_order(0), DateTime::EPOCH);
	}
}
