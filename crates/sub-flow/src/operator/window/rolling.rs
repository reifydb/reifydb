// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap, HashSet};

use reifydb_core::{
	common::{TimeDomain, WindowKind},
	interface::change::{Change, Diff},
	value::column::columns::Columns,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent,
			rolling::{
				RollingBuckets, RollingBuffer, RollingEngine, RollingEviction, RollingExpiry,
				RollingResult,
			},
		},
		store::WindowStore,
	},
};
use reifydb_engine::flow::aggregate::SlotKind;
use reifydb_runtime::hash::Hash128;
use reifydb_value::{
	Result,
	value::{Value, duration::Duration, row_number::RowNumber},
};
use serde::{Deserialize, Serialize};

use super::{
	accumulator::{RowAccumulator, StampedAccumulator, WindowSlotKey},
	operator::WindowOperator,
	store::FlowWindowStore,
	tumbling::slot_coord,
};
use crate::transaction::FlowTransaction;

impl WindowOperator {
	pub fn rolling_lag_ms(&self) -> u64 {
		match &self.kind {
			WindowKind::Rolling {
				lag: Some(lag),
				..
			} => lag.milliseconds().unwrap_or(0) as u64,
			_ => 0,
		}
	}
}

impl WindowOperator {
	pub fn is_rolling_processing(&self) -> bool {
		matches!(self.kind, WindowKind::Rolling { .. })
			&& !self.is_count_based()
			&& self.kind.time() == TimeDomain::Processing
	}
}

#[derive(Default, Serialize, Deserialize)]
struct RollingWindowMeta {
	group_hash: u128,
	row_number: u64,
	group_values: Vec<Value>,
	last_value: Vec<Value>,
}

type RollingEngineBuckets = RollingBuckets<Hash128, u64, (WindowSlotKey, Vec<Option<Value>>)>;

fn combine_rolling(
	buffer: &RollingBuffer<u64, RowAccumulator>,
	kinds: &[SlotKind],
	lag_ms: u64,
	lateness: Option<Duration>,
) -> Option<Vec<Value>> {
	let (&newest, _) = buffer.iter().next_back()?;
	let aggregate_cutoff = newest.saturating_sub(lag_ms);
	let mut merged = RowAccumulator::new(kinds, lateness);
	let mut any = false;
	for (_coord, accumulator) in buffer.range(..=aggregate_cutoff) {
		merged.merge(accumulator);
		any = true;
	}
	if any {
		merged.finalize()
	} else {
		None
	}
}

#[allow(clippy::too_many_arguments)]
fn route_rolling_columns(
	operator: &WindowOperator,
	columns: &Columns,
	is_add: bool,
	is_count: bool,
	buckets: &mut RollingEngineBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	touched: &mut Vec<Hash128>,
	touched_set: &mut HashSet<Hash128>,
) -> Result<()> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(());
	}
	let groups = operator.core.compute_groups(columns)?;
	let timestamps = if is_count {
		Vec::new()
	} else {
		operator.resolve_event_timestamps(columns, row_count)?
	};
	let slot_cols = operator.core.evaluate_slot_inputs(columns)?;
	for row_idx in 0..row_count {
		let (hash, gvals) = &groups[row_idx];
		let coord = if is_count {
			columns.row_numbers[row_idx].0
		} else {
			timestamps[row_idx]
		};
		let slot_key = slot_coord(is_count, coord, columns.row_numbers[row_idx].0);
		let contribution = (slot_key, operator.core.build_contribution(columns, &slot_cols, row_idx));
		let event = if is_add {
			AccumulatorEvent::Add(contribution)
		} else {
			AccumulatorEvent::Remove(contribution)
		};
		buckets.entry((*hash, coord)).or_default().push(event);
		group_values.entry(*hash).or_insert_with(|| gvals.clone());
		if touched_set.insert(*hash) {
			touched.push(*hash);
		}
	}
	Ok(())
}

pub fn apply_rolling_engine(operator: &WindowOperator, txn: &mut FlowTransaction, change: Change) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let is_count = operator.is_count_based();
	let lateness = operator.sealing_lateness();
	let lag_ms = operator.rolling_lag_ms();
	let is_event_time = !is_count && operator.kind.time() == TimeDomain::Event;
	let size_ms = operator.size_duration().map(|d| d.milliseconds().unwrap_or(0) as u64).unwrap_or(0);

	let mut buckets: RollingEngineBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut touched: Vec<Hash128> = Vec::new();
	let mut touched_set: HashSet<Hash128> = HashSet::new();
	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => route_rolling_columns(
				operator,
				post,
				true,
				is_count,
				&mut buckets,
				&mut group_values,
				&mut touched,
				&mut touched_set,
			)?,
			Diff::Remove {
				pre,
				..
			} => route_rolling_columns(
				operator,
				pre,
				false,
				is_count,
				&mut buckets,
				&mut group_values,
				&mut touched,
				&mut touched_set,
			)?,
			Diff::Update {
				pre,
				post,
				..
			} => {
				route_rolling_columns(
					operator,
					pre,
					false,
					is_count,
					&mut buckets,
					&mut group_values,
					&mut touched,
					&mut touched_set,
				)?;
				route_rolling_columns(
					operator,
					post,
					true,
					is_count,
					&mut buckets,
					&mut group_values,
					&mut touched,
					&mut touched_set,
				)?;
			}
		}
	}

	if buckets.is_empty() {
		return Ok(Change::from_flow(operator.core.node, change.version, Vec::new(), change.changed_at));
	}

	let eviction = if is_count {
		RollingEviction::Capacity(operator.size_count().unwrap_or(0) as usize)
	} else if is_event_time {
		let batch_max = buckets.keys().map(|&(_, coord)| coord).max().unwrap_or(0);
		operator.advance_event_watermark(txn, batch_max)?;
		RollingEviction::Before(operator.event_time_cutoff(txn, size_ms + lag_ms)?)
	} else {
		RollingEviction::Before(operator.core.current_timestamp().saturating_sub(size_ms + lag_ms))
	};

	let results = {
		let mut store = FlowWindowStore::new(txn, operator.core.node);
		for hash in &touched {
			let key = operator.core.create_window_key(*hash, 0);
			store.get_or_create_row_number(&key)?;
		}
		let mut engine = RollingEngine::<Hash128, u64, RowAccumulator>::with_late_policy(operator.late_policy);
		let res = engine.apply_evicting(
			&mut store,
			buckets,
			eviction,
			|hash| operator.core.create_window_key(*hash, 0),
			|| RowAccumulator::new(&kinds, lateness),
			|_g, buffer| combine_rolling(buffer, &kinds, lag_ms, lateness),
		)?;
		engine.flush(&mut store)?;
		res
	};

	let diffs = finish_rolling_results(operator, txn, &change, &results, &group_values, &touched)?;
	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

fn finish_rolling_results(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	change: &Change,
	results: &[RollingResult<Hash128, Vec<Value>>],
	group_values: &HashMap<Hash128, Vec<Value>>,
	touched: &[Hash128],
) -> Result<Vec<Diff>> {
	let ts_nanos = change.changed_at.to_nanos();
	let mut diffs = Vec::new();
	let mut emitted: HashSet<Hash128> = HashSet::new();
	let mut store = FlowWindowStore::new(txn, operator.core.node);
	for r in results {
		emitted.insert(r.group);
		let gvals = group_values.get(&r.group).cloned().unwrap_or_default();
		let meta_key = operator.create_rolling_meta_key(r.group);
		let prior = store.state_get::<RollingWindowMeta>(&meta_key)?;
		let post = operator.core.build_engine_row(&gvals, &r.value, r.row_number, ts_nanos)?;
		match prior {
			Some(m) => {
				let pre = operator.core.build_engine_row(
					&gvals,
					&m.last_value,
					r.row_number,
					ts_nanos,
				)?;
				diffs.push(Diff::update(Columns::from_row(&pre), Columns::from_row(&post)));
			}
			None => diffs.push(Diff::insert(Columns::from_row(&post))),
		}
		store.state_set(
			&meta_key,
			&RollingWindowMeta {
				group_hash: r.group.0,
				row_number: r.row_number.0,
				group_values: gvals,
				last_value: r.value.clone(),
			},
		)?;
	}
	for hash in touched {
		if emitted.contains(hash) {
			continue;
		}
		let meta_key = operator.create_rolling_meta_key(*hash);
		if let Some(m) = store.state_get::<RollingWindowMeta>(&meta_key)? {
			let pre = operator.core.build_engine_row(
				&m.group_values,
				&m.last_value,
				RowNumber(m.row_number),
				ts_nanos,
			)?;
			diffs.push(Diff::remove(Columns::from_row(&pre)));
			store.state_remove(&meta_key)?;
		}
	}
	Ok(diffs)
}

pub fn tick_expire_rolling_engine(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	current_timestamp: u64,
) -> Result<Vec<Diff>> {
	let size_ms = match operator.size_duration() {
		Some(d) => d.milliseconds().unwrap_or(0) as u64,
		None => return Ok(Vec::new()),
	};
	if size_ms == 0 {
		return Ok(Vec::new());
	}
	let lag_ms = operator.rolling_lag_ms();
	let lateness = operator.sealing_lateness();
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let cutoff = operator.event_time_cutoff(txn, size_ms + lag_ms)?;
	let ts_nanos = current_timestamp.saturating_mul(1_000_000);

	let expiries = {
		let mut store = FlowWindowStore::new(txn, operator.core.node);
		let mut engine = RollingEngine::<Hash128, u64, RowAccumulator>::with_late_policy(operator.late_policy);
		let res = engine.expire_before(&mut store, cutoff, |_g, buffer| {
			combine_rolling(buffer, &kinds, lag_ms, lateness)
		})?;
		engine.flush(&mut store)?;
		res
	};

	let mut diffs = Vec::new();
	let mut store = FlowWindowStore::new(txn, operator.core.node);
	for expiry in expiries {
		match expiry {
			RollingExpiry::Update {
				row_number,
				group,
				value,
			} => {
				let meta_key = operator.create_rolling_meta_key(group);
				let Some(meta) = store.state_get::<RollingWindowMeta>(&meta_key)? else {
					continue;
				};
				let pre = operator.core.build_engine_row(
					&meta.group_values,
					&meta.last_value,
					row_number,
					ts_nanos,
				)?;
				let post = operator.core.build_engine_row(
					&meta.group_values,
					&value,
					row_number,
					ts_nanos,
				)?;
				diffs.push(Diff::update(Columns::from_row(&pre), Columns::from_row(&post)));
				store.state_set(
					&meta_key,
					&RollingWindowMeta {
						group_hash: meta.group_hash,
						row_number: meta.row_number,
						group_values: meta.group_values,
						last_value: value,
					},
				)?;
			}
			RollingExpiry::Remove {
				row_number,
				group,
			} => {
				let meta_key = operator.create_rolling_meta_key(group);
				let Some(meta) = store.state_get::<RollingWindowMeta>(&meta_key)? else {
					continue;
				};
				let pre = operator.core.build_engine_row(
					&meta.group_values,
					&meta.last_value,
					row_number,
					ts_nanos,
				)?;
				diffs.push(Diff::remove(Columns::from_row(&pre)));
				store.state_remove(&meta_key)?;
			}
		}
	}
	Ok(diffs)
}

type StampedBuckets = RollingBuckets<Hash128, u64, ((WindowSlotKey, Vec<Option<Value>>), u64)>;

fn combine_stamped(buffer: &RollingBuffer<u64, StampedAccumulator>, kinds: &[SlotKind]) -> Option<Vec<Value>> {
	let mut merged = RowAccumulator::new(kinds, None);
	let mut any = false;
	for (_coord, accumulator) in buffer.iter() {
		merged.merge(accumulator.inner());
		any = true;
	}
	if any {
		merged.finalize()
	} else {
		None
	}
}

#[allow(clippy::too_many_arguments)]
fn route_rolling_processing(
	operator: &WindowOperator,
	columns: &Columns,
	is_add: bool,
	now: u64,
	buckets: &mut StampedBuckets,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	touched: &mut Vec<Hash128>,
	touched_set: &mut HashSet<Hash128>,
) -> Result<()> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(());
	}
	let groups = operator.core.compute_groups(columns)?;
	let slot_cols = operator.core.evaluate_slot_inputs(columns)?;
	for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
		let coord = columns.row_numbers[row_idx].0;
		let slot_key = slot_coord(true, 0, coord);
		let value_contrib = (slot_key, operator.core.build_contribution(columns, &slot_cols, row_idx));
		let event = if is_add {
			AccumulatorEvent::Add((value_contrib, now))
		} else {
			AccumulatorEvent::Remove((value_contrib, 0))
		};
		buckets.entry((*hash, coord)).or_default().push(event);
		group_values.entry(*hash).or_insert_with(|| gvals.clone());
		if touched_set.insert(*hash) {
			touched.push(*hash);
		}
	}
	Ok(())
}

pub fn apply_rolling_processing_engine(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	change: Change,
) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let size_ms = operator.size_duration().map(|d| d.milliseconds().unwrap_or(0) as u64).unwrap_or(0);
	let lag_ms = operator.rolling_lag_ms();
	let now = operator.core.current_timestamp();

	let mut buckets: StampedBuckets = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut touched: Vec<Hash128> = Vec::new();
	let mut touched_set: HashSet<Hash128> = HashSet::new();
	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => route_rolling_processing(
				operator,
				post,
				true,
				now,
				&mut buckets,
				&mut group_values,
				&mut touched,
				&mut touched_set,
			)?,
			Diff::Remove {
				pre,
				..
			} => route_rolling_processing(
				operator,
				pre,
				false,
				now,
				&mut buckets,
				&mut group_values,
				&mut touched,
				&mut touched_set,
			)?,
			Diff::Update {
				pre,
				post,
				..
			} => {
				route_rolling_processing(
					operator,
					pre,
					false,
					now,
					&mut buckets,
					&mut group_values,
					&mut touched,
					&mut touched_set,
				)?;
				route_rolling_processing(
					operator,
					post,
					true,
					now,
					&mut buckets,
					&mut group_values,
					&mut touched,
					&mut touched_set,
				)?;
			}
		}
	}

	if buckets.is_empty() {
		return Ok(Change::from_flow(operator.core.node, change.version, Vec::new(), change.changed_at));
	}

	let cutoff = now.saturating_sub(size_ms + lag_ms);
	let results = {
		let mut store = FlowWindowStore::new(txn, operator.core.node);
		for hash in &touched {
			let key = operator.core.create_window_key(*hash, 0);
			store.get_or_create_row_number(&key)?;
		}
		let mut engine =
			RollingEngine::<Hash128, u64, StampedAccumulator>::with_late_policy(operator.late_policy);
		let res = engine.apply_evicting(
			&mut store,
			buckets,
			RollingEviction::BeforeStamp(cutoff),
			|hash| operator.core.create_window_key(*hash, 0),
			|| StampedAccumulator::new(&kinds, None),
			|_g, buffer| combine_stamped(buffer, &kinds),
		)?;
		engine.flush(&mut store)?;
		res
	};

	let diffs = finish_rolling_results(operator, txn, &change, &results, &group_values, &touched)?;
	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

pub fn tick_expire_rolling_processing_engine(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	current_timestamp: u64,
) -> Result<Vec<Diff>> {
	let size_ms = match operator.size_duration() {
		Some(d) => d.milliseconds().unwrap_or(0) as u64,
		None => return Ok(Vec::new()),
	};
	if size_ms == 0 {
		return Ok(Vec::new());
	}
	let lag_ms = operator.rolling_lag_ms();
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let cutoff = current_timestamp.saturating_sub(size_ms + lag_ms);
	let ts_nanos = current_timestamp.saturating_mul(1_000_000);

	let expiries = {
		let mut store = FlowWindowStore::new(txn, operator.core.node);
		let mut engine =
			RollingEngine::<Hash128, u64, StampedAccumulator>::with_late_policy(operator.late_policy);
		let res =
			engine.expire_before_stamp(&mut store, cutoff, |_g, buffer| combine_stamped(buffer, &kinds))?;
		engine.flush(&mut store)?;
		res
	};

	let mut diffs = Vec::new();
	let mut store = FlowWindowStore::new(txn, operator.core.node);
	for expiry in expiries {
		match expiry {
			RollingExpiry::Update {
				row_number,
				group,
				value,
			} => {
				let meta_key = operator.create_rolling_meta_key(group);
				let Some(meta) = store.state_get::<RollingWindowMeta>(&meta_key)? else {
					continue;
				};
				let pre = operator.core.build_engine_row(
					&meta.group_values,
					&meta.last_value,
					row_number,
					ts_nanos,
				)?;
				let post = operator.core.build_engine_row(
					&meta.group_values,
					&value,
					row_number,
					ts_nanos,
				)?;
				diffs.push(Diff::update(Columns::from_row(&pre), Columns::from_row(&post)));
				store.state_set(
					&meta_key,
					&RollingWindowMeta {
						group_hash: meta.group_hash,
						row_number: meta.row_number,
						group_values: meta.group_values,
						last_value: value,
					},
				)?;
			}
			RollingExpiry::Remove {
				row_number,
				group,
			} => {
				let meta_key = operator.create_rolling_meta_key(group);
				let Some(meta) = store.state_get::<RollingWindowMeta>(&meta_key)? else {
					continue;
				};
				let pre = operator.core.build_engine_row(
					&meta.group_values,
					&meta.last_value,
					row_number,
					ts_nanos,
				)?;
				diffs.push(Diff::remove(Columns::from_row(&pre)));
				store.state_remove(&meta_key)?;
			}
		}
	}
	Ok(diffs)
}
