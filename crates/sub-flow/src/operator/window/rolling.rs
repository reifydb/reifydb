// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap, HashSet};

use reifydb_core::{
	common::{TimeDomain, WindowKind},
	interface::change::{Change, Diff},
	state::store::StateStore,
	value::column::columns::Columns,
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, is_sealed,
			rolling::{
				RollingBuckets, RollingBuffer, RollingEngine, RollingEviction, RollingExpiry,
				RollingResult,
			},
			seal_horizon,
		},
	},
};
use reifydb_engine::flow::aggregate::SlotKind;
use reifydb_flow::transaction::FlowTransaction;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, duration::Duration, row_number::RowNumber},
};
use tracing::Span;

use super::{
	accumulator::{RowAccumulator, StampedAccumulator, WindowSlotKey},
	aux::RollingMeta,
	operator::{RollingEngineSlot, WindowOperator},
	tumbling::{WindowGroups, batch_position, group_of, intern_window_groups, slot_coord},
};
use crate::operator::{stateful::utils, store::OperatorStateStore, window::warn_when_expiry_capped};

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

type RollingEngineBuckets = RollingBuckets<Hash128, u64, (WindowSlotKey, Vec<Option<Value>>)>;

/// A rolling group is coord-less: the partition alone. It shares the window group
/// encoding at coordinate zero so one node never mixes two group vocabularies.
fn intern_partitions(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	touched: &[Hash128],
) -> Result<WindowGroups> {
	let partitions: Vec<(Hash128, u64)> = touched.iter().map(|hash| (*hash, 0)).collect();
	let position = batch_position(operator, txn)?;
	intern_window_groups(operator.core.node, txn, &partitions, position)
}

/// Row numbers are minted in `touched` order, which is arrival order: it is the order
/// the sink publishes under and goldens pin it.
fn mint_partition_rows(store: &mut OperatorStateStore<'_>, touched: &[Hash128], groups: &WindowGroups) -> Result<()> {
	for hash in touched {
		store.get_or_create_row_number(group_of(groups, *hash, 0), &utils::empty_key())?;
	}
	Ok(())
}

fn rolling_runnable(operator: &WindowOperator, kinds: &[SlotKind]) -> bool {
	!operator.is_count_based() && RowAccumulator::invertible(kinds, operator.grace())
}

fn row_engine(
	operator: &WindowOperator,
	runnable: bool,
	lag_ms: u64,
) -> &mut RollingEngine<Hash128, u64, RowAccumulator> {
	let slot = operator.rolling_engine_slot();
	if !matches!(slot, Some(RollingEngineSlot::Row(_))) {
		let engine = if runnable {
			RollingEngine::new_runnable_group_scoped(operator.engine_config()).with_lag(lag_ms)
		} else {
			RollingEngine::group_scoped(operator.engine_config())
		};
		*slot = Some(RollingEngineSlot::Row(Box::new(engine)));
	}
	match slot {
		Some(RollingEngineSlot::Row(engine)) => engine.as_mut(),
		_ => unreachable!("rolling engine slot must hold a row engine"),
	}
}

fn stamped_engine(operator: &WindowOperator) -> &mut RollingEngine<Hash128, u64, StampedAccumulator> {
	let slot = operator.rolling_engine_slot();
	if !matches!(slot, Some(RollingEngineSlot::Stamped(_))) {
		*slot = Some(RollingEngineSlot::Stamped(Box::new(RollingEngine::group_scoped(
			operator.engine_config(),
		))));
	}
	match slot {
		Some(RollingEngineSlot::Stamped(engine)) => engine.as_mut(),
		_ => unreachable!("rolling engine slot must hold a stamped engine"),
	}
}

fn combine_rolling(
	buffer: &RollingBuffer<u64, RowAccumulator>,
	kinds: &[SlotKind],
	lag_ms: u64,
	grace: Duration,
) -> Option<Vec<Value>> {
	let (&newest, _) = buffer.iter().next_back()?;
	let aggregate_cutoff = newest.saturating_sub(lag_ms);
	let mut merged = RowAccumulator::new(kinds, grace);
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
	let grace = operator.grace();
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

	if is_event_time {
		let watermark = operator.load_event_watermark(txn)?;
		let horizon = seal_horizon(watermark, size_ms + lag_ms + operator.grace_ms());
		let mut dropped = 0u64;
		buckets.retain(|&(_, coord), events| {
			if is_sealed(coord, horizon) {
				dropped += events.len() as u64;
				false
			} else {
				true
			}
		});
		operator.note_sealed_drops(dropped);
		if buckets.is_empty() {
			return Ok(Change::from_flow(
				operator.core.node,
				change.version,
				Vec::new(),
				change.changed_at,
			));
		}
	}

	let groups = intern_partitions(operator, txn, &touched)?;
	let results = {
		let mut store = OperatorStateStore::new(txn, operator.core.node);
		mint_partition_rows(&mut store, &touched, &groups)?;
		if rolling_runnable(operator, &kinds) {
			let engine = row_engine(operator, true, lag_ms);
			let res = engine.apply_running(
				&mut store,
				buckets,
				eviction,
				|hash| (group_of(&groups, *hash, 0), utils::empty_key()),
				|| RowAccumulator::new(&kinds, grace),
			)?;
			engine.flush(&mut store)?;
			res
		} else {
			let engine = row_engine(operator, false, lag_ms);
			let res = engine.apply_evicting(
				&mut store,
				buckets,
				eviction,
				|hash| (group_of(&groups, *hash, 0), utils::empty_key()),
				|| RowAccumulator::new(&kinds, grace),
				|_g, buffer| combine_rolling(buffer, &kinds, lag_ms, grace),
			)?;
			engine.flush(&mut store)?;
			res
		}
	};

	let diffs = finish_rolling_results(operator, txn, &change, &results, &group_values, &touched, &groups)?;
	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

fn finish_rolling_results(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	change: &Change,
	results: &[RollingResult<Hash128, Vec<Value>>],
	group_values: &HashMap<Hash128, Vec<Value>>,
	touched: &[Hash128],
	groups: &WindowGroups,
) -> Result<Vec<Diff>> {
	let ts_nanos = change.changed_at.to_nanos();
	let mut diffs = Vec::new();
	let mut emitted: HashSet<Hash128> = HashSet::new();
	let mut store = OperatorStateStore::new(txn, operator.core.node);
	for r in results {
		emitted.insert(r.group);
		let group_id = group_of(groups, r.group, 0);
		let prior = operator.aux_slot().rolling_meta(&mut store, group_id)?;
		if matches!(r.kind, EmitKind::Remove) {
			if let Some(m) = prior {
				let pre = operator.core.build_engine_row(
					&m.group_values,
					&m.last_value,
					RowNumber(m.row_number),
					ts_nanos,
				)?;
				diffs.push(Diff::remove(Columns::from_row(&pre)));
				operator.aux_slot().drop_rolling_meta(&mut store, group_id)?;
			}
			continue;
		}
		let gvals = group_values.get(&r.group).cloned().unwrap_or_default();
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
		operator.aux_slot().put_rolling_meta(
			&mut store,
			group_id,
			RollingMeta {
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
		let group_id = group_of(groups, *hash, 0);
		if let Some(m) = operator.aux_slot().rolling_meta(&mut store, group_id)? {
			let pre = operator.core.build_engine_row(
				&m.group_values,
				&m.last_value,
				RowNumber(m.row_number),
				ts_nanos,
			)?;
			diffs.push(Diff::remove(Columns::from_row(&pre)));
			operator.aux_slot().drop_rolling_meta(&mut store, group_id)?;
		}
	}
	Ok(diffs)
}

#[tracing::instrument(name = "flow::window::tick_expire_rolling", level = "debug", skip_all, fields(node = operator.core.node.0, expired = tracing::field::Empty))]
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
	let grace = operator.grace();
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let cutoff = operator.event_time_cutoff(txn, size_ms + lag_ms)?;
	let ts_nanos = current_timestamp.saturating_mul(1_000_000);

	let expiries = {
		let mut store = OperatorStateStore::new(txn, operator.core.node);
		if rolling_runnable(operator, &kinds) {
			let engine = row_engine(operator, true, lag_ms);
			let res = engine.expire_before_running(&mut store, cutoff)?;
			engine.flush(&mut store)?;
			res
		} else {
			let engine = row_engine(operator, false, lag_ms);
			let res = engine.expire_before(&mut store, cutoff, |_g, buffer| {
				combine_rolling(buffer, &kinds, lag_ms, grace)
			})?;
			engine.flush(&mut store)?;
			res
		}
	};
	warn_when_expiry_capped(operator, expiries.len());
	Span::current().record("expired", expiries.len());

	let mut diffs = Vec::new();
	let mut store = OperatorStateStore::new(txn, operator.core.node);
	for expiry in expiries {
		match expiry {
			RollingExpiry::Update {
				row_number,
				group: _,
				group_id,
				value,
			} => {
				let Some(meta) = operator.aux_slot().rolling_meta(&mut store, group_id)? else {
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
				operator.aux_slot().put_rolling_meta(
					&mut store,
					group_id,
					RollingMeta {
						group_hash: meta.group_hash,
						row_number: meta.row_number,
						group_values: meta.group_values,
						last_value: value,
					},
				)?;
			}
			RollingExpiry::Remove {
				row_number,
				group: _,
				group_id,
			} => {
				let Some(meta) = operator.aux_slot().rolling_meta(&mut store, group_id)? else {
					continue;
				};
				let pre = operator.core.build_engine_row(
					&meta.group_values,
					&meta.last_value,
					row_number,
					ts_nanos,
				)?;
				diffs.push(Diff::remove(Columns::from_row(&pre)));
				operator.aux_slot().drop_rolling_meta(&mut store, group_id)?;
			}
		}
	}
	Ok(diffs)
}

type StampedBuckets = RollingBuckets<Hash128, u64, ((WindowSlotKey, Vec<Option<Value>>), u64)>;

fn combine_stamped(buffer: &RollingBuffer<u64, StampedAccumulator>, kinds: &[SlotKind]) -> Option<Vec<Value>> {
	let mut merged = RowAccumulator::new(kinds, Duration::default());
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
	let groups = intern_partitions(operator, txn, &touched)?;
	let results = {
		let mut store = OperatorStateStore::new(txn, operator.core.node);
		mint_partition_rows(&mut store, &touched, &groups)?;
		let engine = stamped_engine(operator);
		let res = engine.apply_evicting(
			&mut store,
			buckets,
			RollingEviction::BeforeStamp(cutoff),
			|hash| (group_of(&groups, *hash, 0), utils::empty_key()),
			|| StampedAccumulator::new(&kinds, Duration::default()),
			|_g, buffer| combine_stamped(buffer, &kinds),
		)?;
		engine.flush(&mut store)?;
		res
	};

	let diffs = finish_rolling_results(operator, txn, &change, &results, &group_values, &touched, &groups)?;
	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

#[tracing::instrument(name = "flow::window::tick_expire_rolling_proc", level = "debug", skip_all, fields(node = operator.core.node.0, expired = tracing::field::Empty))]
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
		let mut store = OperatorStateStore::new(txn, operator.core.node);
		let engine = stamped_engine(operator);
		let res =
			engine.expire_before_stamp(&mut store, cutoff, |_g, buffer| combine_stamped(buffer, &kinds))?;
		engine.flush(&mut store)?;
		res
	};
	warn_when_expiry_capped(operator, expiries.len());
	Span::current().record("expired", expiries.len());

	let mut diffs = Vec::new();
	let mut store = OperatorStateStore::new(txn, operator.core.node);
	for expiry in expiries {
		match expiry {
			RollingExpiry::Update {
				row_number,
				group: _,
				group_id,
				value,
			} => {
				let Some(meta) = operator.aux_slot().rolling_meta(&mut store, group_id)? else {
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
				operator.aux_slot().put_rolling_meta(
					&mut store,
					group_id,
					RollingMeta {
						group_hash: meta.group_hash,
						row_number: meta.row_number,
						group_values: meta.group_values,
						last_value: value,
					},
				)?;
			}
			RollingExpiry::Remove {
				row_number,
				group: _,
				group_id,
			} => {
				let Some(meta) = operator.aux_slot().rolling_meta(&mut store, group_id)? else {
					continue;
				};
				let pre = operator.core.build_engine_row(
					&meta.group_values,
					&meta.last_value,
					row_number,
					ts_nanos,
				)?;
				diffs.push(Diff::remove(Columns::from_row(&pre)));
				operator.aux_slot().drop_rolling_meta(&mut store, group_id)?;
			}
		}
	}
	Ok(diffs)
}

#[cfg(test)]
mod tests {
	use std::{
		collections::{BTreeMap as TestBTreeMap, HashMap as TestHashMap},
		ops::Bound,
	};

	use reifydb_codec::{
		key::encoded::{EncodedKey, EncodedKeyRange},
		state::StateBytes,
	};
	use reifydb_core::{
		key::operator_state::GroupId,
		state::{budget::OperatorStateBudgetHandle, store::StateStore},
		window::engine::config::WindowEngineConfig,
	};
	use reifydb_value::{Result as ValueResult, value::datetime::DateTime};

	use super::*;

	// Minimal in-memory StateStore so the differential runs the real engine
	// paths (buffers, running entries, expiry index) without a FlowTransaction.
	#[derive(Default)]
	struct MockStore {
		state: TestHashMap<Vec<u8>, StateBytes>,
		internal: BTreeMap<Vec<u8>, StateBytes>,
		rows: TestHashMap<(GroupId, Vec<u8>), u64>,
		next_row: u64,
	}

	impl StateStore for MockStore {
		fn state_get(&mut self, key: &EncodedKey) -> ValueResult<Option<StateBytes>> {
			Ok(self.state.get(key.as_bytes()).cloned())
		}
		fn state_get_many_visit(
			&mut self,
			keys: &[EncodedKey],
			visit: &mut dyn FnMut(EncodedKey, StateBytes) -> ValueResult<()>,
		) -> ValueResult<()> {
			for key in keys {
				if let Some(b) = self.state.get(key.as_bytes()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}
		fn state_set(&mut self, key: &EncodedKey, payload: StateBytes) -> ValueResult<()> {
			self.state.insert(key.as_bytes().to_vec(), payload);
			Ok(())
		}
		fn state_remove(&mut self, key: &EncodedKey) -> ValueResult<()> {
			self.state.remove(key.as_bytes());
			Ok(())
		}
		fn internal_get(&mut self, key: &EncodedKey) -> ValueResult<Option<StateBytes>> {
			Ok(self.internal.get(key.as_bytes()).cloned())
		}
		fn internal_get_many_visit(
			&mut self,
			keys: &[EncodedKey],
			visit: &mut dyn FnMut(EncodedKey, StateBytes) -> ValueResult<()>,
		) -> ValueResult<()> {
			for key in keys {
				if let Some(b) = self.internal.get(key.as_bytes()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}
		fn internal_set(&mut self, key: &EncodedKey, payload: StateBytes) -> ValueResult<()> {
			self.internal.insert(key.as_bytes().to_vec(), payload);
			Ok(())
		}
		fn internal_remove(&mut self, key: &EncodedKey) -> ValueResult<()> {
			self.internal.remove(key.as_bytes());
			Ok(())
		}
		fn internal_range_visit(
			&mut self,
			range: EncodedKeyRange,
			limit: Option<usize>,
			visit: &mut dyn FnMut(EncodedKey, StateBytes) -> ValueResult<()>,
		) -> ValueResult<()> {
			let mut seen = 0usize;
			let entries: Vec<(Vec<u8>, StateBytes)> = self
				.internal
				.iter()
				.filter(|(k, _)| {
					let k = k.as_slice();
					let start_ok = match &range.start {
						Bound::Included(s) => k >= s.as_bytes(),
						Bound::Excluded(s) => k > s.as_bytes(),
						Bound::Unbounded => true,
					};
					let end_ok = match &range.end {
						Bound::Included(e) => k <= e.as_bytes(),
						Bound::Excluded(e) => k < e.as_bytes(),
						Bound::Unbounded => true,
					};
					start_ok && end_ok
				})
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect();
			for (k, v) in entries {
				if let Some(limit) = limit
					&& seen >= limit
				{
					break;
				}
				visit(EncodedKey::new(k), v)?;
				seen += 1;
			}
			Ok(())
		}
		fn get_or_create_row_number(
			&mut self,
			group: GroupId,
			key: &EncodedKey,
		) -> ValueResult<(RowNumber, bool)> {
			let slot = (group, key.as_bytes().to_vec());
			if let Some(&row) = self.rows.get(&slot) {
				return Ok((RowNumber(row), false));
			}
			self.next_row += 1;
			self.rows.insert(slot, self.next_row);
			Ok((RowNumber(self.next_row), true))
		}
		fn get_or_create_row_numbers(
			&mut self,
			group: GroupId,
			keys: &[EncodedKey],
		) -> ValueResult<Vec<(RowNumber, bool)>> {
			keys.iter().map(|k| self.get_or_create_row_number(group, k)).collect()
		}
		fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> ValueResult<()> {
			self.rows.remove(&(group, key.as_bytes().to_vec()));
			Ok(())
		}
		fn clock_now_nanos(&self) -> u64 {
			0
		}
	}

	fn kinds() -> Vec<SlotKind> {
		vec![SlotKind::Sum, SlotKind::Sum, SlotKind::Sum]
	}

	fn group_key(hash: &Hash128) -> (GroupId, EncodedKey) {
		(GroupId::NODE_SCOPE, EncodedKey::builder().u128(hash.0).build())
	}

	fn contribution(seq: u64, dollars: [f64; 3]) -> (WindowSlotKey, Vec<Option<Value>>) {
		let coord = WindowSlotKey::new(DateTime::from_timestamp(seq as i64).unwrap(), seq);
		(coord, dollars.iter().map(|d| Some(Value::float8(*d))).collect())
	}

	fn assert_rows_close(legacy: &[Value], runnable: &[Value], context: &str) {
		assert_eq!(legacy.len(), runnable.len(), "row width diverged: {context}");
		for (l, r) in legacy.iter().zip(runnable.iter()) {
			let (Value::Float8(lf), Value::Float8(rf)) = (l, r) else {
				assert_eq!(l, r, "non-float slot diverged: {context}");
				continue;
			};
			let tolerance = lf.value().abs().max(1.0) * 1e-9;
			assert!(
				(lf.value() - rf.value()).abs() <= tolerance,
				"float slot diverged beyond tolerance: legacy={} runnable={} ({context})",
				lf.value(),
				rf.value()
			);
		}
	}

	// The production wiring switches jupiter's pure-sum rolling views onto the
	// running-accumulator engine. This drives the real RowAccumulator (Float8,
	// compensated arithmetic) through both engines on an identical seeded
	// add/retract/expire workload and requires the emitted rows to agree within
	// float tolerance, kinds and cardinality exactly. A divergence means the
	// runnable fast path changes what the views publish.
	#[test]
	fn runnable_row_accumulator_matches_legacy_combine_on_float_churn() {
		let config = || WindowEngineConfig::builder(OperatorStateBudgetHandle::default()).build();
		let mut legacy_store = MockStore::default();
		let mut runnable_store = MockStore::default();
		let mut legacy = RollingEngine::<Hash128, u64, RowAccumulator>::new(config());
		let mut runnable = RollingEngine::<Hash128, u64, RowAccumulator>::new_runnable(config());
		let slot_kinds = kinds();

		let mut state = 0x0123_4567_89AB_CDEFu64;
		let mut roll = |bound: u64| {
			state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			(state >> 33) % bound
		};
		let mut coord_base = 1_000u64;
		let mut cutoff = 0u64;
		let mut added: Vec<(Hash128, u64, [f64; 3])> = Vec::new();

		for round in 0..150u64 {
			let mut plan: Vec<(Hash128, u64, [f64; 3], bool)> = Vec::new();
			for _ in 0..=roll(2) {
				let group = Hash128((roll(4) + 1) as u128);
				let coord = coord_base + roll(30);
				let dollars = [
					(roll(1_000_000_000) as f64) / 100.0,
					(roll(1_000_000) as f64) / 100.0,
					(roll(100) as f64) / 100.0,
				];
				plan.push((group, coord, dollars, true));
				added.push((group, coord, dollars));
			}
			if round % 3 == 2 && !added.is_empty() {
				let (group, coord, dollars) = added.remove(roll(added.len() as u64) as usize);
				plan.push((group, coord, dollars, false));
			}
			let build = |plan: &[(Hash128, u64, [f64; 3], bool)]| {
				let mut buckets: RollingEngineBuckets = TestBTreeMap::new();
				for (group, coord, dollars, is_add) in plan {
					let c = contribution(*coord, *dollars);
					let event = if *is_add {
						AccumulatorEvent::Add(c)
					} else {
						AccumulatorEvent::Remove(c)
					};
					buckets.entry((*group, *coord)).or_default().push(event);
				}
				buckets
			};
			let sk = slot_kinds.clone();
			let legacy_out = legacy
				.apply_evicting(
					&mut legacy_store,
					build(&plan),
					RollingEviction::Before(cutoff),
					group_key,
					|| RowAccumulator::new(&sk, Duration::default()),
					|_g, buffer| combine_rolling(buffer, &sk, 0, Duration::default()),
				)
				.unwrap();
			let sk = slot_kinds.clone();
			let runnable_out = runnable
				.apply_running(
					&mut runnable_store,
					build(&plan),
					RollingEviction::Before(cutoff),
					group_key,
					|| RowAccumulator::new(&sk, Duration::default()),
				)
				.unwrap();
			assert_eq!(legacy_out.len(), runnable_out.len(), "apply cardinality diverged at round {round}");
			for (l, r) in legacy_out.iter().zip(runnable_out.iter()) {
				assert_eq!(l.group, r.group, "apply group order diverged at round {round}");
				assert_eq!(l.kind, r.kind, "apply emit kind diverged at round {round}");
				assert_rows_close(&l.value, &r.value, &format!("apply round {round}"));
			}

			if round % 5 == 4 {
				cutoff = coord_base.saturating_sub(20);
				let sk = slot_kinds.clone();
				let legacy_exp = legacy
					.expire_before(&mut legacy_store, cutoff, |_g, buffer| {
						combine_rolling(buffer, &sk, 0, Duration::default())
					})
					.unwrap();
				let runnable_exp = runnable.expire_before_running(&mut runnable_store, cutoff).unwrap();
				assert_eq!(
					legacy_exp.len(),
					runnable_exp.len(),
					"expiry cardinality diverged at round {round}"
				);
				for (l, r) in legacy_exp.iter().zip(runnable_exp.iter()) {
					match (l, r) {
						(
							RollingExpiry::Update {
								group: lg,
								value: lv,
								..
							},
							RollingExpiry::Update {
								group: rg,
								value: rv,
								..
							},
						) => {
							assert_eq!(lg, rg, "expiry group diverged at round {round}");
							assert_rows_close(lv, rv, &format!("expiry round {round}"));
						}
						(
							RollingExpiry::Remove {
								group: lg,
								..
							},
							RollingExpiry::Remove {
								group: rg,
								..
							},
						) => {
							assert_eq!(lg, rg, "terminal remove diverged at round {round}");
						}
						_ => panic!("expiry kind diverged at round {round}"),
					}
				}
				added.retain(|(_, coord, _)| *coord > cutoff);
			}
			coord_base += roll(10) + 1;
		}

		// Drain both to empty: every group must terminally remove in both
		// engines, leaving no buffers, running entries, or index entries behind.
		let sk = slot_kinds.clone();
		let legacy_final = legacy
			.expire_before(&mut legacy_store, u64::MAX - 1, |_g, buffer| {
				combine_rolling(buffer, &sk, 0, Duration::default())
			})
			.unwrap();
		let runnable_final = runnable.expire_before_running(&mut runnable_store, u64::MAX - 1).unwrap();
		assert_eq!(legacy_final.len(), runnable_final.len(), "terminal drain cardinality diverged");
		assert!(
			runnable_final.iter().all(|e| matches!(e, RollingExpiry::Remove { .. })),
			"draining past every coord must terminally remove all groups"
		);
	}
}
