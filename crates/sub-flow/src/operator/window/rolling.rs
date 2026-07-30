// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	hash::Hash,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	interface::change::{Change, Diff},
	metrics::heap::HeapSize,
	state::store::StateStore,
	value::column::columns::Columns,
};
use reifydb_engine::flow::aggregate::SlotKind;
use reifydb_flow::{
	transaction::FlowTransaction,
	window::{
		accumulator::WindowAccumulator,
		aux::RollingMeta,
		driver::gate::EvictionGate,
		engine::{
			AccumulatorEvent, EmitKind, is_sealed,
			rolling::{
				RollingBuckets, RollingBuffer, RollingEngine, RollingEviction, RollingExpiry,
				RollingResult,
			},
		},
		kind::rolling::{RollingOverRows, RollingOverTime},
		ledger::FiredAt,
		span::WindowAnchor,
	},
};
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, duration::Duration, row_number::RowNumber},
};
use tracing::Span;

use super::operator::{RollingEngineSlot, WindowOperator};
use crate::operator::{
	aggregation::{
		accumulator::{RowAccumulator, WindowSlotKey},
		engine::{WindowGroups, group_of, intern_window_groups},
	},
	stateful::utils,
	store::OperatorStateStore,
};

pub(crate) trait RollingDomain: WindowAnchor + Hash + HeapSize + Send + Sync {
	#[allow(clippy::mut_from_ref)]
	fn engine(
		operator: &WindowOperator,
		runnable: bool,
		lag: Self::Span,
	) -> &mut RollingEngine<Hash128, Self, RowAccumulator>;

	fn lag(declared: Duration) -> Self::Span;

	fn eviction(operator: &WindowOperator, ledger: DateTime, lag: Self::Span) -> RollingEviction<Self>;

	fn coord(columns: &Columns, row_idx: usize, timestamps: &[DateTime]) -> Self;

	fn slot_key(coord: Self, row_number: u64) -> WindowSlotKey;

	fn seal_horizon(operator: &WindowOperator, ledger: DateTime) -> Option<Self>;

	fn needs_event_timestamps() -> bool;

	fn seals_on_timer() -> bool;
}

impl RollingDomain for u64 {
	fn engine(
		operator: &WindowOperator,
		runnable: bool,
		lag: u64,
	) -> &mut RollingEngine<Hash128, u64, RowAccumulator> {
		counted_row_engine(operator, runnable, lag)
	}

	fn lag(_declared: Duration) -> u64 {
		0
	}

	fn eviction(operator: &WindowOperator, _ledger: DateTime, _lag: u64) -> RollingEviction<u64> {
		RollingEviction::Capacity(RollingOverRows::new(operator.size_count().unwrap_or(0)).capacity())
	}

	fn coord(columns: &Columns, row_idx: usize, _timestamps: &[DateTime]) -> u64 {
		columns.row_numbers()[row_idx].0
	}

	fn slot_key(_coord: u64, row_number: u64) -> WindowSlotKey {
		WindowSlotKey::new(DateTime::default(), row_number)
	}

	fn seal_horizon(_operator: &WindowOperator, _ledger: DateTime) -> Option<u64> {
		None
	}

	fn needs_event_timestamps() -> bool {
		false
	}

	fn seals_on_timer() -> bool {
		false
	}
}

impl RollingDomain for DateTime {
	fn engine(
		operator: &WindowOperator,
		runnable: bool,
		lag: Duration,
	) -> &mut RollingEngine<Hash128, DateTime, RowAccumulator> {
		timed_row_engine(operator, runnable, lag)
	}

	fn lag(declared: Duration) -> Duration {
		declared
	}

	fn eviction(operator: &WindowOperator, ledger: DateTime, lag: Duration) -> RollingEviction<DateTime> {
		RollingEviction::Before(rolling_over_time(operator, lag).eviction_cutoff(ledger))
	}

	fn coord(_columns: &Columns, row_idx: usize, timestamps: &[DateTime]) -> DateTime {
		timestamps[row_idx]
	}

	fn slot_key(coord: DateTime, row_number: u64) -> WindowSlotKey {
		WindowSlotKey::new(coord, row_number)
	}

	fn seal_horizon(operator: &WindowOperator, ledger: DateTime) -> Option<DateTime> {
		Some(rolling_over_time(operator, Self::lag(operator.rolling_lag()))
			.seal_horizon(ledger, operator.grace()))
	}

	fn needs_event_timestamps() -> bool {
		true
	}

	fn seals_on_timer() -> bool {
		true
	}
}

fn rolling_over_time(operator: &WindowOperator, lag: Duration) -> RollingOverTime {
	RollingOverTime::new(operator.size_duration().unwrap_or_default(), lag)
}

fn rolling_span(operator: &WindowOperator, lag: Duration) -> Duration {
	rolling_over_time(operator, lag).span()
}

type RollingEngineBuckets<C> = RollingBuckets<Hash128, C, (WindowSlotKey, Vec<Option<Value>>)>;

fn intern_partitions(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	touched: &[Hash128],
) -> Result<WindowGroups> {
	let partitions: Vec<(Hash128, u64)> = touched.iter().map(|hash| (*hash, 0)).collect();
	intern_window_groups(operator.core.node, txn, &partitions)
}

fn mint_partition_rows(store: &mut OperatorStateStore<'_>, touched: &[Hash128], groups: &WindowGroups) -> Result<()> {
	for hash in touched {
		store.get_or_create_row_number(group_of(groups, *hash, 0), &utils::empty_key())?;
	}
	Ok(())
}

fn rolling_runnable(operator: &WindowOperator, kinds: &[SlotKind]) -> bool {
	!operator.is_count_based() && RowAccumulator::invertible(kinds, operator.grace())
}

fn counted_row_engine(
	operator: &WindowOperator,
	runnable: bool,
	lag: u64,
) -> &mut RollingEngine<Hash128, u64, RowAccumulator> {
	let slot = operator.rolling_engine_slot();
	if !matches!(slot, Some(RollingEngineSlot::CountedRow(_))) {
		let engine = if runnable {
			RollingEngine::new_runnable_group_scoped(operator.engine_config()).with_lag(lag)
		} else {
			RollingEngine::group_scoped(operator.engine_config())
		};
		*slot = Some(RollingEngineSlot::CountedRow(Box::new(engine)));
	}
	match slot {
		Some(RollingEngineSlot::CountedRow(engine)) => engine.as_mut(),
		_ => unreachable!("a count-based rolling window must hold a row-numbered engine"),
	}
}

fn timed_row_engine(
	operator: &WindowOperator,
	runnable: bool,
	lag: Duration,
) -> &mut RollingEngine<Hash128, DateTime, RowAccumulator> {
	let slot = operator.rolling_engine_slot();
	if !matches!(slot, Some(RollingEngineSlot::TimedRow(_))) {
		let engine = if runnable {
			RollingEngine::new_runnable_group_scoped(operator.engine_config()).with_lag(lag)
		} else {
			RollingEngine::group_scoped(operator.engine_config())
		};
		*slot = Some(RollingEngineSlot::TimedRow(Box::new(engine)));
	}
	match slot {
		Some(RollingEngineSlot::TimedRow(engine)) => engine.as_mut(),
		_ => unreachable!("an event-time rolling window must hold an instant-keyed engine"),
	}
}

fn combine_rolling<C: RollingDomain>(
	buffer: &RollingBuffer<C, RowAccumulator>,
	kinds: &[SlotKind],
	lag: C::Span,
	grace: Duration,
) -> Option<Vec<Value>> {
	let (&newest, _) = buffer.iter().next_back()?;
	let aggregate_cutoff = newest.saturating_sub_span(lag);
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
fn route_rolling_columns<C: RollingDomain>(
	operator: &WindowOperator,
	columns: &Columns,
	is_add: bool,
	buckets: &mut RollingEngineBuckets<C>,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	touched: &mut Vec<Hash128>,
	touched_set: &mut HashSet<Hash128>,
) -> Result<()> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(());
	}
	let groups = operator.core.compute_groups(columns)?;
	let timestamps = if C::needs_event_timestamps() {
		operator.row_times(columns, row_count)?
	} else {
		Vec::new()
	};
	let slot_cols = operator.core.evaluate_slot_inputs(columns)?;
	for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
		let coord = C::coord(columns, row_idx, &timestamps);
		let slot_key = C::slot_key(coord, columns.row_numbers()[row_idx].0);
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
	if operator.is_count_based() {
		apply_rolling::<u64>(operator, txn, change)
	} else {
		apply_rolling::<DateTime>(operator, txn, change)
	}
}

fn apply_rolling<C: RollingDomain>(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	change: Change,
) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let grace = operator.grace();
	let lag = C::lag(operator.rolling_lag());

	let mut buckets: RollingEngineBuckets<C> = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut touched: Vec<Hash128> = Vec::new();
	let mut touched_set: HashSet<Hash128> = HashSet::new();
	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => route_rolling_columns::<C>(
				operator,
				post,
				true,
				&mut buckets,
				&mut group_values,
				&mut touched,
				&mut touched_set,
			)?,
			Diff::Remove {
				pre,
				..
			} => route_rolling_columns::<C>(
				operator,
				pre,
				false,
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
				route_rolling_columns::<C>(
					operator,
					pre,
					false,
					&mut buckets,
					&mut group_values,
					&mut touched,
					&mut touched_set,
				)?;
				route_rolling_columns::<C>(
					operator,
					post,
					true,
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

	let ledger = operator.seal_ledger(txn)?;
	let eviction = C::eviction(operator, ledger.at(), lag);

	if let Some(horizon) = C::seal_horizon(operator, ledger.at()) {
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
		let admitted: HashSet<Hash128> = buckets.keys().map(|(hash, _)| *hash).collect();
		touched.retain(|hash| admitted.contains(hash));
		if buckets.is_empty() {
			return Ok(Change::from_flow(
				operator.core.node,
				change.version,
				Vec::new(),
				change.changed_at,
			));
		}
	}

	let runnable = rolling_runnable(operator, &kinds);
	let armed_before = rolling_earliest_expiry::<C>(operator, txn, runnable, lag)?;

	let groups = intern_partitions(operator, txn, &touched)?;
	let results = {
		let mut store = OperatorStateStore::new(txn, operator.core.node);
		mint_partition_rows(&mut store, &touched, &groups)?;
		if runnable {
			let engine = C::engine(operator, true, lag);
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
			let engine = C::engine(operator, false, lag);
			let res = engine.apply_evicting(
				&mut store,
				buckets,
				eviction,
				|hash| (group_of(&groups, *hash, 0), utils::empty_key()),
				|| RowAccumulator::new(&kinds, grace),
				|_g, buffer| combine_rolling::<C>(buffer, &kinds, lag, grace),
			)?;
			engine.flush(&mut store)?;
			res
		}
	};

	rearm_rolling_seal::<C>(operator, txn, armed_before, runnable, lag)?;

	let diffs = finish_rolling_results(operator, txn, &change, &results, &group_values, &groups)?;
	Ok(Change::from_flow(operator.core.node, change.version, diffs, change.changed_at))
}

fn rolling_earliest_expiry<C: RollingDomain>(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	runnable: bool,
	lag: C::Span,
) -> Result<Option<C>> {
	let mut store = OperatorStateStore::new(txn, operator.core.node);
	Ok(C::engine(operator, runnable, lag).earliest_expiry(&mut store)?.map(C::from_order))
}

fn rearm_rolling_seal<C: RollingDomain>(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	before: Option<C>,
	runnable: bool,
	lag: C::Span,
) -> Result<()> {
	if !C::seals_on_timer() {
		return Ok(());
	}
	let after = rolling_earliest_expiry::<C>(operator, txn, runnable, lag)?;
	if before == after {
		return Ok(());
	}
	let node = operator.core.node;
	let gate = EvictionGate::new(rolling_span(operator, operator.rolling_lag()));
	let mut store = OperatorStateStore::new(txn, node);
	gate.rearm(&mut store, &EncodedKey::new(Vec::new()), before.map(C::to_order), after.map(C::to_order))
}

fn finish_rolling_results(
	operator: &WindowOperator,
	txn: &mut FlowTransaction,
	change: &Change,
	results: &[RollingResult<Hash128, Vec<Value>>],
	group_values: &HashMap<Hash128, Vec<Value>>,
	groups: &WindowGroups,
) -> Result<Vec<Diff>> {
	let ts = change.changed_at;
	let time = ts;
	let mut diffs = Vec::new();
	let mut store = OperatorStateStore::new(txn, operator.core.node);
	for r in results {
		let group_id = group_of(groups, r.group, 0);
		let prior = operator.aux_slot().rolling_meta(&mut store, group_id)?;
		if matches!(r.kind, EmitKind::Remove) {
			if let Some(m) = prior {
				let pre = operator.core.build_engine_row(
					&m.group_values,
					&m.last_value,
					RowNumber(m.row_number),
					ts,
					time,
				)?;
				diffs.push(Diff::remove(Columns::from_row(&pre)));
				operator.aux_slot().drop_rolling_meta(&mut store, group_id)?;
			}
			continue;
		}
		let gvals = group_values.get(&r.group).cloned().unwrap_or_default();
		let post = operator.core.build_engine_row(&gvals, &r.value, r.row_number, ts, time)?;
		match prior {
			Some(m) => {
				let pre = operator.core.build_engine_row(
					&gvals,
					&m.last_value,
					r.row_number,
					ts,
					time,
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
	Ok(diffs)
}

#[tracing::instrument(name = "flow::window::seal_rolling", level = "debug", skip_all, fields(node = operator.core.node.0, expired = tracing::field::Empty))]
pub fn seal_rolling_engine(operator: &WindowOperator, txn: &mut FlowTransaction, fired: FiredAt) -> Result<Vec<Diff>> {
	let Some(size) = operator.size_duration() else {
		return Ok(Vec::new());
	};
	if size.is_zero() {
		return Ok(Vec::new());
	}
	let lag = <DateTime as RollingDomain>::lag(operator.rolling_lag());
	let grace = operator.grace();
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let ts = fired.at();
	operator.advance_seal_ledger(txn, fired)?;
	let cutoff = rolling_over_time(operator, lag).eviction_cutoff(ts);
	let time = ts;
	let runnable = rolling_runnable(operator, &kinds);
	let armed_before = rolling_earliest_expiry::<DateTime>(operator, txn, runnable, lag)?;

	let expiries = {
		let mut store = OperatorStateStore::new(txn, operator.core.node);
		if runnable {
			let engine = <DateTime as RollingDomain>::engine(operator, true, lag);
			let res = engine.expire_before_running(&mut store, cutoff)?;
			engine.flush(&mut store)?;
			res
		} else {
			let engine = <DateTime as RollingDomain>::engine(operator, false, lag);
			let res = engine.expire_before(&mut store, cutoff, |_g, buffer| {
				combine_rolling::<DateTime>(buffer, &kinds, lag, grace)
			})?;
			engine.flush(&mut store)?;
			res
		}
	};
	Span::current().record("expired", expiries.len());
	rearm_rolling_seal::<DateTime>(operator, txn, armed_before, runnable, lag)?;

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
					ts,
					time,
				)?;
				let post = operator.core.build_engine_row(
					&meta.group_values,
					&value,
					row_number,
					ts,
					time,
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
					ts,
					time,
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

	use reifydb_abi::operator::timer::TimerKind;
	use reifydb_codec::{
		key::encoded::{EncodedKey, EncodedKeyRange},
		state::StateBytes,
	};
	use reifydb_core::{
		key::operator_state::{GroupId, StateKey},
		state::{budget::OperatorStateBudgetHandle, store::StateStore},
	};
	use reifydb_flow::window::{engine::config::WindowEngineConfig, policy::EvictionPolicy, span::WindowCoord};
	use reifydb_value::{Result as ValueResult, value::datetime::DateTime};

	use super::*;

	fn evict_instant(oldest: u64, span: Duration) -> DateTime {
		EvictionPolicy::rolling(span).eviction_instant_from_order(oldest).at()
	}

	#[test]
	fn a_count_window_never_seals_and_never_arms_a_timer() {
		// This is the defect the domain split closes, and it was silent. rearm_rolling_seal took the
		// oldest coordinate from the expiry index and armed a seal timer at seal_instant(oldest, span).
		// For a time window that oldest coordinate is an instant and the arithmetic is sound. For a
		// count window it is a ROW NUMBER, so the timer landed a few milliseconds after the epoch,
		// fired immediately, and rearmed forever - burning the wheel on a window that has nothing to
		// seal. Nothing errored, because a row number is a perfectly good u64 to hand DateTime::from_millis.
		//
		// A count window holds the last N rows and always has a current value, so it has no notion of
		// "closed": it evicts on capacity when a row arrives, and no passage of time can make a further
		// row inadmissible. Both halves of that are asserted here, because it is the pair that keeps a
		// duration away from a row-numbered coordinate.
		//
		// Mutation: make either of these report like the time domain and the row number is back in the
		// timer wheel.
		assert!(!<u64 as RollingDomain>::seals_on_timer(), "a row number is not an instant to arm a timer at");
		assert!(<DateTime as RollingDomain>::seals_on_timer(), "an event-time window does seal on the wheel");

		assert!(
			!<u64 as RollingDomain>::needs_event_timestamps(),
			"a count window buckets by arrival order, so event time must not reach its coordinate"
		);
	}

	#[test]
	fn the_coordinate_one_span_behind_the_watermark_is_due_to_evict_at_that_watermark() {
		// A rolling window holds (watermark - span, watermark]. Eviction is INCLUSIVE at the low end:
		// seal_rolling_engine expires buffer.range(..=timer.at - span). So the coordinate sitting
		// exactly one span behind the watermark must already be gone, which means its timer has to be
		// armed at exactly coord + span - the wheel fires at `at <= watermark`.
		// Rolling used to borrow tumbling's seal_instant, whose +1 implements a STRICT gate
		// (watermark - last > cutoff) for bucketed windows. That armed at coord + span + 1, one tick
		// past the watermark that justifies the eviction, so the oldest entry on the boundary never
		// expired: interval 5s over ts 1000..10000 summed 45 instead of 40, forever.
		// Mutation: add a millisecond to evict_instant and the boundary coordinate stops being due.
		let span = Duration::from_seconds(5).expect("representable span");
		let watermark = 10_000u64;

		let armed = evict_instant(5_000, span);
		assert!(
			armed.to_order() <= watermark,
			"a coordinate exactly one span behind the watermark must already be due"
		);
		assert_eq!(
			armed.saturating_sub_span(span).to_order(),
			5_000,
			"and the cutoff that firing derives must land on that coordinate, not past it"
		);
		assert!(
			evict_instant(5_001, span).to_order() > watermark,
			"one millisecond newer is still inside the window and must not be armed yet"
		);
	}

	#[test]
	fn a_count_window_reports_no_lag_even_when_one_is_declared() {
		// lag is declared as a duration. In the time domain it shifts the aggregate cutoff back from the
		// newest instant; in the count domain the coordinate is a row number, and subtracting
		// milliseconds from it would silently drop rows in proportion to the lag in MILLIseconds - a
		// 30s lag would demand 30000 rows of headroom before anything aggregated.
		// grace() already guarded this way; rolling_lag did not, which is why the guard now lives in the
		// domain rather than in whoever remembers to check is_count_based first.
		// Mutation: return the declared duration for the counted domain and the units cross again.
		let declared = Duration::from_seconds(30).expect("representable span");

		assert_eq!(<u64 as RollingDomain>::lag(declared), 0, "a row count has no millisecond lag");
		assert_eq!(
			<DateTime as RollingDomain>::lag(declared),
			declared,
			"the time domain honours the lag it was given"
		);
	}

	// Minimal in-memory StateStore so the differential runs the real engine
	// paths (buffers, running entries, expiry index) without a FlowTransaction.
	#[derive(Default)]
	struct MockStore {
		state: TestHashMap<Vec<u8>, StateBytes>,
		groups: TestHashMap<Vec<u8>, GroupId>,
		rows: TestHashMap<(GroupId, Vec<u8>), u64>,
		next_row: u64,
	}

	impl StateStore for MockStore {
		fn arm_timer(&mut self, _at: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			unreachable!("the window engine never arms timers; only the shell above it does")
		}

		fn disarm_timer(&mut self, _at: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			unreachable!("the window engine never disarms timers; only the shell above it does")
		}

		fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
			Ok(None)
		}

		fn intern_group(&mut self, group: &EncodedKey) -> ValueResult<GroupId> {
			let next = GroupId(self.groups.len() as u64 + GroupId::FIRST.0);
			Ok(*self.groups.entry(group.as_bytes().to_vec()).or_insert(next))
		}

		fn lookup_group(&mut self, group: &EncodedKey) -> ValueResult<Option<GroupId>> {
			Ok(self.groups.get(group.as_bytes()).copied())
		}

		fn state_get(&mut self, key: &StateKey) -> ValueResult<Option<StateBytes>> {
			Ok(self.state.get(key.as_slice()).cloned())
		}
		fn state_get_many_visit(
			&mut self,
			keys: &[StateKey],
			visit: &mut dyn FnMut(StateKey, StateBytes) -> ValueResult<()>,
		) -> ValueResult<()> {
			for key in keys {
				if let Some(b) = self.state.get(key.as_slice()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}
		fn state_set(&mut self, key: &StateKey, payload: StateBytes) -> ValueResult<()> {
			self.state.insert(key.as_slice().to_vec(), payload);
			Ok(())
		}
		fn state_remove(&mut self, key: &StateKey) -> ValueResult<()> {
			self.state.remove(key.as_slice());
			Ok(())
		}
		fn state_range_visit(
			&mut self,
			range: EncodedKeyRange,
			limit: Option<usize>,
			visit: &mut dyn FnMut(StateKey, StateBytes) -> ValueResult<()>,
		) -> ValueResult<()> {
			let mut seen = 0usize;
			let entries: Vec<(Vec<u8>, StateBytes)> = self
				.state
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
				let Some(k) = StateKey::from_framed(EncodedKey::new(k)) else {
					continue;
				};
				visit(k, v)?;
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
		fn clock_now(&self) -> DateTime {
			DateTime::EPOCH
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
				let mut buckets: RollingEngineBuckets<u64> = TestBTreeMap::new();
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
