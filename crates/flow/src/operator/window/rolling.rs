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
	value::column::columns::Columns,
};
use reifydb_rql::flow::aggregate::SlotKind;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, duration::Duration, row_number::RowNumber},
};
use tracing::{Span, instrument};

use super::operator::{RollingEngineSlot, WindowOperator};
use crate::{
	operator::{
		aggregation::{
			accumulator::{RowAccumulator, WindowSlotKey},
			engine::{WindowGroups, group_of, intern_window_groups},
		},
		host::HostContext,
		state::{
			seal::{gate::EvictionGate, ledger::FiredAt, policy::is_sealed},
			store,
		},
	},
	window::{
		accumulator::WindowAccumulator,
		coord::{OrdinalCoord, RowSpan},
		engine::{
			AccumulatorEvent, EmitKind,
			rolling::{
				RollingBuckets, RollingBuffer, RollingEngine, RollingEviction, RollingExpiry,
				RollingResult,
			},
		},
		kind::rolling::{RollingOverRows, RollingOverTime},
		meta::RollingMeta,
		span::WindowAnchor,
	},
};

pub(crate) trait RollingDomain: WindowAnchor + Hash + HeapSize + Send + Sync {
	fn engine(
		operator: &mut WindowOperator,
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

impl RollingDomain for OrdinalCoord {
	fn engine(
		operator: &mut WindowOperator,
		runnable: bool,
		lag: RowSpan,
	) -> &mut RollingEngine<Hash128, OrdinalCoord, RowAccumulator> {
		counted_row_engine(operator, runnable, lag)
	}

	fn lag(_declared: Duration) -> RowSpan {
		RowSpan::ZERO
	}

	fn eviction(operator: &WindowOperator, _ledger: DateTime, _lag: RowSpan) -> RollingEviction<OrdinalCoord> {
		RollingEviction::Capacity(
			RollingOverRows::new(RowSpan::of(operator.size_count().unwrap_or(0))).capacity(),
		)
	}

	fn coord(columns: &Columns, row_idx: usize, _timestamps: &[DateTime]) -> OrdinalCoord {
		OrdinalCoord::from_row_number(columns.row_numbers()[row_idx])
	}

	fn slot_key(_coord: OrdinalCoord, row_number: u64) -> WindowSlotKey {
		WindowSlotKey::new(DateTime::default(), row_number)
	}

	fn seal_horizon(_operator: &WindowOperator, _ledger: DateTime) -> Option<OrdinalCoord> {
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
		operator: &mut WindowOperator,
		runnable: bool,
		lag: Duration,
	) -> &mut RollingEngine<Hash128, DateTime, RowAccumulator> {
		timed_row_engine(operator, runnable, lag)
	}

	fn lag(declared: Duration) -> Duration {
		declared
	}

	fn eviction(operator: &WindowOperator, ledger: DateTime, lag: Duration) -> RollingEviction<DateTime> {
		match rolling_over_time(operator, lag).eviction_cutoff(ledger) {
			Some(cutoff) => RollingEviction::Before(cutoff),
			None => RollingEviction::Nothing,
		}
	}

	fn coord(_columns: &Columns, row_idx: usize, timestamps: &[DateTime]) -> DateTime {
		timestamps[row_idx]
	}

	fn slot_key(coord: DateTime, row_number: u64) -> WindowSlotKey {
		WindowSlotKey::new(coord, row_number)
	}

	fn seal_horizon(operator: &WindowOperator, ledger: DateTime) -> Option<DateTime> {
		Some(rolling_over_time(operator, Self::lag(operator.rolling_lag()))
			.seal_horizon(ledger, operator.lateness()))
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

#[instrument(name = "flow::operator::window::intern_partitions", level = "trace", skip_all, fields(partitions = touched.len()))]
fn intern_partitions(host: &mut dyn HostContext, touched: &[Hash128]) -> Result<WindowGroups> {
	let partitions: Vec<(Hash128, u64)> = touched.iter().map(|hash| (*hash, 0)).collect();
	intern_window_groups(host, &partitions)
}

fn rolling_runnable(operator: &WindowOperator, kinds: &[SlotKind]) -> bool {
	!operator.is_count_based() && RowAccumulator::invertible(kinds, operator.amendable())
}

fn counted_row_engine(
	operator: &mut WindowOperator,
	runnable: bool,
	lag: RowSpan,
) -> &mut RollingEngine<Hash128, OrdinalCoord, RowAccumulator> {
	let config = operator.engine_config();
	let slot = operator.rolling_engine_slot();
	if !matches!(slot, Some(RollingEngineSlot::CountedRow(_))) {
		let engine = if runnable {
			RollingEngine::new_runnable(config).with_lag(lag)
		} else {
			RollingEngine::new(config)
		};
		*slot = Some(RollingEngineSlot::CountedRow(Box::new(engine)));
	}
	match slot {
		Some(RollingEngineSlot::CountedRow(engine)) => engine.as_mut(),
		_ => unreachable!("a count-based rolling window must hold a row-numbered engine"),
	}
}

fn timed_row_engine(
	operator: &mut WindowOperator,
	runnable: bool,
	lag: Duration,
) -> &mut RollingEngine<Hash128, DateTime, RowAccumulator> {
	let config = operator.engine_config();
	let slot = operator.rolling_engine_slot();
	if !matches!(slot, Some(RollingEngineSlot::TimedRow(_))) {
		let engine = if runnable {
			RollingEngine::new_runnable(config).with_lag(lag)
		} else {
			RollingEngine::new(config)
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
	amendable: Duration,
) -> Option<Vec<Value>> {
	let (&newest, _) = buffer.iter().next_back()?;
	let aggregate_cutoff = newest.saturating_sub_span(lag);
	let mut merged = RowAccumulator::new(kinds, amendable);
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

#[instrument(name = "flow::operator::window::rolling", level = "trace", skip_all)]
pub fn apply_rolling_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	if operator.is_count_based() {
		apply_rolling::<OrdinalCoord>(operator, host, change)
	} else {
		apply_rolling::<DateTime>(operator, host, change)
	}
}

fn apply_rolling<C: RollingDomain>(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let amendable = operator.amendable();
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
		return Ok(Change::from_flow(operator.core.operator, change.version, Vec::new(), change.changed_at));
	}

	let ledger = operator.seal_ledger(host)?;
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
				operator.core.operator,
				change.version,
				Vec::new(),
				change.changed_at,
			));
		}
	}

	let runnable = rolling_runnable(operator, &kinds);
	let armed_before = rolling_earliest_expiry::<C>(operator, host, runnable, lag)?;

	let groups = intern_partitions(host, &touched)?;
	let results = if runnable {
		let engine = C::engine(operator, true, lag);
		engine.apply_running(
			host,
			buckets,
			eviction,
			|hash| (group_of(&groups, *hash, 0), store::empty_key()),
			|| RowAccumulator::new(&kinds, amendable),
		)?
	} else {
		let engine = C::engine(operator, false, lag);
		engine.apply_evicting(
			host,
			buckets,
			eviction,
			|hash| (group_of(&groups, *hash, 0), store::empty_key()),
			|| RowAccumulator::new(&kinds, amendable),
			|_g, buffer| combine_rolling::<C>(buffer, &kinds, lag, amendable),
		)?
	};

	rearm_rolling_seal::<C>(operator, host, armed_before, runnable, lag)?;

	let diffs = finish_rolling_results(operator, host, &change, &results, &group_values, &groups)?;
	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

fn rolling_earliest_expiry<C: RollingDomain>(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	runnable: bool,
	lag: C::Span,
) -> Result<Option<C>> {
	Ok(C::engine(operator, runnable, lag).earliest_expiry(host)?.map(C::from_order))
}

fn rearm_rolling_seal<C: RollingDomain>(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	before: Option<C>,
	runnable: bool,
	lag: C::Span,
) -> Result<()> {
	if !C::seals_on_timer() {
		return Ok(());
	}
	let after = rolling_earliest_expiry::<C>(operator, host, runnable, lag)?;
	if before == after {
		return Ok(());
	}
	let gate = EvictionGate::new(rolling_span(operator, operator.rolling_lag()));
	gate.rearm(host, &EncodedKey::new(Vec::new()), before.map(C::to_order), after.map(C::to_order))
}

fn finish_rolling_results(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: &Change,
	results: &[RollingResult<Hash128, Vec<Value>>],
	group_values: &HashMap<Hash128, Vec<Value>>,
	groups: &WindowGroups,
) -> Result<Vec<Diff>> {
	let ts = change.changed_at;
	let time = ts;
	let mut diffs = Vec::new();
	for r in results {
		let group_id = group_of(groups, r.group, 0);
		let prior = operator.meta_slot().rolling_meta(host, group_id)?;
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
				operator.meta_slot().drop_rolling_meta(host, group_id)?;
			}
			continue;
		}
		let gvals = group_values.get(&r.group).cloned().unwrap_or_default();
		let post = operator.core.build_engine_row(&gvals, &r.value, r.row_number, ts, time)?;
		match (r.kind, prior) {
			(EmitKind::Insert, _) => diffs.push(Diff::insert(Columns::from_row(&post))),
			(_, Some(m)) => {
				let pre = operator.core.build_engine_row(
					&gvals,
					&m.last_value,
					r.row_number,
					ts,
					time,
				)?;
				diffs.push(Diff::update(Columns::from_row(&pre), Columns::from_row(&post)));
			}
			(_, None) => diffs.push(Diff::update(Columns::from_row(&post), Columns::from_row(&post))),
		}
		operator.meta_slot().put_rolling_meta(
			host,
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

#[tracing::instrument(name = "flow::window::seal_rolling", level = "debug", skip_all, fields(operator = operator.core.operator.0, expired = tracing::field::Empty))]
pub fn seal_rolling_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	fired: FiredAt,
) -> Result<Vec<Diff>> {
	let Some(size) = operator.size_duration() else {
		return Ok(Vec::new());
	};
	if size.is_zero() {
		return Ok(Vec::new());
	}
	let lag = <DateTime as RollingDomain>::lag(operator.rolling_lag());
	let amendable = operator.amendable();
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let ts = fired.at();
	operator.advance_seal_ledger(host, fired)?;
	let cutoff = rolling_over_time(operator, lag).eviction_cutoff(ts);
	let time = ts;
	let runnable = rolling_runnable(operator, &kinds);
	let armed_before = rolling_earliest_expiry::<DateTime>(operator, host, runnable, lag)?;

	let expiries = match cutoff {
		Some(cutoff) => {
			if runnable {
				let engine = <DateTime as RollingDomain>::engine(operator, true, lag);
				engine.expire_before_running(host, cutoff)?
			} else {
				let engine = <DateTime as RollingDomain>::engine(operator, false, lag);
				engine.expire_before(host, cutoff, |_g, buffer| {
					combine_rolling::<DateTime>(buffer, &kinds, lag, amendable)
				})?
			}
		}
		None => Vec::new(),
	};
	Span::current().record("expired", expiries.len());
	rearm_rolling_seal::<DateTime>(operator, host, armed_before, runnable, lag)?;

	let mut diffs = Vec::new();
	for expiry in expiries {
		match expiry {
			RollingExpiry::Update {
				row_number,
				group: _,
				group_id,
				value,
			} => {
				let Some(meta) = operator.meta_slot().rolling_meta(host, group_id)? else {
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
				operator.meta_slot().put_rolling_meta(
					host,
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
				let Some(meta) = operator.meta_slot().rolling_meta(host, group_id)? else {
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
				operator.meta_slot().drop_rolling_meta(host, group_id)?;
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
		row::operator::EncodedOperatorRow,
	};
	use reifydb_core::{
		key::operator_state::{GroupId, GroupStateKey},
		state::store::{StateStore, TimerKind, TimerStore},
	};
	use reifydb_value::{Result as ValueResult, value::datetime::DateTime};

	use super::*;
	use crate::{
		operator::state::seal::{coord::Coord, policy::EvictionPolicy},
		window::engine::config::WindowEngineConfig,
	};

	fn ordinal(value: u64) -> OrdinalCoord {
		OrdinalCoord::from_arrival_counter(value)
	}

	fn order(millis: u64) -> u64 {
		DateTime::from_millis(millis).to_order()
	}

	fn evict_instant(oldest: u64, span: Duration) -> DateTime {
		EvictionPolicy::rolling(span).eviction_instant_from_order(oldest).at()
	}

	#[test]
	fn a_count_window_never_seals_and_never_arms_a_timer() {
		// A count window's coordinate is a row number, not an instant, and nothing errors if one
		// is fed to duration arithmetic: the timer lands just past the epoch, fires at once and
		// rearms forever. A count window evicts on capacity and has no notion of closed.
		assert!(
			!<OrdinalCoord as RollingDomain>::seals_on_timer(),
			"a row number is not an instant to arm a timer at"
		);
		assert!(<DateTime as RollingDomain>::seals_on_timer(), "an event-time window does seal on the wheel");

		assert!(
			!<OrdinalCoord as RollingDomain>::needs_event_timestamps(),
			"a count window buckets by arrival order, so event time must not reach its coordinate"
		);
	}

	#[test]
	fn the_coordinate_one_span_behind_the_watermark_is_due_to_evict_at_that_watermark() {
		// A rolling window holds (watermark - span, watermark] and evicts inclusively at the low
		// end, so the coordinate exactly one span behind must arm at coord + span. Tumbling's
		// strict +1 gate arms one tick late and the boundary entry then never expires.
		let span = Duration::from_seconds(5).expect("representable span");
		let watermark = order(10_000);

		let armed = evict_instant(order(5_000), span);
		assert!(
			armed.to_order() <= watermark,
			"a coordinate exactly one span behind the watermark must already be due"
		);
		assert_eq!(
			armed.saturating_sub_span(span).to_order(),
			order(5_000),
			"and the cutoff that firing derives must land on that coordinate, not past it"
		);
		assert!(
			evict_instant(order(5_001), span).to_order() > watermark,
			"one millisecond newer is still inside the window and must not be armed yet"
		);
	}

	#[test]
	fn a_count_window_reports_no_lag_even_when_one_is_declared() {
		// lag is a duration, and in the count domain the coordinate is a row number: subtracting
		// milliseconds from it would demand 30000 rows of headroom for a 30s lag. The guard lives
		// in the domain rather than in whoever remembers to check the count case first.
		let declared = Duration::from_seconds(30).expect("representable span");

		assert_eq!(
			<OrdinalCoord as RollingDomain>::lag(declared),
			RowSpan::ZERO,
			"a row count has no millisecond lag"
		);
		assert_eq!(
			<DateTime as RollingDomain>::lag(declared),
			declared,
			"the time domain honours the lag it was given"
		);
	}

	/// Minimal in-memory StateStore so the differential runs the real engine paths without a
	/// FlowTransaction.
	#[derive(Default)]
	struct MockStore {
		state: TestHashMap<Vec<u8>, EncodedOperatorRow>,
		groups: TestHashMap<Vec<u8>, GroupId>,
		rows: TestHashMap<(GroupId, Vec<u8>), u64>,
		next_row: u64,
	}

	impl TimerStore for MockStore {
		fn arm_timer(&mut self, _due: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			unreachable!("the window engine never arms timers; only the shell above it does")
		}

		fn disarm_timer(&mut self, _due: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			unreachable!("the window engine never disarms timers; only the shell above it does")
		}

		fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
			Ok(None)
		}
	}

	impl StateStore for MockStore {
		fn intern_group(&mut self, group: &EncodedKey) -> ValueResult<GroupId> {
			let next = GroupId(self.groups.len() as u64 + GroupId::FIRST.0);
			Ok(*self.groups.entry(group.as_bytes().to_vec()).or_insert(next))
		}

		fn lookup_group(&mut self, group: &EncodedKey) -> ValueResult<Option<GroupId>> {
			Ok(self.groups.get(group.as_bytes()).copied())
		}

		fn state_get(&mut self, key: &GroupStateKey) -> ValueResult<Option<EncodedOperatorRow>> {
			Ok(self.state.get(key.as_slice()).cloned())
		}
		fn state_get_many_visit(
			&mut self,
			keys: &[GroupStateKey],
			visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> ValueResult<()>,
		) -> ValueResult<()> {
			for key in keys {
				if let Some(b) = self.state.get(key.as_slice()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}
		fn state_set(&mut self, key: &GroupStateKey, payload: EncodedOperatorRow) -> ValueResult<()> {
			self.state.insert(key.as_slice().to_vec(), payload);
			Ok(())
		}
		fn state_remove(&mut self, key: &GroupStateKey) -> ValueResult<()> {
			self.state.remove(key.as_slice());
			Ok(())
		}
		fn state_range_visit(
			&mut self,
			range: EncodedKeyRange,
			limit: Option<usize>,
			visit: &mut dyn FnMut(GroupStateKey, EncodedOperatorRow) -> ValueResult<()>,
		) -> ValueResult<()> {
			// The backing map is a HashMap, so without this sort the visit order is arbitrary and the real
			// store's key order is not reproduced.
			let mut seen = 0usize;
			let mut entries: Vec<(Vec<u8>, EncodedOperatorRow)> = self
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
			entries.sort_by(|a, b| a.0.cmp(&b.0));
			for (k, v) in entries {
				if let Some(limit) = limit
					&& seen >= limit
				{
					break;
				}
				let k = GroupStateKey::from_framed(EncodedKey::new(k))
					.expect("fake store holds an unframed state key");
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
		fn written_at(&self) -> DateTime {
			DateTime::EPOCH
		}
	}

	fn kinds() -> Vec<SlotKind> {
		vec![SlotKind::Sum, SlotKind::Sum, SlotKind::Sum]
	}

	fn group_key(hash: &Hash128) -> (GroupId, EncodedKey) {
		(GroupId::ROOT, EncodedKey::builder().u128(hash.0).build())
	}

	fn contribution(seq: u64, dollars: [f64; 3]) -> (WindowSlotKey, Vec<Option<Value>>) {
		let coord = WindowSlotKey::new(DateTime::from_epoch_secs(seq as i64).unwrap(), seq);
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

	#[test]
	fn runnable_row_accumulator_matches_legacy_combine_on_float_churn() {
		// Pure-sum rolling views run on the running-accumulator engine, so any divergence from
		// the recombining engine on the same workload changes what those views publish.
		let config = || WindowEngineConfig::builder().build();
		let mut legacy_store = MockStore::default();
		let mut runnable_store = MockStore::default();
		let mut legacy = RollingEngine::<Hash128, OrdinalCoord, RowAccumulator>::new(config());
		let mut runnable = RollingEngine::<Hash128, OrdinalCoord, RowAccumulator>::new_runnable(config());
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
				let mut buckets: RollingEngineBuckets<OrdinalCoord> = TestBTreeMap::new();
				for (group, coord, dollars, is_add) in plan {
					let c = contribution(*coord, *dollars);
					let event = if *is_add {
						AccumulatorEvent::Add(c)
					} else {
						AccumulatorEvent::Remove(c)
					};
					buckets.entry((*group, ordinal(*coord))).or_default().push(event);
				}
				buckets
			};
			let sk = slot_kinds.clone();
			let legacy_out = legacy
				.apply_evicting(
					&mut legacy_store,
					build(&plan),
					RollingEviction::Before(ordinal(cutoff)),
					group_key,
					|| RowAccumulator::new(&sk, Duration::default()),
					|_g, buffer| combine_rolling(buffer, &sk, RowSpan::ZERO, Duration::default()),
				)
				.unwrap();
			let sk = slot_kinds.clone();
			let runnable_out = runnable
				.apply_running(
					&mut runnable_store,
					build(&plan),
					RollingEviction::Before(ordinal(cutoff)),
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
					.expire_before(&mut legacy_store, ordinal(cutoff), |_g, buffer| {
						combine_rolling(buffer, &sk, RowSpan::ZERO, Duration::default())
					})
					.unwrap();
				let runnable_exp =
					runnable.expire_before_running(&mut runnable_store, ordinal(cutoff)).unwrap();
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

		// Draining to empty must terminally remove every group in both engines, leaving no
		// buffers, running entries or index entries behind.
		let sk = slot_kinds.clone();
		let legacy_final = legacy
			.expire_before(&mut legacy_store, ordinal(u64::MAX - 1), |_g, buffer| {
				combine_rolling(buffer, &sk, RowSpan::ZERO, Duration::default())
			})
			.unwrap();
		let runnable_final =
			runnable.expire_before_running(&mut runnable_store, ordinal(u64::MAX - 1)).unwrap();
		assert_eq!(legacy_final.len(), runnable_final.len(), "terminal drain cardinality diverged");
		assert!(
			runnable_final.iter().all(|e| matches!(e, RollingExpiry::Remove { .. })),
			"draining past every coord must terminally remove all groups"
		);
	}
}
