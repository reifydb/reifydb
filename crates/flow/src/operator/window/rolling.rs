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
			seal::{domain::SealDomain, gate::EvictionGate, ledger::FiredAt, rule::is_sealed},
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

pub(crate) trait RollingDomain: WindowAnchor + SealDomain + Hash + HeapSize + Send + Sync {
	fn engine(
		operator: &mut WindowOperator,
		runnable: bool,
		lag: Self::Span,
	) -> &mut RollingEngine<Hash128, Self, RowAccumulator>;

	fn lag(declared: Duration) -> Self::Span;

	fn eviction(operator: &WindowOperator, ledger: DateTime, lag: Self::Span) -> RollingEviction<Self>;

	fn slot(columns: &Columns, row_idx: usize, timestamps: &[DateTime]) -> Self;

	fn slot_key(slot: Self, row_number: u64) -> WindowSlotKey;

	fn seal_horizon(operator: &WindowOperator, ledger: DateTime) -> Option<Self>;
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

	fn slot(columns: &Columns, row_idx: usize, _timestamps: &[DateTime]) -> OrdinalCoord {
		OrdinalCoord::from_row_number(columns.row_numbers()[row_idx])
	}

	fn slot_key(_slot: OrdinalCoord, row_number: u64) -> WindowSlotKey {
		WindowSlotKey::new(DateTime::default(), row_number)
	}

	fn seal_horizon(_operator: &WindowOperator, _ledger: DateTime) -> Option<OrdinalCoord> {
		None
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

	fn slot(_columns: &Columns, row_idx: usize, timestamps: &[DateTime]) -> DateTime {
		timestamps[row_idx]
	}

	fn slot_key(slot: DateTime, row_number: u64) -> WindowSlotKey {
		WindowSlotKey::new(slot, row_number)
	}

	fn seal_horizon(operator: &WindowOperator, ledger: DateTime) -> Option<DateTime> {
		Some(rolling_over_time(operator, Self::lag(operator.rolling_lag()))
			.seal_horizon(ledger, operator.lateness().unwrap_or_else(Duration::zero)))
	}
}

fn rolling_over_time(operator: &WindowOperator, lag: Duration) -> RollingOverTime {
	RollingOverTime::new(operator.size_duration().unwrap_or_default(), lag)
}

fn rolling_span(operator: &WindowOperator, lag: Duration) -> Duration {
	rolling_over_time(operator, lag).span()
}

type RollingEngineBuckets<S> = RollingBuckets<Hash128, S, (WindowSlotKey, Vec<Option<Value>>)>;

#[instrument(name = "flow::operator::window::intern_partitions", level = "trace", skip_all, fields(partitions = touched.len()))]
fn intern_partitions(touched: &[Hash128]) -> WindowGroups {
	let partitions: Vec<(Hash128, u64)> = touched.iter().map(|hash| (*hash, 0)).collect();
	intern_window_groups(&partitions)
}

fn rolling_runnable(operator: &WindowOperator, kinds: &[SlotKind]) -> bool {
	!operator.is_count_based() && RowAccumulator::invertible(kinds, operator.immutable())
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

fn combine_rolling<S: RollingDomain>(
	buffer: &RollingBuffer<S, RowAccumulator>,
	kinds: &[SlotKind],
	lag: S::Span,
	immutable: Option<Duration>,
) -> Option<Vec<Value>> {
	let (&newest, _) = buffer.iter().next_back()?;
	let aggregate_cutoff = newest.saturating_sub_span(lag);
	let mut merged = RowAccumulator::new(kinds, immutable);
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
fn route_rolling_columns<S: RollingDomain>(
	operator: &WindowOperator,
	columns: &Columns,
	is_add: bool,
	buckets: &mut RollingEngineBuckets<S>,
	group_values: &mut HashMap<Hash128, Vec<Value>>,
	touched: &mut Vec<Hash128>,
	touched_set: &mut HashSet<Hash128>,
) -> Result<()> {
	let row_count = columns.row_count();
	if row_count == 0 {
		return Ok(());
	}
	let groups = operator.core.compute_groups(columns)?;
	let timestamps = if S::arms_timer() || operator.core.needs_event_time() {
		operator.row_times(columns, row_count)?
	} else {
		Vec::new()
	};
	let slot_cols = operator.core.evaluate_slot_inputs(columns)?;
	for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
		let slot = S::slot(columns, row_idx, &timestamps);
		let slot_key = S::slot_key(slot, columns.row_numbers()[row_idx].0);
		let contribution = (
			slot_key,
			operator.core.build_contribution(
				columns,
				&slot_cols,
				row_idx,
				timestamps.get(row_idx).copied().unwrap_or_default(),
			),
		);
		let event = if is_add {
			AccumulatorEvent::Add(contribution)
		} else {
			AccumulatorEvent::Remove(contribution)
		};
		buckets.entry((*hash, slot)).or_default().push(event);
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

fn apply_rolling<S: RollingDomain>(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let immutable = operator.immutable();
	let lag = S::lag(operator.rolling_lag());

	let mut buckets: RollingEngineBuckets<S> = BTreeMap::new();
	let mut group_values: HashMap<Hash128, Vec<Value>> = HashMap::new();
	let mut touched: Vec<Hash128> = Vec::new();
	let mut touched_set: HashSet<Hash128> = HashSet::new();
	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => route_rolling_columns::<S>(
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
			} => route_rolling_columns::<S>(
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
				route_rolling_columns::<S>(
					operator,
					pre,
					false,
					&mut buckets,
					&mut group_values,
					&mut touched,
					&mut touched_set,
				)?;
				route_rolling_columns::<S>(
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
	let eviction = S::eviction(operator, ledger.at(), lag);

	if let Some(horizon) = S::seal_horizon(operator, ledger.at()) {
		let mut dropped = 0u64;
		buckets.retain(|&(_, slot), events| {
			if is_sealed(slot, horizon) {
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
	let armed_before = rolling_earliest_expiry::<S>(operator, host, runnable, lag)?;

	let groups = intern_partitions(&touched);
	let results = if runnable {
		let engine = S::engine(operator, true, lag);
		engine.apply_running(
			host,
			buckets,
			eviction,
			|hash| (group_of(&groups, *hash, 0), store::empty_key()),
			|| RowAccumulator::new(&kinds, immutable),
		)?
	} else {
		let engine = S::engine(operator, false, lag);
		engine.apply_evicting(
			host,
			buckets,
			eviction,
			|hash| (group_of(&groups, *hash, 0), store::empty_key()),
			|| RowAccumulator::new(&kinds, immutable),
			|_g, buffer| combine_rolling::<S>(buffer, &kinds, lag, immutable),
		)?
	};

	rearm_rolling_seal::<S>(operator, host, armed_before, runnable, lag)?;

	let diffs = finish_rolling_results(operator, host, &change, &results, &group_values, &groups)?;
	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

fn rolling_earliest_expiry<S: RollingDomain>(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	runnable: bool,
	lag: S::Span,
) -> Result<Option<S>> {
	Ok(S::engine(operator, runnable, lag).earliest_expiry(host)?.map(S::from_order))
}

fn rearm_rolling_seal<S: RollingDomain>(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	before: Option<S>,
	runnable: bool,
	lag: S::Span,
) -> Result<()> {
	if !S::arms_timer() {
		return Ok(());
	}
	let after = rolling_earliest_expiry::<S>(operator, host, runnable, lag)?;
	if before == after {
		return Ok(());
	}
	let gate = EvictionGate::new(rolling_span(operator, operator.rolling_lag()));
	gate.rearm(host, &EncodedKey::new(Vec::new()), before.map(S::to_order), after.map(S::to_order))
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
					None,
				)?;
				diffs.push(Diff::remove(Columns::from_row(&pre)));
				operator.meta_slot().drop_rolling_meta(host, group_id)?;
			}
			continue;
		}
		let gvals = group_values.get(&r.group).cloned().unwrap_or_default();
		let post = operator.core.build_engine_row(&gvals, &r.value, r.row_number, ts, None)?;
		match (r.kind, prior) {
			(EmitKind::Insert, _) => diffs.push(Diff::insert(Columns::from_row(&post))),
			(_, Some(m)) => {
				let pre = operator.core.build_engine_row(
					&gvals,
					&m.last_value,
					r.row_number,
					ts,
					None,
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
	let immutable = operator.immutable();
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let ts = fired.at();
	operator.advance_seal_ledger(host, fired)?;
	let cutoff = rolling_over_time(operator, lag).eviction_cutoff(ts);
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
					combine_rolling::<DateTime>(buffer, &kinds, lag, immutable)
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
					None,
				)?;
				let post = operator.core.build_engine_row(
					&meta.group_values,
					&value,
					row_number,
					ts,
					None,
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
					None,
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
		sync::Arc,
	};

	use reifydb_codec::{
		key::encoded::{EncodedKey, EncodedKeyRange},
		row::pod::EncodedPodRow,
	};
	use reifydb_core::{
		common::{WindowKind, WindowSize},
		interface::catalog::flow::OperatorId,
		key::operator::state::{GroupId, GroupStateKey},
		state::timer::{StateStore, TimerKind, TimerStore},
	};
	use reifydb_routine_abi::registry::Routines;
	use reifydb_runtime::context::RuntimeContext;
	use reifydb_value::{
		Result as ValueResult,
		value::{datetime::DateTime, digest::Digest, value_type::ValueType},
	};

	use super::*;
	use crate::{
		context::FlowContext,
		operator::{
			state::seal::{coord::Coord, rule::EvictionRule},
			window::operator::{WindowConfig, WindowOperator},
		},
		window::engine::config::WindowEngineConfig,
	};

	fn ordinal(value: u64) -> OrdinalCoord {
		OrdinalCoord::from_arrival_counter(value)
	}

	fn order(millis: u64) -> u64 {
		DateTime::from_millis(millis).to_order()
	}

	fn evict_instant(oldest: u64, span: Duration) -> DateTime {
		EvictionRule::rolling(span).eviction_instant_from_order(oldest).at()
	}

	#[test]
	fn a_count_window_never_seals_and_never_arms_a_timer() {
		// A count window's coordinate is a row number, not an instant, and nothing errors if one
		// is fed to duration arithmetic: the timer lands just past the epoch, fires at once and
		// rearms forever. A count window evicts on capacity and has no notion of closed.
		assert!(
			!<OrdinalCoord as SealDomain>::arms_timer(),
			"a row number is not an instant to arm a timer at"
		);
		assert!(<DateTime as SealDomain>::arms_timer(), "an event-time window does seal on the wheel");

		assert!(
			!<OrdinalCoord as SealDomain>::arms_timer(),
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
		state: TestHashMap<Vec<u8>, EncodedPodRow>,
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

	impl MockStore {
		fn row_number_for(&mut self, group: GroupId, key: &EncodedKey) -> (RowNumber, bool) {
			let slot = (group, key.as_bytes().to_vec());
			if let Some(&row) = self.rows.get(&slot) {
				return (RowNumber(row), false);
			}
			self.next_row += 1;
			self.rows.insert(slot, self.next_row);
			(RowNumber(self.next_row), true)
		}
	}

	impl StateStore for MockStore {
		fn state_get(&mut self, key: &GroupStateKey) -> ValueResult<Option<EncodedPodRow>> {
			Ok(self.state.get(key.as_slice()).cloned())
		}
		fn state_get_many_visit(
			&mut self,
			keys: &[GroupStateKey],
			visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> ValueResult<()>,
		) -> ValueResult<()> {
			for key in keys {
				if let Some(b) = self.state.get(key.as_slice()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}
		fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> ValueResult<()> {
			self.state.insert(key.as_slice().to_vec(), payload);
			Ok(())
		}
		fn state_remove(&mut self, key: &GroupStateKey) -> ValueResult<()> {
			self.state.remove(key.as_slice());
			Ok(())
		}
		fn state_page_inner(
			&mut self,
			range: EncodedKeyRange,
			limit: Option<usize>,
		) -> ValueResult<Vec<(GroupStateKey, EncodedPodRow)>> {
			// The backing map is a HashMap, so without this sort the page order is arbitrary and the real
			// store's key order is not reproduced.
			let mut entries: Vec<(Vec<u8>, EncodedPodRow)> = self
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
			if let Some(limit) = limit {
				entries.truncate(limit);
			}
			Ok(entries
				.into_iter()
				.map(|(k, v)| {
					let k = GroupStateKey::from_framed(EncodedKey::new(k))
						.expect("fake store holds an unframed state key");
					(k, v)
				})
				.collect())
		}
		fn get_or_create_row_numbers(
			&mut self,
			group: GroupId,
			keys: &[EncodedKey],
		) -> ValueResult<Vec<(RowNumber, bool)>> {
			Ok(keys.iter().map(|key| self.row_number_for(group, key)).collect())
		}
		fn get_or_create_row_numbers_for_groups(
			&mut self,
			groups: &[GroupId],
		) -> ValueResult<Vec<(RowNumber, bool)>> {
			Ok(groups
				.iter()
				.map(|group| self.row_number_for(*group, &EncodedKey::new(Vec::new())))
				.collect())
		}
		fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> ValueResult<()> {
			self.rows.remove(&(group, key.as_bytes().to_vec()));
			Ok(())
		}
		fn remove_row_number_for_group(&mut self, group: GroupId) -> ValueResult<()> {
			self.rows.remove(&(group, Vec::new()));
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
		let slot_key = WindowSlotKey::new(DateTime::from_epoch_secs(seq as i64).unwrap(), seq);
		(slot_key, dollars.iter().map(|d| Some(Value::float8(*d))).collect())
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
		let mut slot_base = 1_000u64;
		let mut cutoff = 0u64;
		let mut added: Vec<(Hash128, u64, [f64; 3])> = Vec::new();

		for round in 0..150u64 {
			let mut plan: Vec<(Hash128, u64, [f64; 3], bool)> = Vec::new();
			for _ in 0..=roll(2) {
				let group = Hash128((roll(4) + 1) as u128);
				let slot = slot_base + roll(30);
				let dollars = [
					(roll(1_000_000_000) as f64) / 100.0,
					(roll(1_000_000) as f64) / 100.0,
					(roll(100) as f64) / 100.0,
				];
				plan.push((group, slot, dollars, true));
				added.push((group, slot, dollars));
			}
			if round % 3 == 2 && !added.is_empty() {
				let (group, slot, dollars) = added.remove(roll(added.len() as u64) as usize);
				plan.push((group, slot, dollars, false));
			}
			let build = |plan: &[(Hash128, u64, [f64; 3], bool)]| {
				let mut buckets: RollingEngineBuckets<OrdinalCoord> = TestBTreeMap::new();
				for (group, slot, dollars, is_add) in plan {
					let c = contribution(*slot, *dollars);
					let event = if *is_add {
						AccumulatorEvent::Add(c)
					} else {
						AccumulatorEvent::Remove(c)
					};
					buckets.entry((*group, ordinal(*slot))).or_default().push(event);
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
					|| RowAccumulator::new(&sk, None),
					|_g, buffer| combine_rolling(buffer, &sk, RowSpan::ZERO, None),
				)
				.unwrap();
			let sk = slot_kinds.clone();
			let runnable_out = runnable
				.apply_running(
					&mut runnable_store,
					build(&plan),
					RollingEviction::Before(ordinal(cutoff)),
					group_key,
					|| RowAccumulator::new(&sk, None),
				)
				.unwrap();
			assert_eq!(legacy_out.len(), runnable_out.len(), "apply cardinality diverged at round {round}");
			for (l, r) in legacy_out.iter().zip(runnable_out.iter()) {
				assert_eq!(l.group, r.group, "apply group order diverged at round {round}");
				assert_eq!(l.kind, r.kind, "apply emit kind diverged at round {round}");
				assert_rows_close(&l.value, &r.value, &format!("apply round {round}"));
			}

			if round % 5 == 4 {
				cutoff = slot_base.saturating_sub(20);
				let sk = slot_kinds.clone();
				let legacy_exp = legacy
					.expire_before(&mut legacy_store, ordinal(cutoff), |_g, buffer| {
						combine_rolling(buffer, &sk, RowSpan::ZERO, None)
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
				added.retain(|(_, slot, _)| *slot > cutoff);
			}
			slot_base += roll(10) + 1;
		}

		// Draining to empty must terminally remove every group in both engines, leaving no
		// buffers, running entries or index entries behind.
		let sk = slot_kinds.clone();
		let legacy_final = legacy
			.expire_before(&mut legacy_store, ordinal(u64::MAX - 1), |_g, buffer| {
				combine_rolling(buffer, &sk, RowSpan::ZERO, None)
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

	const PPM: u32 = 10_000;

	type TimeKey = (u64, u64);

	struct Lcg(u64);

	impl Lcg {
		fn below(&mut self, bound: u64) -> u64 {
			self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			(self.0 >> 33) % bound
		}

		fn group(&mut self) -> Hash128 {
			Hash128((self.below(4) + 1) as u128)
		}

		fn latency(&mut self) -> Option<f64> {
			match self.below(10) {
				0 => None,
				1 => Some(0.0),
				2 | 3 => Some(-((self.below(1_000_000) + 1) as f64) / 7.0),
				_ => Some(self.below(1_000_000) as f64 / 3.0),
			}
		}
	}

	fn digest_kind() -> SlotKind {
		SlotKind::Digest {
			accuracy: Some(PPM),
		}
	}

	fn rolling_operator(size: WindowSize, immutable: Option<Duration>) -> WindowOperator {
		WindowOperator::new(WindowConfig {
			parent_schema: None,
			operator: OperatorId(1),
			kind: WindowKind::Rolling {
				size,
				lag: None,
				pane: None,
			},
			group_by: Vec::new(),
			aggregations: Vec::new(),
			runtime_context: RuntimeContext::testing(0, 1),
			routines: Routines::empty(),
			lateness: None,
			immutable,
			ctx: Arc::new(FlowContext::default()),
		})
		.expect("the window operator must build")
	}

	fn minute() -> WindowSize {
		WindowSize::Duration(Duration::from_seconds(60).unwrap())
	}

	fn inputs(kinds: &[SlotKind], latency: Option<f64>) -> Vec<Option<Value>> {
		let value = latency.map(Value::float8).unwrap_or_else(Value::none);
		kinds.iter().map(|_| Some(value.clone())).collect()
	}

	fn assert_frame(kinds: &[SlotKind], output: &[Value], frame: &[Option<f64>], context: &str) {
		let present: Vec<f64> = frame.iter().flatten().copied().collect();
		for (kind, value) in kinds.iter().zip(output) {
			let expected = match kind {
				SlotKind::Digest {
					..
				} => {
					let mut digest = Digest::new(ValueType::Float8, PPM).unwrap();
					for v in &present {
						digest.add_value(&Value::float8(*v)).unwrap();
					}
					if present.is_empty() {
						Value::none()
					} else {
						Value::Digest(Box::new(digest))
					}
				}
				SlotKind::Min => present
					.iter()
					.copied()
					.reduce(f64::min)
					.map(Value::float8)
					.unwrap_or_else(Value::none),
				other => panic!("the frame oracle has no answer for {other:?}"),
			};
			assert_eq!(value, &expected, "{kind:?} diverged from a rebuild of the frame at {context}");
		}
	}

	fn time_frame_of(
		live: &TestBTreeMap<Hash128, TestBTreeMap<TimeKey, Option<f64>>>,
		group: &Hash128,
	) -> Vec<Option<f64>> {
		live.get(group).map(|rows| rows.values().copied().collect()).unwrap_or_default()
	}

	fn drive_time_frame(kinds: &[SlotKind], immutable: Option<Duration>, runnable: bool, seed: u64) {
		let config = WindowEngineConfig::builder().build();
		let mut engine = if runnable {
			RollingEngine::<Hash128, DateTime, RowAccumulator>::new_runnable(config)
		} else {
			RollingEngine::<Hash128, DateTime, RowAccumulator>::new(config)
		};
		let mut store = MockStore::default();
		let new_accumulator = || RowAccumulator::new(kinds, immutable);
		let combine = |_group: &Hash128, buffer: &RollingBuffer<DateTime, RowAccumulator>| {
			combine_rolling::<DateTime>(buffer, kinds, Duration::zero(), immutable)
		};
		let mut rng = Lcg(seed);
		let mut live: TestBTreeMap<Hash128, TestBTreeMap<TimeKey, Option<f64>>> = TestBTreeMap::new();
		let mut evicted: Vec<(Hash128, TimeKey, Option<f64>)> = Vec::new();
		let mut cutoff: Option<u64> = None;
		let (mut base, mut seq, mut checked, mut shared_slots, mut evictions) =
			(1_000u64, 0u64, 0usize, 0usize, 0usize);

		for round in 0..300u64 {
			let mut plan: Vec<(Hash128, TimeKey, Option<f64>, bool)> = Vec::new();
			if round % 3 == 2 {
				for _ in 0..=rng.below(2) {
					let held: Vec<(Hash128, TimeKey)> = live
						.iter()
						.flat_map(|(g, rows)| rows.keys().map(move |k| (*g, *k)))
						.collect();
					if held.is_empty() {
						break;
					}
					let (group, key) = held[rng.below(held.len() as u64) as usize];
					let value = live.get_mut(&group).unwrap().remove(&key).unwrap();
					plan.push((group, key, value, false));
				}
			}
			if round % 7 == 6 && !evicted.is_empty() {
				let (group, key, value) = evicted.swap_remove(rng.below(evicted.len() as u64) as usize);
				plan.push((group, key, value, false));
			}
			for _ in 0..=rng.below(4) {
				seq += 1;
				let group = rng.group();
				let key = (base + rng.below(6), seq);
				let value = rng.latency();
				let rows = live.entry(group).or_default();
				if rows.keys().any(|(ms, _)| *ms == key.0) {
					shared_slots += 1;
				}
				rows.insert(key, value);
				plan.push((group, key, value, true));
			}

			let mut buckets: RollingEngineBuckets<DateTime> = TestBTreeMap::new();
			for (group, (ms, seq), value, is_add) in &plan {
				let contribution =
					(WindowSlotKey::new(DateTime::from_millis(*ms), *seq), inputs(kinds, *value));
				let event = if *is_add {
					AccumulatorEvent::Add(contribution)
				} else {
					AccumulatorEvent::Remove(contribution)
				};
				buckets.entry((*group, DateTime::from_millis(*ms))).or_default().push(event);
			}
			let eviction = match cutoff {
				Some(cutoff) => RollingEviction::Before(DateTime::from_millis(cutoff)),
				None => RollingEviction::Nothing,
			};
			let results = if runnable {
				engine.apply_running(&mut store, buckets, eviction, group_key, &new_accumulator)
					.unwrap()
			} else {
				engine.apply_evicting(
					&mut store,
					buckets,
					eviction,
					group_key,
					&new_accumulator,
					&combine,
				)
				.unwrap()
			};
			for result in &results {
				let frame = time_frame_of(&live, &result.group);
				match result.kind {
					EmitKind::Remove => assert!(
						frame.is_empty(),
						"a group with rows in its frame was withdrawn at apply round {round}"
					),
					_ => assert_frame(
						kinds,
						&result.value,
						&frame,
						&format!("apply round {round}"),
					),
				}
				checked += 1;
			}

			if round % 5 == 4 {
				let next = base.saturating_sub(20);
				cutoff = Some(next);
				let at = DateTime::from_millis(next);
				let expiries = if runnable {
					engine.expire_before_running(&mut store, at).unwrap()
				} else {
					engine.expire_before(&mut store, at, &combine).unwrap()
				};
				for (group, rows) in live.iter_mut() {
					let kept = rows.split_off(&(next + 1, 0));
					evictions += rows.len();
					evicted.extend(rows.iter().map(|(key, value)| (*group, *key, *value)));
					*rows = kept;
				}
				for expiry in &expiries {
					match expiry {
						RollingExpiry::Update {
							group,
							value,
							..
						} => assert_frame(
							kinds,
							value,
							&time_frame_of(&live, group),
							&format!("expiry round {round}"),
						),
						RollingExpiry::Remove {
							group,
							..
						} => assert!(
							time_frame_of(&live, group).is_empty(),
							"a group with rows in its frame expired at round {round}"
						),
					}
					checked += 1;
				}
			}
			base += rng.below(4) + 1;
		}

		let past_every_slot = DateTime::from_millis(base + 1_000_000);
		let drained = if runnable {
			engine.expire_before_running(&mut store, past_every_slot).unwrap()
		} else {
			engine.expire_before(&mut store, past_every_slot, &combine).unwrap()
		};
		assert!(
			drained.iter().all(|e| matches!(e, RollingExpiry::Remove { .. })),
			"draining past every slot must withdraw every group"
		);
		assert!(checked > 500, "the churn must check the frame often, checked {checked}");
		assert!(
			shared_slots > 50,
			"rows must share a slot, or a per-slot answer cannot differ from the frame, shared {shared_slots}"
		);
		assert!(
			evictions > 200,
			"slots must leave the frame, or unmerge is never exercised, evicted {evictions}"
		);
	}

	#[test]
	fn a_digest_slot_takes_the_running_path_unless_a_sealed_min_or_max_or_a_count_frame_sends_it_to_recombine() {
		// A sealed extreme on the running path unmerges a slot that cannot unmerge, so it must recombine.
		let immutable = Some(Duration::from_seconds(30).unwrap());
		let cases = [
			(minute(), None, vec![digest_kind()], true),
			(minute(), None, vec![SlotKind::Min, digest_kind()], true),
			(minute(), immutable, vec![digest_kind()], true),
			(minute(), immutable, vec![SlotKind::Min, digest_kind()], false),
			(minute(), immutable, vec![digest_kind(), SlotKind::Max], false),
			(WindowSize::Count(8), None, vec![digest_kind()], false),
		];
		for (size, immutable, kinds, runnable) in cases {
			let operator = rolling_operator(size.clone(), immutable);
			assert_eq!(
				rolling_runnable(&operator, &kinds),
				runnable,
				"{kinds:?} over {size:?} with immutable {immutable:?}"
			);
		}
	}

	#[test]
	fn a_running_digest_equals_a_digest_rebuilt_from_the_time_frame_across_seeded_add_evict_churn() {
		// A merge or unmerge that drifts from the frame reports percentiles of rows that already left it.
		let kinds = [digest_kind()];
		let runnable = rolling_runnable(&rolling_operator(minute(), None), &kinds);
		assert!(runnable, "a lone digest slot must take the running path");
		drive_time_frame(&kinds, None, runnable, 0xD16E_5744_0001);
	}

	#[test]
	fn a_digest_next_to_a_min_equals_a_rebuild_of_the_time_frame_on_the_running_and_the_recombine_path() {
		// Recombine beside a sealed min must answer from exactly the frame the running path answers from.
		let kinds = [SlotKind::Min, digest_kind()];
		for (immutable, expected) in [(None, true), (Some(Duration::from_milliseconds(15).unwrap()), false)] {
			let runnable = rolling_runnable(&rolling_operator(minute(), immutable), &kinds);
			assert_eq!(runnable, expected, "immutable {immutable:?} picks the wrong rolling path");
			drive_time_frame(&kinds, immutable, runnable, 0xD16E_5744_0002);
		}
	}

	#[test]
	fn a_count_frame_digest_equals_a_digest_rebuilt_from_the_rows_it_holds_across_seeded_churn() {
		// A retraction or update that miscounts a capacity frame reports rows the frame no longer holds.
		const CAPACITY: usize = 6;
		for kinds in [vec![digest_kind()], vec![SlotKind::Min, digest_kind()]] {
			assert!(
				!rolling_runnable(&rolling_operator(WindowSize::Count(CAPACITY as u64), None), &kinds),
				"a count frame must recombine {kinds:?}"
			);
			let mut engine = RollingEngine::<Hash128, OrdinalCoord, RowAccumulator>::new(
				WindowEngineConfig::builder().build(),
			);
			let mut store = MockStore::default();
			let new_accumulator = || RowAccumulator::new(&kinds, None);
			let combine = |_group: &Hash128, buffer: &RollingBuffer<OrdinalCoord, RowAccumulator>| {
				combine_rolling::<OrdinalCoord>(buffer, &kinds, RowSpan::ZERO, None)
			};
			let mut rng = Lcg(0xD16E_5744_0003);
			let mut table: TestBTreeMap<u64, (Hash128, Option<f64>)> = TestBTreeMap::new();
			let mut frames: TestBTreeMap<Hash128, TestBTreeMap<u64, Vec<Option<f64>>>> =
				TestBTreeMap::new();
			let (mut next_row, mut checked, mut evictions, mut reentries) = (0u64, 0usize, 0usize, 0usize);

			for round in 0..400u64 {
				let mut events: TestBTreeMap<(Hash128, u64), Vec<(Option<f64>, bool)>> =
					TestBTreeMap::new();
				for _ in 0..=rng.below(3) {
					next_row += 1;
					let (group, value) = (rng.group(), rng.latency());
					table.insert(next_row, (group, value));
					events.entry((group, next_row)).or_default().push((value, true));
				}
				if round % 3 == 2 {
					let row = *table.keys().nth(rng.below(table.len() as u64) as usize).unwrap();
					let (group, value) = table.remove(&row).unwrap();
					events.entry((group, row)).or_default().push((value, false));
				}
				if round % 4 == 3 {
					let row = *table.keys().nth(rng.below(table.len() as u64) as usize).unwrap();
					let (group, value) = table[&row];
					let moved = if rng.below(3) == 0 {
						rng.group()
					} else {
						group
					};
					let updated = rng.latency();
					events.entry((group, row)).or_default().push((value, false));
					events.entry((moved, row)).or_default().push((updated, true));
					table.insert(row, (moved, updated));
				}

				let mut buckets: RollingEngineBuckets<OrdinalCoord> = TestBTreeMap::new();
				for ((group, row), row_events) in &events {
					let frame = frames.entry(*group).or_default();
					let mut held = frame.remove(row).unwrap_or_default();
					let mut touched = false;
					for (value, is_add) in row_events {
						let contribution = (
							WindowSlotKey::new(DateTime::default(), *row),
							inputs(&kinds, *value),
						);
						let slot = OrdinalCoord::from_row_number(RowNumber(*row));
						if *is_add {
							if held.is_empty()
								&& frame.first_key_value()
									.is_some_and(|(oldest, _)| oldest > row)
							{
								reentries += 1;
							}
							held.push(*value);
							touched = true;
							buckets.entry((*group, slot))
								.or_default()
								.push(AccumulatorEvent::Add(contribution));
						} else {
							buckets.entry((*group, slot))
								.or_default()
								.push(AccumulatorEvent::Remove(contribution));
							if held.is_empty() {
								continue;
							}
							let at = held
								.iter()
								.position(|v| v == value)
								.expect("a retraction names the value its row holds");
							held.swap_remove(at);
							touched = true;
						}
					}
					if !held.is_empty() {
						frame.insert(*row, held);
					}
					if touched {
						while frame.len() > CAPACITY {
							frame.pop_first();
							evictions += 1;
						}
					}
				}

				let results = engine
					.apply_evicting(
						&mut store,
						buckets,
						RollingEviction::Capacity(CAPACITY),
						group_key,
						&new_accumulator,
						&combine,
					)
					.unwrap();
				for result in &results {
					let frame: Vec<Option<f64>> = frames
						.get(&result.group)
						.map(|rows| rows.values().flatten().copied().collect())
						.unwrap_or_default();
					match result.kind {
						EmitKind::Remove => assert!(
							frame.is_empty(),
							"a group with rows in its frame was withdrawn at round {round}"
						),
						_ => assert_frame(
							&kinds,
							&result.value,
							&frame,
							&format!("{kinds:?} round {round}"),
						),
					}
					checked += 1;
				}
			}
			assert!(checked > 500, "the churn must check the frame often, checked {checked}");
			assert!(evictions > 200, "rows must leave the frame by capacity, evicted {evictions}");
			assert!(reentries > 0, "an evicted row updated back into a short frame must be exercised");
		}
	}
}
