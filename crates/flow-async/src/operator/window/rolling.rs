// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::{BTreeMap, HashMap};

use arrow_array::RecordBatch;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::interface::change::{Change, Diff};
use reifydb_flow::aggregate::SlotKind;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, duration::Duration, row_number::RowNumber},
};
use tracing::{Span, instrument};

use super::operator::{WindowOperator, required_row_numbers};
use crate::{
	operator::{
		aggregation::{
			accumulator::{RowAccumulator, WindowSlotKey},
			core::{EmitRow, SlotRow},
			engine::{WindowGroups, group_of, intern_window_groups},
		},
		host::HostContext,
		state::seal::{coord::Coord, gate::EvictionGate, ledger::FiredAt},
	},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			daba::Merge,
			pane::{PaneEngine, PaneEvent, PaneGroups, PaneSpans, RollingEmit},
		},
		kind::rolling::RollingOverTime,
		prefetch::Prefetched,
	},
};

pub(crate) type NativePaneEngine = PaneEngine<Hash128, DateTime, RowAccumulator>;

type Contribution = (WindowSlotKey, SlotRow);

type NativeEmit = RollingEmit<Hash128, Vec<Value>, Vec<Value>>;

pub(crate) fn rolling_runs_on_running_total(kinds: &[SlotKind]) -> bool {
	kinds.iter().all(|kind| {
		matches!(
			kind,
			SlotKind::Count { .. }
				| SlotKind::Sum | SlotKind::Avg
				| SlotKind::WindowStart | SlotKind::WindowEnd
				| SlotKind::WindowDuration | SlotKind::Digest { .. }
		)
	})
}

fn floor_to_pane(operator: &WindowOperator, at: DateTime) -> DateTime {
	match operator.rolling_pane() {
		Some(pane) => at.floor_to(pane),
		None => at,
	}
}

fn rolling_over_time(operator: &WindowOperator) -> RollingOverTime {
	RollingOverTime::new(operator.size_duration().unwrap_or_default(), operator.rolling_lag())
}

fn expiry_cutoff(operator: &WindowOperator, clock: DateTime) -> Option<DateTime> {
	rolling_over_time(operator).eviction_cutoff(clock).map(|cutoff| floor_to_pane(operator, cutoff))
}

fn rolling_spans(operator: &WindowOperator) -> PaneSpans<Duration> {
	let lateness = operator.lateness().unwrap_or_else(Duration::zero);
	PaneSpans {
		size: operator.size_duration().unwrap_or_default(),
		lag: operator.rolling_lag(),
		lateness,
		retract: operator.immutable().unwrap_or(lateness),
	}
}

fn pane_engine<'o>(operator: &'o mut WindowOperator, kinds: &[SlotKind]) -> &'o mut NativePaneEngine {
	let config = operator.engine_config();
	let spans = rolling_spans(operator);
	operator.rolling_engine_slot().get_or_insert_with(|| {
		Box::new(if rolling_runs_on_running_total(kinds) {
			NativePaneEngine::running(config, spans)
		} else {
			NativePaneEngine::queue(config, spans, Merge::of())
		})
	})
}

#[instrument(name = "flow::operator::window::intern_partitions", level = "trace", skip_all, fields(partitions = touched.len()))]
fn intern_partitions(touched: &[Hash128]) -> WindowGroups {
	let partitions: Vec<(Hash128, u64)> = touched.iter().map(|hash| (*hash, 0)).collect();
	intern_window_groups(&partitions)
}

struct RoutedRow {
	group: Hash128,
	group_values: Vec<Value>,
	pane: DateTime,
	contribution: Contribution,
}

fn routed_rows(operator: &WindowOperator, columns: &RecordBatch) -> Result<Vec<RoutedRow>> {
	let row_count = columns.num_rows();
	if row_count == 0 {
		return Ok(Vec::new());
	}
	let groups = operator.core.compute_groups(columns)?;
	let row_numbers = required_row_numbers(columns)?;
	let timestamps = operator.row_times(columns, row_count)?;
	let slot_cols = operator.core.evaluate_slot_inputs(columns)?;
	let views = operator.core.slot_views(columns, &slot_cols)?;
	let mut rows = Vec::with_capacity(row_count);
	for (row_idx, (hash, gvals)) in groups.iter().enumerate() {
		let pane = floor_to_pane(operator, timestamps[row_idx]);
		rows.push(RoutedRow {
			group: *hash,
			group_values: gvals.to_vec(),
			pane,
			contribution: (
				WindowSlotKey::new(pane, row_numbers[row_idx].0),
				views.contribution(row_idx, timestamps[row_idx]),
			),
		});
	}
	Ok(rows)
}

#[derive(Default)]
struct Routing {
	events: BTreeMap<Hash128, Vec<PaneEvent<DateTime, Contribution>>>,
	group_values: HashMap<Hash128, Vec<Value>>,
	touched: Vec<Hash128>,
}

impl Routing {
	fn push(&mut self, group: Hash128, group_values: Vec<Value>, event: PaneEvent<DateTime, Contribution>) {
		if !self.events.contains_key(&group) {
			self.touched.push(group);
		}
		self.events.entry(group).or_default().push(event);
		self.group_values.entry(group).or_insert(group_values);
	}
}

fn route_rolling(operator: &WindowOperator, change: &Change) -> Result<Routing> {
	let mut routing = Routing::default();
	for diff in change.diffs.iter() {
		match diff {
			Diff::Insert {
				post,
				..
			} => {
				for row in routed_rows(operator, post)? {
					routing.push(
						row.group,
						row.group_values,
						PaneEvent::Insert(row.pane, row.contribution),
					);
				}
			}
			Diff::Remove {
				pre,
				..
			} => {
				for row in routed_rows(operator, pre)? {
					routing.push(
						row.group,
						row.group_values,
						PaneEvent::Remove(row.pane, row.contribution),
					);
				}
			}
			Diff::Update {
				pre,
				post,
				..
			} => {
				for (before, after) in
					routed_rows(operator, pre)?.into_iter().zip(routed_rows(operator, post)?)
				{
					if before.group == after.group {
						routing.push(
							after.group,
							after.group_values,
							PaneEvent::Update {
								pre: (before.pane, before.contribution),
								post: (after.pane, after.contribution),
							},
						);
					} else {
						routing.push(
							before.group,
							before.group_values,
							PaneEvent::Remove(before.pane, before.contribution),
						);
						routing.push(
							after.group,
							after.group_values,
							PaneEvent::Insert(after.pane, after.contribution),
						);
					}
				}
			}
		}
	}
	Ok(routing)
}

fn finalize_rolling(_group: &Hash128, _newest: DateTime, accumulator: &RowAccumulator) -> Option<Vec<Value>> {
	accumulator.finalize()
}

#[instrument(name = "flow::operator::window::rolling", level = "trace", skip_all)]
pub fn apply_rolling_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let routing = route_rolling(operator, &change)?;
	if routing.events.is_empty() {
		return Ok(Change::from_flow(operator.core.operator, change.version, Vec::new(), change.changed_at));
	}

	let ledger = operator.seal_ledger(host)?.at();
	let clock = host.flow_watermark()?.map_or(ledger, |watermark| ledger.max(watermark));
	let cutoff = expiry_cutoff(operator, clock);

	let groups = intern_partitions(&routing.touched);
	let mut pane_groups: PaneGroups<Hash128, DateTime, Contribution> = BTreeMap::new();
	for (hash, events) in routing.events {
		pane_groups.insert(hash, (group_of(&groups, hash, 0), events));
	}

	let armed_before = pane_engine(operator, &kinds).earliest_expiry(host)?;
	let keys = pane_engine(operator, &kinds).state_keys(&pane_groups);
	let mut state = Prefetched::read(host, keys)?;
	let group_values = routing.group_values;
	let applied = pane_engine(operator, &kinds).apply(
		&mut state,
		pane_groups,
		cutoff,
		&|| RowAccumulator::new(&kinds, None),
		&|hash: &Hash128| group_values.get(hash).cloned().unwrap_or_default(),
		&finalize_rolling,
	)?;
	state.write()?;
	operator.note_sealed_drops(applied.sealed_drops);
	operator.note_stale_retractions(applied.stale_retractions);

	rearm_rolling_seal(operator, host, &kinds, armed_before)?;
	let diffs = rolling_diffs(operator, applied.emits, change.changed_at)?;
	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

fn rearm_rolling_seal(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	kinds: &[SlotKind],
	before: Option<u64>,
) -> Result<()> {
	let after = pane_engine(operator, kinds).earliest_expiry(host)?;
	if before == after {
		return Ok(());
	}
	let gate = EvictionGate::new(rolling_over_time(operator).span());
	gate.rearm(host, &EncodedKey::new(Vec::new()), before, after)
}

fn emit_row(published: &RowNumber, group_values: Vec<Value>, last_value: Vec<Value>) -> EmitRow {
	EmitRow {
		group_values,
		slot_values: last_value,
		row_number: *published,
		span: None,
	}
}

fn rolling_diffs(operator: &mut WindowOperator, emits: Vec<NativeEmit>, changed_at: DateTime) -> Result<Vec<Diff>> {
	let mut inserts = Vec::new();
	let mut updates = Vec::new();
	let mut removes = Vec::new();
	for emit in emits {
		match (emit.prior, emit.post) {
			(None, Some(post)) => {
				inserts.push(emit_row(&RowNumber(post.row_number), post.key, post.last_value))
			}
			(Some(prior), Some(post)) => updates.push((
				emit_row(&RowNumber(prior.row_number), prior.key, prior.last_value),
				emit_row(&RowNumber(post.row_number), post.key, post.last_value),
			)),
			(Some(prior), None) => {
				removes.push(emit_row(&RowNumber(prior.row_number), prior.key, prior.last_value))
			}
			(None, None) => {}
		}
	}
	operator.core.emit_diffs(inserts, updates, removes, changed_at)
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
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let ts = fired.at();
	operator.advance_seal_ledger(host, fired)?;
	let cutoff = expiry_cutoff(operator, ts);
	let armed_before = pane_engine(operator, &kinds).earliest_expiry(host)?;
	let emits = match cutoff {
		Some(cutoff) => pane_engine(operator, &kinds).expire(host, cutoff, &finalize_rolling)?,
		None => Vec::new(),
	};
	Span::current().record("expired", emits.len());
	rearm_rolling_seal(operator, host, &kinds, armed_before)?;
	rolling_diffs(operator, emits, ts)
}

#[cfg(test)]
mod tests {
	use std::{
		collections::{BTreeMap as TestBTreeMap, HashMap as TestHashMap},
		ops::Bound,
	};

	use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
	use reifydb_core::{
		key::operator::state::{GroupId, GroupStateKey, GroupStateRange, KeyspaceMask},
		state::{
			batch::StateBatch,
			per_key,
			timer::{GroupSweep, StateStore, TimerKind, TimerStore},
		},
	};
	use reifydb_value::{
		Result as ValueResult,
		value::{datetime::DateTime, digest::Digest, value_type::ValueType},
	};

	use super::*;
	use crate::{
		operator::{
			aggregation::core::slot_rows::slot_row,
			state::{
				seal::{coord::Coord, rule::EvictionRule},
				test_utils::test::inner_bytes,
			},
		},
		window::engine::config::WindowEngineConfig,
	};

	fn order(millis: i64) -> u64 {
		DateTime::from_millis(millis).to_order()
	}

	fn evict_instant(oldest: u64, span: Duration) -> DateTime {
		EvictionRule::rolling(span).eviction_instant_from_order(oldest).at()
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
		fn state_get(&mut self, keys: &[GroupStateKey]) -> ValueResult<Vec<Option<EncodedPodRow>>> {
			Ok(keys.iter().map(|key| self.state.get(key.as_slice()).cloned()).collect())
		}
		fn state_page_inner(
			&mut self,
			range: GroupStateRange,
			limit: Option<usize>,
		) -> ValueResult<Vec<(GroupStateKey, EncodedPodRow)>> {
			let range = inner_bytes(range);
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

		fn get_row_numbers(
			&mut self,
			group: GroupId,
			keys: &[EncodedKey],
		) -> ValueResult<Vec<Option<RowNumber>>> {
			Ok(keys.iter()
				.map(|key| self.rows.get(&(group, key.as_bytes().to_vec())).map(|&row| RowNumber(row)))
				.collect())
		}

		fn state_remove(&mut self, keys: &[GroupStateKey]) -> ValueResult<()> {
			for key in keys {
				self.state.remove(key.as_slice());
			}
			Ok(())
		}

		fn state_batch(&mut self, keys: Vec<GroupStateKey>) -> ValueResult<StateBatch> {
			per_key::read_batch(self, keys)
		}

		fn state_write_batch(&mut self, batch: StateBatch) -> ValueResult<()> {
			per_key::write_batch(self, batch)
		}

		fn state_set(&mut self, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> ValueResult<()> {
			for (key, payload) in rows {
				self.state.insert(key.as_slice().to_vec(), payload);
			}
			Ok(())
		}

		fn group_sweep(
			&mut self,
			groups: &[GroupId],
			limit: usize,
			keyspaces: KeyspaceMask,
		) -> ValueResult<GroupSweep> {
			per_key::group_sweep(self, groups, limit, keyspaces)
		}

		fn remove_row_number(&mut self, group: GroupId, keys: &[EncodedKey]) -> ValueResult<()> {
			for key in keys {
				self.rows.remove(&(group, key.as_bytes().to_vec()));
			}
			Ok(())
		}

		fn remove_row_number_for_group(&mut self, groups: &[GroupId]) -> ValueResult<()> {
			for group in groups {
				self.rows.remove(&(*group, Vec::new()));
			}
			Ok(())
		}

		fn written_at(&self) -> DateTime {
			DateTime::EPOCH
		}
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

	fn inputs(kinds: &[SlotKind], latency: Option<f64>) -> SlotRow {
		let value = latency.map(Value::float8).unwrap_or_else(Value::none);
		slot_row(kinds.iter().map(|_| Some(value.clone())).collect())
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

	struct Frame {
		live: TestBTreeMap<Hash128, TestBTreeMap<TimeKey, Option<f64>>>,
		newest: TestHashMap<Hash128, u64>,
		lateness: u64,
		retract: u64,
	}

	impl Frame {
		fn admits(&self, group: &Hash128, ms: u64, cutoff: Option<u64>, retraction: bool) -> bool {
			if cutoff.is_some_and(|cutoff| ms <= cutoff) {
				return false;
			}
			let line = if retraction {
				self.retract
			} else {
				self.lateness
			};
			self.newest.get(group).is_none_or(|newest| ms >= newest.saturating_sub(line))
		}

		fn forget_empty(&mut self) {
			let empty: Vec<Hash128> =
				self.live.iter().filter(|(_, rows)| rows.is_empty()).map(|(group, _)| *group).collect();
			for group in empty {
				self.live.remove(&group);
				self.newest.remove(&group);
			}
		}
	}

	fn millis(ms: u64) -> DateTime {
		DateTime::from_millis(i64::try_from(ms).expect("test millis fit in i64"))
	}

	fn drive_time_frame(kinds: &[SlotKind], immutable_ms: Option<u64>, seed: u64) {
		let lateness_ms = 3u64;
		let lateness = Duration::from_milliseconds(lateness_ms as i64).unwrap();
		let retract_ms = immutable_ms.unwrap_or(lateness_ms);
		let retract = Duration::from_milliseconds(retract_ms as i64).unwrap();
		let spans = PaneSpans {
			size: Duration::from_seconds(60).unwrap(),
			lag: Duration::zero(),
			lateness,
			retract,
		};
		let config = WindowEngineConfig::builder().build();
		let mut engine = if rolling_runs_on_running_total(kinds) {
			NativePaneEngine::running(config, spans)
		} else {
			NativePaneEngine::queue(config, spans, Merge::of())
		};
		let mut store = MockStore::default();
		let new_accumulator = || RowAccumulator::new(kinds, None);
		let mut rng = Lcg(seed);
		let mut frame = Frame {
			live: TestBTreeMap::new(),
			newest: TestHashMap::new(),
			lateness: lateness_ms,
			retract: retract_ms,
		};
		let mut evicted: Vec<(Hash128, TimeKey, Option<f64>)> = Vec::new();
		let mut cutoff: Option<u64> = None;
		let (mut base, mut seq, mut checked, mut shared_slots, mut evictions, mut dropped) =
			(1_000u64, 0u64, 0usize, 0usize, 0usize, 0u64);

		for round in 0..300u64 {
			let mut plan: Vec<(Hash128, TimeKey, Option<f64>, bool)> = Vec::new();
			if round % 3 == 2 {
				let held: Vec<(Hash128, TimeKey, Option<f64>)> = frame
					.live
					.iter()
					.flat_map(|(g, rows)| rows.iter().map(move |(k, v)| (*g, *k, *v)))
					.collect();
				for _ in 0..=rng.below(2) {
					if held.is_empty() {
						break;
					}
					let (group, key, value) = held[rng.below(held.len() as u64) as usize];
					if !plan.iter().any(|(g, k, _, _)| *g == group && *k == key) {
						plan.push((group, key, value, false));
					}
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
				plan.push((group, key, rng.latency(), true));
			}

			let mut groups: PaneGroups<Hash128, DateTime, Contribution> = TestBTreeMap::new();
			let mut admitted: Vec<(Hash128, TimeKey, Option<f64>, bool)> = Vec::new();
			let mut round_dropped = 0u64;
			for (group, (ms, seq), value, is_add) in &plan {
				let contribution = (WindowSlotKey::new(millis(*ms), *seq), inputs(kinds, *value));
				let event = if *is_add {
					PaneEvent::Insert(millis(*ms), contribution)
				} else {
					PaneEvent::Remove(millis(*ms), contribution)
				};
				groups.entry(*group)
					.or_insert_with(|| (GroupId::hashed(*group), Vec::new()))
					.1
					.push(event);
				if frame.admits(group, *ms, cutoff, !*is_add) {
					admitted.push((*group, (*ms, *seq), *value, *is_add));
				} else {
					round_dropped += 1;
				}
			}
			for (group, key, value, is_add) in admitted {
				let rows = frame.live.entry(group).or_default();
				if is_add {
					if rows.keys().any(|(ms, _)| *ms == key.0) {
						shared_slots += 1;
					}
					rows.insert(key, value);
				} else {
					rows.remove(&key);
				}
				let newest = frame.newest.entry(group).or_insert(key.0);
				*newest = (*newest).max(key.0);
			}
			frame.forget_empty();

			let applied = engine
				.apply(
					&mut store,
					groups,
					cutoff.map(millis),
					&new_accumulator,
					&|_: &Hash128| Vec::<Value>::new(),
					&finalize_rolling,
				)
				.unwrap();
			dropped += round_dropped;
			assert_eq!(
				applied.sealed_drops + applied.stale_retractions,
				round_dropped,
				"the engine dropped a different number of late rows than the seal and retract lines allow at round {round}"
			);
			for emit in &applied.emits {
				let rows = time_frame_of(&frame.live, &emit.group);
				match &emit.post {
					None => assert!(
						rows.is_empty(),
						"a group with rows in its frame was withdrawn at apply round {round}"
					),
					Some(post) => assert_frame(
						kinds,
						&post.last_value,
						&rows,
						&format!("apply round {round}"),
					),
				}
				checked += 1;
			}

			if round % 5 == 4 {
				let next = base.saturating_sub(20);
				cutoff = Some(next);
				let expiries = engine
					.expire::<Vec<Value>, Vec<Value>>(&mut store, millis(next), &finalize_rolling)
					.unwrap();
				for (group, rows) in frame.live.iter_mut() {
					let kept = rows.split_off(&(next + 1, 0));
					evictions += rows.len();
					evicted.extend(rows.iter().map(|(key, value)| (*group, *key, *value)));
					*rows = kept;
				}
				frame.forget_empty();
				for expiry in &expiries {
					let rows = time_frame_of(&frame.live, &expiry.group);
					match &expiry.post {
						None => assert!(
							rows.is_empty(),
							"a group with rows in its frame expired at round {round}"
						),
						Some(post) => assert_frame(
							kinds,
							&post.last_value,
							&rows,
							&format!("expiry round {round}"),
						),
					}
					checked += 1;
				}
			}
			base += rng.below(4) + 1;
		}

		let drained = engine
			.expire::<Vec<Value>, Vec<Value>>(&mut store, millis(base + 1_000_000), &finalize_rolling)
			.unwrap();
		assert!(
			drained.iter().all(|emit| emit.post.is_none()),
			"draining past every slot must withdraw every group"
		);
		assert!(checked > 500, "the churn must check the frame often, checked {checked}");
		assert!(
			shared_slots > 50,
			"rows must share a slot, or a per-slot answer cannot differ from the frame, shared {shared_slots}"
		);
		assert!(
			evictions > 200,
			"slots must leave the frame, or a pane is never subtracted or popped, evicted {evictions}"
		);
		assert!(dropped > 20, "late rows must reach the seal and retract lines, dropped {dropped}");
	}

	#[test]
	fn a_digest_slot_takes_the_running_path_unless_a_min_or_max_sends_it_to_the_queue() {
		// A min or max on the running path would unmerge a slot that cannot unmerge, so it must take the queue.
		let cases = [
			(vec![digest_kind()], true),
			(vec![SlotKind::Min, digest_kind()], false),
			(vec![digest_kind(), SlotKind::Max], false),
		];
		for (kinds, running) in cases {
			assert_eq!(
				rolling_runs_on_running_total(&kinds),
				running,
				"{kinds:?} picks the wrong rolling fold"
			);
		}
	}

	#[test]
	fn a_running_digest_equals_a_digest_rebuilt_from_the_time_frame_across_seeded_add_evict_churn() {
		// A merge or unmerge that drifts from the frame reports percentiles of rows that already left it.
		let kinds = [digest_kind()];
		assert!(rolling_runs_on_running_total(&kinds), "a lone digest slot must take the running path");
		drive_time_frame(&kinds, None, 0xD16E_5744_0001);
	}

	#[test]
	fn a_digest_next_to_a_min_on_the_queue_equals_a_rebuild_of_the_time_frame() {
		// The queue must answer from exactly the frame a rebuild answers from, with and without an immutable
		// line.
		let kinds = [SlotKind::Min, digest_kind()];
		assert!(!rolling_runs_on_running_total(&kinds), "a min beside a digest must take the queue");
		for immutable in [None, Some(15)] {
			drive_time_frame(&kinds, immutable, 0xD16E_5744_0002);
		}
	}
}
