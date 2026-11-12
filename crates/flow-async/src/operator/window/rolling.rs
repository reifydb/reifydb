// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb_core::{
	common::OnEmpty,
	interface::change::{Change, Diff},
	internal_err,
	key::operator::state::GroupId,
	state::timer::TimerKind,
};
use reifydb_flow::aggregate::SlotKind;
use reifydb_value::{
	Result,
	util::hash::Hash128,
	value::{Value, datetime::DateTime, duration::Duration, row_number::RowNumber, value_type::ValueType},
};
use tracing::{Span, instrument};

use super::{operator::WindowOperator, rows::BuiltinRows};
use crate::{
	operator::{
		aggregation::{accumulator::RowAccumulator, core::EmitRow},
		host::HostContext,
		state::seal::ledger::FiredAt,
	},
	window::{
		accumulator::WindowAccumulator,
		coord::DateTimeCoord,
		driver::rolling::{RollingRunner, RowHooks, route_rolling},
		engine::{
			daba::Merge,
			pane::{PaneEngine, PaneSpans, RollingEmit},
		},
	},
};

pub(crate) type NativePaneEngine = PaneEngine<Hash128, DateTimeCoord, RowAccumulator>;

type NativeEmit = RollingEmit<Hash128, Vec<Value>, Vec<Value>>;

pub(crate) struct BuiltinRolling {
	runner: RollingRunner<DateTimeCoord>,
	engine: NativePaneEngine,
}

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

fn rolling_spans(operator: &WindowOperator) -> PaneSpans<Duration> {
	let lateness = operator.lateness().unwrap_or_else(Duration::zero);
	PaneSpans {
		size: operator.size_duration().unwrap_or_default(),
		lag: operator.rolling_lag(),
		lateness,
		retract: operator.immutable().unwrap_or(lateness),
	}
}

fn rolling_parts<'o>(operator: &'o mut WindowOperator, kinds: &[SlotKind]) -> &'o mut BuiltinRolling {
	let config = operator.engine_config();
	let spans = rolling_spans(operator);
	let runner = RollingRunner::new(operator.core.operator, spans.size, spans.lag, operator.rolling_pane());
	operator.rolling_slot().get_or_insert_with(|| {
		Box::new(BuiltinRolling {
			runner,
			engine: if rolling_runs_on_running_total(kinds) {
				NativePaneEngine::running(config, spans)
			} else {
				NativePaneEngine::queue(config, spans, Merge::of())
			},
		})
	})
}

fn finalize_rolling(_group: &Hash128, _newest: DateTimeCoord, accumulator: &RowAccumulator) -> Option<Vec<Value>> {
	accumulator.finalize()
}

fn group_key(group_values: &HashMap<Hash128, Vec<Value>>, group: &Hash128) -> Result<Vec<Value>> {
	match group_values.get(group) {
		Some(values) => Ok(values.clone()),
		None => internal_err!("a rolling group {group:?} was published without its group values"),
	}
}

fn zero_rolling(kinds: &[SlotKind], types: &[Option<ValueType>], prior: &[Value]) -> Result<Option<Vec<Value>>> {
	if kinds.len() != prior.len() {
		return internal_err!("a rolling row holds {} slots but the window has {}", prior.len(), kinds.len());
	}
	let mut zero = Vec::with_capacity(prior.len());
	for (slot, (kind, value)) in kinds.iter().zip(prior).enumerate() {
		zero.push(match kind {
			SlotKind::Count {
				..
			} => Value::Int8(0),
			SlotKind::Sum => {
				let declared = types.get(slot).cloned().flatten().unwrap_or_else(|| value.get_type());
				match Value::zero_of(declared.inner_type()) {
					Some(zero) => zero,
					None => {
						return internal_err!(
							"a rolling sum slot of type {declared:?} has no zero"
						);
					}
				}
			}
			SlotKind::WindowLast => value.clone(),
			_ => Value::none(),
		});
	}
	Ok((zero != prior).then_some(zero))
}

fn on_empty_of(operator: &WindowOperator) -> OnEmpty {
	match operator.rolling_on_empty() {
		OnEmpty::Zero => OnEmpty::Zero,
		OnEmpty::Remove | OnEmpty::Carry => OnEmpty::Remove,
	}
}

#[instrument(name = "flow::operator::window::rolling", level = "trace", skip_all)]
pub fn apply_rolling_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	change: Change,
) -> Result<Change> {
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let pane = operator.rolling_pane();
	let (routed, group_values) = {
		let mut rows = BuiltinRows::rolling(operator, &change, pane);
		let routed = route_rolling(&mut rows, pane, &|hash: &Hash128| GroupId::window(*hash, 0))?;
		(routed, rows.into_group_values())
	};
	if routed.is_empty() {
		return Ok(Change::from_flow(operator.core.operator, change.version, Vec::new(), change.changed_at));
	}
	let types = operator.core.slot_types().to_vec();
	let empty = |prior: &Vec<Value>| zero_rolling(&kinds, &types, prior);
	let key = |group: &Hash128| group_key(&group_values, group);
	let new_accumulator = || RowAccumulator::new(&kinds, None);
	let mut hooks = RowHooks {
		new_accumulator: &new_accumulator,
		key: Some(&key),
		finalize: &finalize_rolling,
		on_empty: on_empty_of(operator),
		empty: &empty,
	};
	let parts = rolling_parts(operator, &kinds);
	let emits = parts.runner.apply(&mut parts.engine, host, routed, &mut hooks)?;
	let diffs = rolling_diffs(operator, emits, change.changed_at)?;
	Ok(Change::from_flow(operator.core.operator, change.version, diffs, change.changed_at))
}

#[tracing::instrument(name = "flow::window::seal_rolling", level = "debug", skip_all, fields(operator = operator.core.operator.0, expired = tracing::field::Empty))]
pub fn seal_rolling_engine(
	operator: &mut WindowOperator,
	host: &mut dyn HostContext,
	fired: FiredAt,
	kind: TimerKind,
) -> Result<Vec<Diff>> {
	let Some(size) = operator.size_duration() else {
		return Ok(Vec::new());
	};
	if size.is_zero() {
		return Ok(Vec::new());
	}
	let kinds = operator.core.slot_kinds.clone().expect("engine mode requires slot kinds");
	let types = operator.core.slot_types().to_vec();
	let empty = |prior: &Vec<Value>| zero_rolling(&kinds, &types, prior);
	let new_accumulator = || RowAccumulator::new(&kinds, None);
	let mut hooks = RowHooks {
		new_accumulator: &new_accumulator,
		key: None,
		finalize: &finalize_rolling,
		on_empty: on_empty_of(operator),
		empty: &empty,
	};
	let parts = rolling_parts(operator, &kinds);
	let emits = parts.runner.on_timer(&mut parts.engine, host, kind, fired, &mut hooks)?;
	Span::current().record("expired", emits.len());
	rolling_diffs(operator, emits, fired.at())
}

fn emit_row(published: &RowNumber, group_values: Vec<Value>, last_value: Vec<Value>, time: DateTime) -> EmitRow {
	EmitRow {
		group_values,
		slot_values: last_value,
		row_number: *published,
		span: None,
		time: Some(time),
	}
}

fn rolling_diffs(operator: &mut WindowOperator, emits: Vec<NativeEmit>, changed_at: DateTime) -> Result<Vec<Diff>> {
	let mut inserts = Vec::new();
	let mut updates = Vec::new();
	let mut removes = Vec::new();
	for emit in emits {
		match (emit.prior, emit.post) {
			(None, Some(post)) => inserts.push(emit_row(
				&RowNumber(post.row_number),
				post.key,
				post.last_value,
				post.first_time,
			)),
			(Some(prior), Some(post)) => updates.push((
				emit_row(&RowNumber(prior.row_number), prior.key, prior.last_value, prior.first_time),
				emit_row(&RowNumber(post.row_number), post.key, post.last_value, post.first_time),
			)),
			(Some(prior), None) => removes.push(emit_row(
				&RowNumber(prior.row_number),
				prior.key,
				prior.last_value,
				prior.first_time,
			)),
			(None, None) => {}
		}
	}
	operator.core.emit_diffs(inserts, updates, removes, changed_at)
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
			aggregation::{
				accumulator::WindowSlotKey,
				core::{SlotRow, slot_rows::slot_row},
			},
			state::{
				seal::{coord::Coord, rule::EvictionRule},
				test_utils::test::inner_bytes,
			},
		},
		window::engine::{
			config::WindowEngineConfig,
			pane::{PaneEvents, PaneGroups},
			publish::RowPublisher,
		},
	};

	type Contribution = (WindowSlotKey, SlotRow);

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
			DateTimeCoord(armed).saturating_sub_span(span).to_order(),
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

		fn mint_row_numbers(&mut self, count: usize) -> ValueResult<RowNumber> {
			let first = self.next_row + 1;
			self.next_row += count as u64;
			Ok(RowNumber(first))
		}

		fn get_row_numbers_for_groups(&mut self, groups: &[GroupId]) -> ValueResult<Vec<Option<RowNumber>>> {
			Ok(groups
				.iter()
				.map(|group| self.rows.get(&(*group, Vec::new())).map(|&row| RowNumber(row)))
				.collect())
		}

		fn set_row_numbers_for_groups(&mut self, rows: &[(GroupId, RowNumber)]) -> ValueResult<()> {
			for (group, row) in rows {
				self.rows.insert((*group, Vec::new()), row.0);
			}
			Ok(())
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

		fn batch_time(&self) -> DateTime {
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

	fn millis(ms: u64) -> DateTimeCoord {
		DateTimeCoord(DateTime::from_millis(i64::try_from(ms).expect("test millis fit in i64")))
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

			let mut groups: PaneGroups<Hash128, DateTimeCoord, Contribution> = TestBTreeMap::new();
			let mut admitted: Vec<(Hash128, TimeKey, Option<f64>, bool)> = Vec::new();
			let mut round_dropped = 0u64;
			for (group, (ms, seq), value, is_add) in &plan {
				let contribution = (WindowSlotKey::new(millis(*ms), *seq), inputs(kinds, *value));
				let events = &mut groups
					.entry(*group)
					.or_insert_with(|| (GroupId::hashed(*group), PaneEvents::default()))
					.1;
				if *is_add {
					events.insert(millis(*ms), *seq, contribution);
				} else {
					events.remove(millis(*ms), *seq, contribution);
				}
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

			let order: Vec<_> = groups.keys().cloned().collect();
			let applied = engine
				.apply(
					&mut store,
					groups,
					&order,
					cutoff.map(millis),
					&new_accumulator,
					&RowPublisher {
						key: Some(&|_: &Hash128| Ok(Vec::<Value>::new())),
						finalize: &finalize_rolling,
						on_empty: OnEmpty::Remove,
						empty: &|_| Ok(None),
					},
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
					.expire(
						&mut store,
						millis(next),
						&RowPublisher::<_, Vec<Value>, _, _, _> {
							key: None,
							finalize: &finalize_rolling,
							on_empty: OnEmpty::Remove,
							empty: &|_| Ok(None),
						},
					)
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
			.expire(
				&mut store,
				millis(base + 1_000_000),
				&RowPublisher::<_, Vec<Value>, _, _, _> {
					key: None,
					finalize: &finalize_rolling,
					on_empty: OnEmpty::Remove,
					empty: &|_| Ok(None),
				},
			)
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

	#[test]
	fn a_zero_row_is_count_zero_typed_sum_zero_kept_last_time_and_none_for_the_rest() {
		// a wrong zero would publish a quiet pool with stale min or a sum of the wrong type.
		let at = DateTime::from_millis(42_000);
		let kinds = [
			SlotKind::Count {
				count_star: true,
			},
			SlotKind::Sum,
			SlotKind::Sum,
			SlotKind::WindowLast,
			SlotKind::Min,
			SlotKind::Avg,
			SlotKind::Last,
		];
		let types = [None, Some(ValueType::Float8), None, None, None, None, None];
		let prior = vec![
			Value::Int8(3),
			Value::float8(2.5),
			Value::Int4(9),
			Value::DateTime(at),
			Value::Int4(1),
			Value::float8(4.0),
			Value::Int4(7),
		];
		let zero = zero_rolling(&kinds, &types, &prior).unwrap().expect("a non-zero row must change");
		assert_eq!(
			zero,
			vec![
				Value::Int8(0),
				Value::float8(0.0),
				Value::Int4(0),
				Value::DateTime(at),
				Value::none(),
				Value::none(),
				Value::none(),
			]
		);
		assert_eq!(zero_rolling(&kinds, &types, &zero).unwrap(), None, "a zero row must not zero again");
	}

	#[test]
	fn a_sum_with_no_type_to_zero_fails_loud() {
		// writing none for a sum would turn a quiet pool into a missing value instead of zero.
		let err = zero_rolling(&[SlotKind::Sum], &[None], &[Value::none()]).unwrap_err();
		assert!(err.to_string().contains("has no zero"), "{err}");
	}

	#[test]
	fn a_rolling_group_published_without_its_group_values_fails_loud() {
		// Defaulting the missing values publishes a row with empty group columns instead of stopping.
		let err = group_key(&HashMap::new(), &Hash128(7)).unwrap_err();
		assert!(err.to_string().contains("without its group values"), "{err}");
	}
}
