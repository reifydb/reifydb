// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{ops::Bound, sync::Arc};

use dashmap::DashMap;
use reifydb_abi::operator::timer::TimerKind;
use reifydb_codec::key::{
	decode_u64_asc, encode_u64_asc,
	encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		EncodableKey,
		operator_group_state::{GroupId, GroupStateKey, Keyspace, OperatorGroupStateKey, keyspace_inner_range},
		operator_state::OperatorStateKey,
	},
};
use reifydb_value::{Result, reifydb_assertions, value::datetime::DateTime};

use super::{
	FlowTransaction,
	group::{decode_payload, encode_payload},
};
use crate::timer::Timer;

const TAKE_CHUNK: usize = 1_024;

fn timer_suffix(at: DateTime, kind: TimerKind, key: &EncodedKey) -> Vec<u8> {
	let mut suffix = Vec::with_capacity(9 + key.len());
	suffix.extend_from_slice(&encode_u64_asc(at.to_millis()));
	suffix.push(kind as u8);
	suffix.extend_from_slice(key.as_ref());
	suffix
}

fn timer_key(at: DateTime, kind: TimerKind, key: &EncodedKey) -> GroupStateKey {
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::TIMER_WHEEL, timer_suffix(at, kind, key))
}

fn index_key(kind: TimerKind, key: &EncodedKey) -> GroupStateKey {
	let mut suffix = Vec::with_capacity(1 + key.len());
	suffix.push(kind as u8);
	suffix.extend_from_slice(key.as_ref());
	OperatorGroupStateKey::inner_encoded(GroupId::NODE_SCOPE, Keyspace::TIMER_INDEX, suffix)
}

fn armed_at(operator: OperatorId, txn: &mut FlowTransaction, index: &GroupStateKey) -> Result<Option<u64>> {
	match txn.state_get(operator, index)? {
		Some(row) => Ok(Some(decode_payload::<u64>(&row)?)),
		None => Ok(None),
	}
}

fn decode_timer(suffix: &[u8]) -> Timer {
	assert!(suffix.len() >= 9, "a timer wheel suffix must carry at least the instant and the kind");
	let at = decode_u64_asc(suffix[..8].try_into().expect("eight instant bytes"));
	let kind = TimerKind::from_u8(suffix[8]).expect("a timer wheel suffix must carry a known kind");
	Timer {
		at: DateTime::from_millis(at),
		kind,
		key: EncodedKey::new(&suffix[9..]),
	}
}

#[derive(Default)]
struct WheelState {
	hydrated: bool,
	earliest: Option<u64>,
}

impl WheelState {
	fn invalidate_if_earliest(&mut self, at: u64) {
		if self.earliest.is_some_and(|earliest| at <= earliest) {
			self.hydrated = false;
			self.earliest = None;
		}
	}
}

#[derive(Clone, Default)]
pub struct TimerWheel {
	inner: Arc<DashMap<OperatorId, WheelState>>,
}

impl TimerWheel {
	pub fn arm(&self, operator: OperatorId, txn: &mut FlowTransaction, timer: &Timer) -> Result<()> {
		let now = txn.clock().now();
		let at = timer.at.to_millis();
		if !timer.kind.is_unique() {
			let mut state = self.inner.entry(operator).or_default();
			if state.hydrated {
				state.earliest = Some(state.earliest.map_or(at, |earliest| earliest.min(at)));
			}
			drop(state);
			txn.state_set(
				operator,
				&timer_key(timer.at, timer.kind, &timer.key),
				encode_payload(&1u64, now)?,
			)?;
			return Ok(());
		}
		let index = index_key(timer.kind, &timer.key);
		let previous = armed_at(operator, txn, &index)?;
		if previous == Some(at) {
			return Ok(());
		}
		if let Some(previous) = previous {
			let stale = timer_key(DateTime::from_millis(previous), timer.kind, &timer.key);
			reifydb_assertions! {
				assert!(
					txn.state_get(operator, &stale)?.is_some(),
					"the timer index names an instant the wheel does not hold, so the two have \
					 diverged and the stale wheel row can never be cancelled again; every arm would \
					 then leave a permanent entry and the due probe degrades with it \
					 (operator={}, previous={}, requested={})",
					operator.0,
					previous,
					at
				);
			}
			txn.state_remove(operator, &stale)?;
		}
		{
			let mut state = self.inner.entry(operator).or_default();
			if let Some(previous) = previous {
				state.invalidate_if_earliest(previous);
			}
			if state.hydrated {
				state.earliest = Some(state.earliest.map_or(at, |earliest| earliest.min(at)));
			}
		}
		txn.state_set(operator, &timer_key(timer.at, timer.kind, &timer.key), encode_payload(&1u64, now)?)?;
		txn.state_set(operator, &index, encode_payload(&at, now)?)?;
		Ok(())
	}

	pub fn disarm(&self, operator: OperatorId, txn: &mut FlowTransaction, timer: &Timer) -> Result<()> {
		let at = timer.at.to_millis();
		if timer.kind.is_unique() {
			let index = index_key(timer.kind, &timer.key);
			if armed_at(operator, txn, &index)? == Some(at) {
				txn.state_remove(operator, &index)?;
			}
		}
		self.inner.entry(operator).or_default().invalidate_if_earliest(at);
		txn.state_remove(operator, &timer_key(timer.at, timer.kind, &timer.key))?;
		Ok(())
	}

	pub fn take_due(
		&self,
		operator: OperatorId,
		txn: &mut FlowTransaction,
		watermark: DateTime,
		limit: usize,
	) -> Result<Vec<Timer>> {
		if limit == 0 {
			return Ok(Vec::new());
		}
		{
			let mut state = self.inner.entry(operator).or_default();
			Self::hydrate_once(&mut state, operator, txn)?;
			match state.earliest {
				None => return Ok(Vec::new()),
				Some(earliest) if earliest > watermark.to_millis() => return Ok(Vec::new()),
				Some(_) => {}
			}
			state.hydrated = false;
		}

		let base = keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::TIMER_WHEEL);
		let ceiling = watermark.to_millis();
		let mut due = Vec::new();
		let mut next_earliest = None;
		let mut start = base.start.clone();
		'scan: loop {
			let want = (limit - due.len()).min(TAKE_CHUNK);
			let range = EncodedKeyRange::new(start, base.end.clone());
			let batch = txn.state_range(operator, range, Some(want + 1), "timer::take_due")?;
			let mut last_inner: Option<EncodedKey> = None;
			for item in &batch.items {
				let decoded = OperatorStateKey::decode(&item.key)
					.expect("state_range must return OperatorState keys");
				let inner = OperatorGroupStateKey::decode_inner(&decoded.key)
					.expect("the timer wheel range must yield structured operator state keys");
				let timer = decode_timer(&inner.2);
				if timer.at.to_millis() > ceiling || due.len() == limit {
					next_earliest = Some(timer.at.to_millis());
					break 'scan;
				}
				due.push(timer);
				last_inner = Some(EncodedKey::new(decoded.key.clone()));
			}
			if !batch.has_more {
				break;
			}
			let Some(last) = last_inner else {
				break;
			};
			start = Bound::Excluded(last);
		}

		for timer in &due {
			txn.state_remove(operator, &timer_key(timer.at, timer.kind, &timer.key))?;
			if timer.kind.is_unique() {
				let index = index_key(timer.kind, &timer.key);
				if armed_at(operator, txn, &index)? == Some(timer.at.to_millis()) {
					txn.state_remove(operator, &index)?;
				}
			}
		}

		let mut state = self.inner.entry(operator).or_default();
		state.hydrated = true;
		state.earliest = next_earliest;
		Ok(due)
	}

	fn hydrate_once(state: &mut WheelState, operator: OperatorId, txn: &mut FlowTransaction) -> Result<()> {
		if state.hydrated {
			return Ok(());
		}
		state.hydrated = true;
		let range = keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::TIMER_WHEEL);
		let batch = txn.state_range(operator, range, Some(1), "timer::hydrate_probe")?;
		state.earliest = batch.items.first().map(|item| {
			let decoded = OperatorStateKey::decode(&item.key)
				.expect("state_range must return OperatorState keys");
			let inner = OperatorGroupStateKey::decode_inner(&decoded.key)
				.expect("the timer wheel range must yield structured operator state keys");
			decode_timer(&inner.2).at.to_millis()
		});
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use reifydb_catalog::catalog::Catalog;
	use reifydb_core::actors::pending::PendingWrite;
	use reifydb_engine::test_harness::TestEngine;
	use reifydb_runtime::context::clock::{Clock, MockClock};
	use reifydb_transaction::interceptor::interceptors::Interceptors;
	use reifydb_value::value::identity::IdentityId;

	use super::*;

	const NODE: OperatorId = OperatorId(1);
	const NO_LIMIT: usize = usize::MAX;

	fn deferred(engine: &TestEngine) -> FlowTransaction {
		deferred_with_clock(engine, MockClock::from_millis(0))
	}

	fn deferred_with_clock(engine: &TestEngine, clock: MockClock) -> FlowTransaction {
		let parent = engine.begin_admin(IdentityId::system()).unwrap();
		let version = parent.version();
		FlowTransaction::deferred(&parent, version, Catalog::testing(), Interceptors::new(), Clock::Mock(clock))
	}

	fn commit_pending(engine: &TestEngine, txn: &mut FlowTransaction) {
		// Persists the pending writes so a cold wheel resolves them as a restarted process would.
		let pending = txn.take_pending();
		let mut cmd = engine.begin_command(IdentityId::system()).unwrap();
		cmd.disable_conflict_tracking().unwrap();
		for (k, pw) in pending.iter_sorted() {
			match pw {
				PendingWrite::Set(v) => cmd.set(k, v.clone()).unwrap(),
				PendingWrite::Remove {
					announce: true,
				} => cmd.remove(k).unwrap(),
				PendingWrite::Remove {
					announce: false,
				} => cmd.remove_silent(k).unwrap(),
			};
		}
		cmd.commit_unchecked().unwrap();
	}

	fn at(millis: u64) -> DateTime {
		DateTime::from_millis(millis)
	}

	fn is_hydrated(wheel: &TimerWheel) -> bool {
		// Reads the earliest-instant hint directly: whether a disarm forces the next dispatch to
		// re-scan is the whole point of the cache and is invisible in firing behaviour alone.
		wheel.inner.get(&NODE).expect("the wheel must hold a cache entry once probed").hydrated
	}

	fn earliest_hint(wheel: &TimerWheel) -> Option<u64> {
		// A warm hint that names the wrong instant is worse than a cold one: the dispatch skips the
		// probe and trusts it, so an instant that is too late strands every timer before it.
		wheel.inner.get(&NODE).expect("the wheel must hold a cache entry once probed").earliest
	}

	fn timer(millis: u64, kind: TimerKind, key: &str) -> Timer {
		Timer {
			at: at(millis),
			kind,
			key: EncodedKey::new(key.as_bytes()),
		}
	}

	#[test]
	fn a_timer_is_due_exactly_when_the_watermark_reaches_it() {
		// Firing before the watermark reaches T seals unreached state - silent data loss.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "bucket")).unwrap();

		assert!(wheel.take_due(NODE, &mut txn, at(4_999), NO_LIMIT).unwrap().is_empty(), "must not fire early");
		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(5_000), NO_LIMIT).unwrap(),
			vec![timer(5_000, TimerKind::Seal, "bucket")]
		);
	}

	#[test]
	fn rearming_a_unique_kind_moves_its_deadline_instead_of_minting_a_second_timer() {
		// Maintenance is a sliding deadline that operators re-arm every batch as event time
		// advances. A wheel row is keyed by its instant, so without engine-owned identity each
		// re-arm abandons the previous instant instead of moving it, and the wheel settles at one
		// row per distinct arm across the whole horizon. Every due probe then scans that pile.
		// Only the newest deadline may survive, and it must be the one that fires.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Maintenance, "m")).unwrap();
		wheel.arm(NODE, &mut txn, &timer(6_000, TimerKind::Maintenance, "m")).unwrap();
		wheel.arm(NODE, &mut txn, &timer(7_000, TimerKind::Maintenance, "m")).unwrap();

		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(10_000), NO_LIMIT).unwrap(),
			vec![timer(7_000, TimerKind::Maintenance, "m")],
			"re-arming must move the one deadline, not leave the superseded instants armed"
		);
	}

	#[test]
	fn a_backlog_kind_still_holds_every_instant_it_was_armed_at() {
		// Uniqueness has to be per kind, never universal. Seal timers are per bucket, so a flow
		// catching up after an outage legitimately holds many outstanding seals on one key;
		// collapsing those to the newest would silently drop every earlier bucket. This pins the
		// contrast against the Maintenance case above so the policy cannot be widened by accident.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "m")).unwrap();
		wheel.arm(NODE, &mut txn, &timer(6_000, TimerKind::Seal, "m")).unwrap();

		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(10_000), NO_LIMIT).unwrap(),
			vec![timer(5_000, TimerKind::Seal, "m"), timer(6_000, TimerKind::Seal, "m")],
			"a backlog kind must keep every bucket it was armed for"
		);
	}

	#[test]
	fn a_fired_unique_timer_can_be_armed_again_at_a_later_instant() {
		// take_due deletes the wheel row, so the identity entry has to go with it. Were it left
		// behind it would name an instant the wheel no longer holds: the next arm would try to
		// cancel a row that is not there, and a re-arm landing on that same stale instant would be
		// mistaken for "already armed" and skipped entirely, so the timer would never fire again.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Maintenance, "m")).unwrap();
		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(5_000), NO_LIMIT).unwrap(),
			vec![timer(5_000, TimerKind::Maintenance, "m")]
		);

		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Maintenance, "m")).unwrap();

		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(10_000), NO_LIMIT).unwrap(),
			vec![timer(5_000, TimerKind::Maintenance, "m")],
			"re-arming the instant that just fired must arm a live timer, not be skipped as a duplicate"
		);
	}

	#[test]
	fn due_timers_return_in_at_then_kind_then_key_order() {
		// Due timers return in (at, kind, key) order, which is what makes a replay fire
		// byte-identically. That order falls out of the key encoding, so this pins the encoding too.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		wheel.arm(NODE, &mut txn, &timer(7_000, TimerKind::Grace, "a")).unwrap();
		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Grace, "a")).unwrap();
		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "z")).unwrap();
		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "a")).unwrap();

		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(10_000), NO_LIMIT).unwrap(),
			vec![
				timer(5_000, TimerKind::Seal, "a"),
				timer(5_000, TimerKind::Seal, "z"),
				timer(5_000, TimerKind::Grace, "a"),
				timer(7_000, TimerKind::Grace, "a"),
			]
		);
	}

	#[test]
	fn arming_the_same_timer_twice_fires_once() {
		// Arming the same (at, kind, key) twice is an idempotent overwrite, so per-bucket coalescing
		// does not double-fire. The clock advances between the arms because the realistic bug is a
		// wall-time uniquifier leaking into the wheel key, which a frozen clock would hide.
		let engine = TestEngine::new();
		let clock = MockClock::from_millis(0);
		let mut txn = deferred_with_clock(&engine, clock.clone());
		let wheel = TimerWheel::default();

		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Grace, "group")).unwrap();
		clock.advance_millis(250);
		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Grace, "group")).unwrap();

		assert_eq!(wheel.take_due(NODE, &mut txn, at(5_000), NO_LIMIT).unwrap().len(), 1);
	}

	#[test]
	fn a_capped_take_drains_the_earliest_first_and_leaves_the_rest_armed() {
		// A flow catching up after an outage has every bucket due at once, so a take must be
		// bounded. The cap has to cut in firing order - an arbitrary subset seals a later instant
		// before an earlier one - and must leave the remainder armed rather than dropping it.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		for at_ms in [9_000u64, 5_000, 7_000] {
			wheel.arm(NODE, &mut txn, &timer(at_ms, TimerKind::Seal, "b")).unwrap();
		}

		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(10_000), 2).unwrap(),
			vec![timer(5_000, TimerKind::Seal, "b"), timer(7_000, TimerKind::Seal, "b")],
			"a capped take must drain in firing order, earliest first"
		);
		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(10_000), NO_LIMIT).unwrap(),
			vec![timer(9_000, TimerKind::Seal, "b")],
			"what the cap left behind must still be armed for the next round"
		);
	}

	#[test]
	fn a_drained_take_keeps_the_hint_warm_at_the_first_timer_it_did_not_fire() {
		// The draining scan already passes over the first timer beyond the watermark, so the wheel
		// can keep that instant instead of paying for it twice. Discarding it made every dispatch
		// that found work force the next one to re-probe the whole keyspace, so the cache switched
		// itself off exactly when the wheel was busiest.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "a")).unwrap();
		wheel.arm(NODE, &mut txn, &timer(9_000, TimerKind::Seal, "b")).unwrap();

		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(5_000), NO_LIMIT).unwrap(),
			vec![timer(5_000, TimerKind::Seal, "a")],
			"precondition: the take fires the due timer and leaves the later one armed"
		);

		assert!(is_hydrated(&wheel), "a take that drained work must not leave the hint cold");
		assert_eq!(
			earliest_hint(&wheel),
			Some(9_000),
			"the hint must name the timer the watermark excluded, so the next dispatch skips the probe"
		);
	}

	#[test]
	fn a_capped_take_points_the_hint_at_the_timer_the_cap_left_behind() {
		// The cap stops the scan short of the watermark, so the survivor it names is still due. The
		// hint has to be that instant: naming anything later would make the next dispatch believe
		// nothing is due and strand the timers the cap skipped.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		for at_ms in [9_000u64, 5_000, 7_000] {
			wheel.arm(NODE, &mut txn, &timer(at_ms, TimerKind::Seal, "b")).unwrap();
		}

		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(10_000), 2).unwrap().len(),
			2,
			"precondition: the cap must bite and leave one timer behind"
		);

		assert!(is_hydrated(&wheel), "a capped take must not leave the hint cold either");
		assert_eq!(
			earliest_hint(&wheel),
			Some(9_000),
			"the hint must name the first timer the cap skipped, which is still due"
		);
	}

	#[test]
	fn draining_the_wheel_empty_clears_the_hint_rather_than_stranding_it() {
		// With nothing left the hint must become none, not the instant that was just fired. Keeping
		// a fired instant would make the next dispatch scan a keyspace it has already emptied, and
		// keeping it warm is only safe because none is the honest answer here.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "a")).unwrap();

		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(10_000), NO_LIMIT).unwrap(),
			vec![timer(5_000, TimerKind::Seal, "a")],
			"precondition: the only armed timer fires"
		);

		assert!(is_hydrated(&wheel), "an emptied wheel is still a known wheel");
		assert_eq!(earliest_hint(&wheel), None, "an emptied wheel must report no earliest instant");
		assert!(
			wheel.take_due(NODE, &mut txn, at(20_000), NO_LIMIT).unwrap().is_empty(),
			"the cleared hint must short-circuit the next dispatch instead of re-firing"
		);
	}

	#[test]
	fn a_disarmed_timer_does_not_fire_and_its_replacement_does() {
		// Sealing is activity-based, so every window kind re-arms as its last event time rises;
		// without an exact disarm the wheel accumulates a dead timer per extension. The disarmed
		// instant is the earliest, so this also pins that the earliest-hint still finds the survivor.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "session")).unwrap();
		wheel.disarm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "session")).unwrap();
		wheel.arm(NODE, &mut txn, &timer(8_000, TimerKind::Seal, "session")).unwrap();

		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(9_000), NO_LIMIT).unwrap(),
			vec![timer(8_000, TimerKind::Seal, "session")],
			"the superseded instant must not fire and the re-armed one must"
		);
	}

	#[test]
	fn disarming_a_later_timer_keeps_the_earliest_hint_warm() {
		// Every window re-arm disarms the group's prior seal instant, so a blanket cache drop here
		// made the next dispatch re-scan the wheel keyspace once per operator per version - the
		// probe that dominated batch time. A removal above the minimum cannot move the minimum, so
		// the hint must survive it while the disarmed instant still must not fire.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "a")).unwrap();
		wheel.arm(NODE, &mut txn, &timer(8_000, TimerKind::Seal, "b")).unwrap();
		assert!(
			wheel.take_due(NODE, &mut txn, at(1_000), NO_LIMIT).unwrap().is_empty(),
			"precondition: nothing is due yet, and this probe is what warms the hint"
		);
		assert!(is_hydrated(&wheel), "precondition: the probe left the hint warm");

		wheel.disarm(NODE, &mut txn, &timer(8_000, TimerKind::Seal, "b")).unwrap();

		assert!(is_hydrated(&wheel), "disarming above the minimum must not force a re-scan");
		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(9_000), NO_LIMIT).unwrap(),
			vec![timer(5_000, TimerKind::Seal, "a")],
			"the kept hint must still fire the survivor and never the disarmed instant"
		);
	}

	#[test]
	fn disarming_the_earliest_timer_drops_the_hint() {
		// Removing the minimum leaves the wheel with no way to name the next one, so the hint has
		// to go. Keeping it would gate later dispatches on an instant that is no longer armed.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "a")).unwrap();
		wheel.arm(NODE, &mut txn, &timer(8_000, TimerKind::Seal, "b")).unwrap();
		assert!(wheel.take_due(NODE, &mut txn, at(1_000), NO_LIMIT).unwrap().is_empty());
		assert!(is_hydrated(&wheel), "precondition: the probe left the hint warm");

		wheel.disarm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "a")).unwrap();

		assert!(!is_hydrated(&wheel), "disarming the minimum must force a re-scan");
		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(9_000), NO_LIMIT).unwrap(),
			vec![timer(8_000, TimerKind::Seal, "b")]
		);
	}

	#[test]
	fn a_restart_does_not_fire_a_disarmed_timer() {
		// A disarm is durable, not a RAM-only retraction. A session extended just before a crash
		// would otherwise seal twice on restart, once for the superseded instant and once for the
		// live one, because the cold wheel reads only what the store holds.
		let engine = TestEngine::new();
		let warm = TimerWheel::default();

		let mut txn = deferred(&engine);
		warm.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "session")).unwrap();
		warm.disarm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "session")).unwrap();
		commit_pending(&engine, &mut txn);

		let mut cold_txn = deferred(&engine);
		let cold = TimerWheel::default();
		assert!(
			cold.take_due(NODE, &mut cold_txn, at(9_000), NO_LIMIT).unwrap().is_empty(),
			"a disarm that only lived in RAM lets the superseded timer survive the restart"
		);
	}

	#[test]
	fn take_due_removes_what_it_returns_and_keeps_the_rest() {
		// take_due removes what it returns inside the same transaction, so exactly-once rests on the
		// removal committing atomically with the firing's effects.
		let engine = TestEngine::new();
		let mut txn = deferred(&engine);
		let wheel = TimerWheel::default();

		wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "due")).unwrap();
		wheel.arm(NODE, &mut txn, &timer(9_000, TimerKind::Seal, "later")).unwrap();

		assert_eq!(wheel.take_due(NODE, &mut txn, at(6_000), NO_LIMIT).unwrap().len(), 1);
		assert!(wheel.take_due(NODE, &mut txn, at(6_000), NO_LIMIT).unwrap().is_empty());
		assert_eq!(
			wheel.take_due(NODE, &mut txn, at(9_000), NO_LIMIT).unwrap(),
			vec![timer(9_000, TimerKind::Seal, "later")]
		);
	}

	#[test]
	fn a_restart_still_fires_persisted_timers() {
		// Armed timers are state, not RAM: a restart must fire what was armed before the crash or
		// every in-flight window seal and grace deadline dies with the process.
		let engine = TestEngine::new();
		let warm = TimerWheel::default();

		let mut txn = deferred(&engine);
		warm.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "bucket")).unwrap();
		commit_pending(&engine, &mut txn);

		let mut cold_txn = deferred(&engine);
		let cold = TimerWheel::default();
		assert_eq!(
			cold.take_due(NODE, &mut cold_txn, at(5_000), NO_LIMIT).unwrap(),
			vec![timer(5_000, TimerKind::Seal, "bucket")]
		);
	}
}
