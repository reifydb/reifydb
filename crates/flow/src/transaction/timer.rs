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
use reifydb_value::{Result, value::datetime::DateTime};

use super::{FlowTransaction, group::encode_payload};
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

fn due_range(watermark: DateTime) -> EncodedKeyRange {
	let base = keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::TIMER_WHEEL);
	let bound = OperatorGroupStateKey::inner_encoded(
		GroupId::NODE_SCOPE,
		Keyspace::TIMER_WHEEL,
		encode_u64_asc(watermark.to_millis() + 1),
	);
	EncodedKeyRange::new(base.start, Bound::Excluded(bound.into_encoded()))
}

#[derive(Default)]
struct WheelState {
	hydrated: bool,
	earliest: Option<u64>,
}

#[derive(Clone, Default)]
pub struct TimerWheel {
	inner: Arc<DashMap<OperatorId, WheelState>>,
}

impl TimerWheel {
	pub fn arm(&self, operator: OperatorId, txn: &mut FlowTransaction, timer: &Timer) -> Result<()> {
		let now = txn.clock().now();
		let mut state = self.inner.entry(operator).or_default();
		if state.hydrated {
			let at = timer.at.to_millis();
			state.earliest = Some(state.earliest.map_or(at, |earliest| earliest.min(at)));
		}
		txn.state_set(operator, &timer_key(timer.at, timer.kind, &timer.key), encode_payload(&1u64, now)?)?;
		Ok(())
	}

	pub fn disarm(&self, operator: OperatorId, txn: &mut FlowTransaction, timer: &Timer) -> Result<()> {
		self.inner.entry(operator).or_default().hydrated = false;
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

		let base = due_range(watermark);
		let mut due = Vec::new();
		let mut start = base.start.clone();
		loop {
			let want = (limit - due.len()).min(TAKE_CHUNK);
			let range = EncodedKeyRange::new(start, base.end.clone());
			let batch = txn.state_range(operator, range, Some(want))?;
			let mut last_inner: Option<EncodedKey> = None;
			for item in &batch.items {
				let decoded = OperatorStateKey::decode(&item.key)
					.expect("state_range must return OperatorState keys");
				let inner = OperatorGroupStateKey::decode_inner(&decoded.key)
					.expect("the timer wheel range must yield structured operator state keys");
				due.push(decode_timer(&inner.2));
				last_inner = Some(EncodedKey::new(decoded.key.clone()));
			}
			if due.len() >= limit || !batch.has_more {
				break;
			}
			let Some(last) = last_inner else {
				break;
			};
			start = Bound::Excluded(last);
		}

		for timer in &due {
			txn.state_remove(operator, &timer_key(timer.at, timer.kind, &timer.key))?;
		}
		Ok(due)
	}

	fn hydrate_once(state: &mut WheelState, operator: OperatorId, txn: &mut FlowTransaction) -> Result<()> {
		if state.hydrated {
			return Ok(());
		}
		state.hydrated = true;
		let range = keyspace_inner_range(GroupId::NODE_SCOPE, Keyspace::TIMER_WHEEL);
		let batch = txn.state_range(operator, range, Some(1))?;
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
