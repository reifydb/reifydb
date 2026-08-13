// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{actors::pending::PendingLayers, interface::catalog::flow::OperatorId, state::store::TimerKind};
use reifydb_flow::{
	timer::Timer,
	transaction::{
		DeferredParams, FlowTransaction,
		deferred::DeferredTransaction,
		substrate::{FlowSubstrate, apply_operator_state},
		timer::*,
	},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{factory::time::at_millis, value::identity::IdentityId};

const NODE: OperatorId = OperatorId(1);
const NO_LIMIT: usize = usize::MAX;

fn deferred(engine: &TestEngine) -> DeferredTransaction {
	deferred_with_clock(engine, MockClock::from_millis(0))
}

fn deferred_with_clock(engine: &TestEngine, clock: MockClock) -> DeferredTransaction {
	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();
	DeferredTransaction::new(DeferredParams {
		version,
		pending: PendingLayers::empty(),
		query: parent.multi.begin_query().unwrap(),
		state_query: parent.multi.begin_query().unwrap(),
		catalog: Catalog::testing(),
		interceptors: Interceptors::new(),
		clock: Clock::Mock(clock),
		substrate: FlowSubstrate {
			operators: engine.inner().operator_state(),
			..FlowSubstrate::default()
		},
	})
}

fn commit_pending(engine: &TestEngine, txn: &mut impl FlowTransaction) {
	// Persists into the operator state store so a cold wheel resolves them from the store.
	let pending = txn.take_pending();
	apply_operator_state(&engine.inner().operator_state(), &pending);
}

fn timer(millis: u64, kind: TimerKind, key: &str) -> Timer {
	Timer {
		at: at_millis(millis),
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

	assert!(wheel.take_due(NODE, &mut txn, at_millis(4_999), NO_LIMIT).unwrap().is_empty(), "must not fire early");
	assert_eq!(
		wheel.take_due(NODE, &mut txn, at_millis(5_000), NO_LIMIT).unwrap(),
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
		wheel.take_due(NODE, &mut txn, at_millis(10_000), NO_LIMIT).unwrap(),
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
		wheel.take_due(NODE, &mut txn, at_millis(10_000), NO_LIMIT).unwrap(),
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
		wheel.take_due(NODE, &mut txn, at_millis(5_000), NO_LIMIT).unwrap(),
		vec![timer(5_000, TimerKind::Maintenance, "m")]
	);

	wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Maintenance, "m")).unwrap();

	assert_eq!(
		wheel.take_due(NODE, &mut txn, at_millis(10_000), NO_LIMIT).unwrap(),
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
		wheel.take_due(NODE, &mut txn, at_millis(10_000), NO_LIMIT).unwrap(),
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

	assert_eq!(wheel.take_due(NODE, &mut txn, at_millis(5_000), NO_LIMIT).unwrap().len(), 1);
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
		wheel.take_due(NODE, &mut txn, at_millis(10_000), 2).unwrap(),
		vec![timer(5_000, TimerKind::Seal, "b"), timer(7_000, TimerKind::Seal, "b")],
		"a capped take must drain in firing order, earliest first"
	);
	assert_eq!(
		wheel.take_due(NODE, &mut txn, at_millis(10_000), NO_LIMIT).unwrap(),
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
		wheel.take_due(NODE, &mut txn, at_millis(9_000), NO_LIMIT).unwrap(),
		vec![timer(8_000, TimerKind::Seal, "session")],
		"the superseded instant must not fire and the re-armed one must"
	);
}

#[test]
fn disarming_either_end_of_the_wheel_leaves_the_other_timer_firing() {
	// A disarm must remove exactly its own instant; taking the neighbour with it drops a seal silently.
	let engine = TestEngine::new();

	let mut txn = deferred(&engine);
	let wheel = TimerWheel::default();
	wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "a")).unwrap();
	wheel.arm(NODE, &mut txn, &timer(8_000, TimerKind::Seal, "b")).unwrap();
	wheel.disarm(NODE, &mut txn, &timer(8_000, TimerKind::Seal, "b")).unwrap();
	assert_eq!(
		wheel.take_due(NODE, &mut txn, at_millis(9_000), NO_LIMIT).unwrap(),
		vec![timer(5_000, TimerKind::Seal, "a")],
		"disarming the later instant must leave the earlier one armed"
	);

	let mut txn = deferred(&engine);
	let wheel = TimerWheel::default();
	wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "a")).unwrap();
	wheel.arm(NODE, &mut txn, &timer(8_000, TimerKind::Seal, "b")).unwrap();
	wheel.disarm(NODE, &mut txn, &timer(5_000, TimerKind::Seal, "a")).unwrap();
	assert_eq!(
		wheel.take_due(NODE, &mut txn, at_millis(9_000), NO_LIMIT).unwrap(),
		vec![timer(8_000, TimerKind::Seal, "b")],
		"disarming the earliest instant must leave the later one armed"
	);
}

#[test]
fn disarming_by_key_cancels_the_instant_the_index_names_and_spares_every_other_key() {
	// A disarm by key must follow the index, never the caller's memory, or the live 8_000 row survives.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let wheel = TimerWheel::default();

	wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Maintenance, "emptied")).unwrap();
	wheel.arm(NODE, &mut txn, &timer(8_000, TimerKind::Maintenance, "emptied")).unwrap();
	wheel.arm(NODE, &mut txn, &timer(6_000, TimerKind::Maintenance, "neighbour")).unwrap();

	wheel.disarm_by_key(NODE, &mut txn, TimerKind::Maintenance, &EncodedKey::new(b"emptied")).unwrap();

	assert_eq!(
		wheel.take_due(NODE, &mut txn, at_millis(10_000), NO_LIMIT).unwrap(),
		vec![timer(6_000, TimerKind::Maintenance, "neighbour")],
		"only the disarmed key's armed instant may go"
	);
}

#[test]
fn a_key_disarmed_by_key_can_be_armed_again_at_the_very_instant_it_held() {
	// An index left behind still names 5_000, so the next arm there is skipped as a duplicate and the group never seals.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let wheel = TimerWheel::default();

	wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Maintenance, "refilled")).unwrap();
	wheel.disarm_by_key(NODE, &mut txn, TimerKind::Maintenance, &EncodedKey::new(b"refilled")).unwrap();
	wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Maintenance, "refilled")).unwrap();

	assert_eq!(
		wheel.take_due(NODE, &mut txn, at_millis(10_000), NO_LIMIT).unwrap(),
		vec![timer(5_000, TimerKind::Maintenance, "refilled")],
		"a re-arm after a disarm by key must arm a live timer, not be swallowed as a duplicate"
	);
}

#[test]
fn disarming_an_unarmed_key_leaves_the_wheel_untouched() {
	// A group can be cleared in the batch that created it, so an unarmed key must disarm to nothing at all.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let wheel = TimerWheel::default();

	wheel.arm(NODE, &mut txn, &timer(5_000, TimerKind::Maintenance, "armed")).unwrap();
	wheel.disarm_by_key(NODE, &mut txn, TimerKind::Maintenance, &EncodedKey::new(b"never-armed")).unwrap();

	assert_eq!(
		wheel.take_due(NODE, &mut txn, at_millis(10_000), NO_LIMIT).unwrap(),
		vec![timer(5_000, TimerKind::Maintenance, "armed")]
	);
}

#[test]
fn a_restart_does_not_fire_a_timer_disarmed_by_key() {
	// The cold wheel reads only the store, so a disarm that lived in RAM alone fires the emptied group again.
	let engine = TestEngine::new();
	let warm = TimerWheel::default();

	let mut txn = deferred(&engine);
	warm.arm(NODE, &mut txn, &timer(5_000, TimerKind::Maintenance, "emptied")).unwrap();
	warm.disarm_by_key(NODE, &mut txn, TimerKind::Maintenance, &EncodedKey::new(b"emptied")).unwrap();
	commit_pending(&engine, &mut txn);

	let mut cold_txn = deferred(&engine);
	let cold = TimerWheel::default();
	assert!(cold.take_due(NODE, &mut cold_txn, at_millis(9_000), NO_LIMIT).unwrap().is_empty());
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
		cold.take_due(NODE, &mut cold_txn, at_millis(9_000), NO_LIMIT).unwrap().is_empty(),
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

	assert_eq!(wheel.take_due(NODE, &mut txn, at_millis(6_000), NO_LIMIT).unwrap().len(), 1);
	assert!(wheel.take_due(NODE, &mut txn, at_millis(6_000), NO_LIMIT).unwrap().is_empty());
	assert_eq!(
		wheel.take_due(NODE, &mut txn, at_millis(9_000), NO_LIMIT).unwrap(),
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
		cold.take_due(NODE, &mut cold_txn, at_millis(5_000), NO_LIMIT).unwrap(),
		vec![timer(5_000, TimerKind::Seal, "bucket")]
	);
}
