// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::{
	interface::{
		catalog::{
			id::QueueId,
			queue::{
				AttemptOutcome, QueueAttemptRecord, QueueItemState, QueueItemStatus,
				QueuePartitionCounters, decode_queue_attempt, decode_queue_item_state,
				decode_queue_partition_counters,
			},
		},
		store::{SingleVersionGet, SingleVersionRange},
	},
	key::{
		EncodableKey,
		queue_attempt::QueueAttemptKey,
		queue_schedule::{QueueDueKey, QueueItemStateKey, QueuePartitionKey},
	},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{
	change::{QueueAckTransition, QueueRowAck},
	queue::scheduling::apply_ack_transitions,
	transaction::Transaction,
};
use reifydb_value::value::{Value, frame::frame::Frame, row_number::RowNumber};

fn engine_with_queue(declaration: &str) -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(declaration);
	t
}

fn queue_id(t: &TestEngine, name: &str) -> QueueId {
	let catalog = t.inner().catalog();
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
	catalog.find_queue_by_name(&mut txn, namespace.id(), name).unwrap().unwrap().id
}

fn states(t: &TestEngine, queue: QueueId) -> Vec<(QueueItemStateKey, QueueItemState)> {
	let store = t.inner().single().read_store();
	SingleVersionRange::range_batch(&store, QueueItemStateKey::queue_scan(queue), 1024)
		.unwrap()
		.items
		.iter()
		.map(|item| {
			(QueueItemStateKey::decode(&item.key).unwrap(), decode_queue_item_state(&item.bytes).unwrap())
		})
		.collect()
}

fn state_of(t: &TestEngine, queue: QueueId) -> QueueItemState {
	states(t, queue).into_iter().next().expect("the queue must hold exactly one item").1
}

fn dues(t: &TestEngine, queue: QueueId) -> Vec<QueueDueKey> {
	let store = t.inner().single().read_store();
	SingleVersionRange::range_batch(&store, QueueDueKey::queue_scan(queue), 1024)
		.unwrap()
		.items
		.iter()
		.map(|item| QueueDueKey::decode(&item.key).unwrap())
		.collect()
}

fn counters(t: &TestEngine, queue: QueueId, partition: u16) -> QueuePartitionCounters {
	let store = t.inner().single().read_store();
	SingleVersionGet::get(&store, &QueuePartitionKey::encoded(queue, partition))
		.unwrap()
		.map(|stored| decode_queue_partition_counters(&stored.bytes))
		.unwrap_or_default()
}

fn attempts(t: &TestEngine, queue: QueueId) -> Vec<(QueueAttemptKey, QueueAttemptRecord)> {
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let mut stream = txn
		.range(QueueAttemptKey::queue_scan(queue), reifydb_transaction::multi::RangeScope::All, 1024)
		.unwrap();

	let mut out = Vec::new();
	while let Some(item) = stream.next() {
		let item = item.unwrap();
		out.push((QueueAttemptKey::decode(&item.key).unwrap(), decode_queue_attempt(&item.bytes).unwrap()));
	}
	out
}

fn claim_one(t: &TestEngine, worker: &str) -> String {
	let frames = t.command(&format!(r#"CALL queue::claim("{worker}", "test::jobs", 1, duration::seconds(30))"#));
	token_of(&frames)
}

fn token_of(frames: &[Frame]) -> String {
	let frame = frames.first().expect("claim must return a frame");
	assert_eq!(frame.row_count(), 1, "expected exactly one claimed item");
	match frame.columns.iter().find(|c| c.name == "token").unwrap().data.get_value(0) {
		Value::Utf8(t) => t,
		other => panic!("token must be Utf8, got {other:?}"),
	}
}

fn ack(t: &TestEngine, token: &str, outcome: &str) -> String {
	let frames = t.command(&format!(r#"CALL queue::ack("{token}", "{outcome}", none)"#));
	match frames[0].columns.iter().find(|c| c.name == "status").unwrap().data.get_value(0) {
		Value::Utf8(s) => s,
		other => panic!("status must be Utf8, got {other:?}"),
	}
}

fn claimable(t: &TestEngine) -> usize {
	TestEngine::row_count(&t.command(r#"CALL queue::claim("probe", "test::jobs", 10, duration::seconds(30))"#))
}

const ONE_PARTITION: &str = "CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }";

#[test]
fn test_an_ok_ack_finishes_the_item_for_good() {
	// This is the success path of the entire primitive: work reported done must never come back.
	// It also pins the whole pipeline - the procedure writes a row change, the post-commit
	// interceptor CASes the state, and the counters follow. Any broken link leaves the item
	// leased forever or re-delivers it once the lease expires.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	let token = claim_one(&t, "w1");

	assert_eq!(ack(&t, &token, "ok"), "ok");

	let state = state_of(&t, queue);
	assert_eq!(state.status, QueueItemStatus::Done);
	assert_eq!(state.lease_deadline, None, "a finished item must not keep a live lease");
	assert_eq!(counters(&t, queue, 0).in_flight, 0);
	assert_eq!(counters(&t, queue, 0).depth, 0, "depth already fell at claim time");
	assert_eq!(claimable(&t), 0, "a done item must never be delivered again");
}

#[test]
fn test_an_ok_ack_records_what_the_worker_reported() {
	// The attempt record is the durable audit trail and the only thing repeat detection reads.
	// Without it a redelivered ack after a crash cannot be told apart from a first ack.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_nanos(4_200);
	let token = claim_one(&t, "worker-a");

	t.command(&format!(r#"CALL queue::ack("{token}", "ok", "delivered")"#));

	let recorded = attempts(&t, queue);
	assert_eq!(recorded.len(), 1);
	let (key, record) = &recorded[0];
	assert_eq!(key.attempt, 1);
	assert_eq!(record.worker, "worker-a");
	assert_eq!(record.outcome, AttemptOutcome::Ok);
	assert_eq!(record.response.as_deref(), Some("delivered"));
	assert_eq!(record.lost, false, "only the step-5 reaper writes lost attempts");
	assert_eq!(record.anomaly, None);
}

#[test]
fn test_an_err_ack_requeues_the_item_until_the_retry_budget_is_spent() {
	// The retry budget is what stops a permanently failing item from cycling forever. Both halves
	// matter: an err inside budget must return the item to the ready set with a fresh due entry,
	// and the ack at the budget must bury it instead.
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retry: { attempts: 2 } }",
	);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");

	assert_eq!(ack(&t, &claim_one(&t, "w1"), "err"), "ok");
	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Ready, "attempt 1 of 2 must be retried");
	assert_eq!(dues(&t, queue).len(), 1, "a retried item needs a due entry or no scan will find it");
	assert_eq!(counters(&t, queue, 0).depth, 1);
	assert_eq!(counters(&t, queue, 0).in_flight, 0);

	// The retry is parked behind the backoff; redelivering it now would defeat the load-shedding
	// the backoff exists for.
	assert_eq!(claimable(&t), 0, "a backed-off retry must not be redelivered before its delay elapses");
	t.mock_clock().advance_millis(10_000);

	let second = claim_one(&t, "w1");
	assert_eq!(ack(&t, &second, "err"), "ok");

	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Dead, "the budget is spent at attempt 2");
	assert_eq!(claimable(&t), 0);
	assert_eq!(counters(&t, queue, 0).in_flight, 0);
}

#[test]
fn test_an_err_ack_places_the_due_entry_at_the_backoff_instant() {
	// The due-index key IS the redelivery schedule: claim scans it by instant and never consults
	// backoff_until on its own. A due entry written at the wrong instant would hand the item back
	// immediately no matter what the state record says.
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retry: { attempts: 5 } }",
	);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(50_000);

	assert_eq!(ack(&t, &claim_one(&t, "w1"), "err"), "ok");

	let state = state_of(&t, queue);
	assert_eq!(
		state.backoff_until.map(|b| b.to_nanos()),
		Some(60_000_000_000),
		"the default 10s backoff must land 10s after the ack"
	);
	let due = dues(&t, queue);
	assert_eq!(due.len(), 1);
	assert_eq!(
		due[0].due.to_nanos(),
		60_000_000_000,
		"the due entry and backoff_until must name the same instant or claim removes the wrong key"
	);
}

#[test]
fn test_consecutive_failures_double_the_wait() {
	// One retry proves the delay exists; two prove it grows. A constant delay would let a
	// permanently failing item hammer a struggling endpoint at a fixed rate for its whole budget.
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retry: { attempts: 5 } }",
	);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);

	assert_eq!(ack(&t, &claim_one(&t, "w1"), "err"), "ok");
	assert_eq!(dues(&t, queue)[0].due.to_nanos(), 10_000_000_000, "first failure waits one base interval");

	t.mock_clock().set_millis(10_000);
	assert_eq!(ack(&t, &claim_one(&t, "w1"), "err"), "ok");
	assert_eq!(dues(&t, queue)[0].due.to_nanos(), 30_000_000_000, "the second failure waits 20s, not another 10s");
}

#[test]
fn test_a_retry_does_not_overwrite_the_user_declared_not_before() {
	// not_before is the caller's scheduling instruction and part of the item's history; the retry
	// delay is the queue's own. Collapsing the two would erase what the caller asked for, and the
	// next backoff would be computed against a value the caller never set.
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retry: { attempts: 5 } }",
	);
	t.command(r#"INSERT test::jobs [{ id: 1 }] WITH { not_before: datetime::from_epoch_millis(10000) }"#);
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(10_000);

	assert_eq!(ack(&t, &claim_one(&t, "w1"), "err"), "ok");

	let state = state_of(&t, queue);
	assert_eq!(
		state.not_before.map(|n| n.to_nanos()),
		Some(10_000_000_000),
		"the declared not_before must survive a retry untouched"
	);
	assert_eq!(state.backoff_until.map(|b| b.to_nanos()), Some(20_000_000_000));
}

#[test]
fn test_a_dead_ack_buries_the_item_immediately() {
	// "dead" is the worker saying the work is unrecoverable. Honouring the retry budget here
	// instead would keep re-running work already known to be poison.
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retry: { attempts: 5 } }",
	);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");

	assert_eq!(ack(&t, &claim_one(&t, "w1"), "dead"), "ok");

	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Dead);
	assert_eq!(claimable(&t), 0, "a dead item must not be retried despite an unspent budget");
}

#[test]
fn test_a_repeated_ack_is_a_no_op() {
	// Workers retry their ack after a network timeout. R5 requires repeats to change nothing: a
	// second transition would decrement in_flight twice and corrupt the counters permanently.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	let token = claim_one(&t, "w1");

	assert_eq!(ack(&t, &token, "ok"), "ok");
	let after_first = counters(&t, queue, 0);

	assert_eq!(ack(&t, &token, "ok"), "repeat");

	assert_eq!(attempts(&t, queue).len(), 1, "a repeat must not write a second attempt record");
	assert_eq!(counters(&t, queue, 0), after_first, "a repeat must not move the counters");
	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Done);
}

#[test]
fn test_the_first_outcome_wins_and_the_conflicting_one_is_recorded() {
	// A worker that reports success and then failure for the same attempt is reporting a bug in
	// itself. R5 says the first outcome stands and the contradiction is recorded rather than
	// silently dropped, so the disagreement is visible in the audit trail afterwards.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	let token = claim_one(&t, "w1");
	ack(&t, &token, "ok");

	assert_eq!(ack(&t, &token, "err"), "stale");

	let (_, record) = attempts(&t, queue).into_iter().next().unwrap();
	assert_eq!(record.outcome, AttemptOutcome::Ok, "the first outcome must stand");
	assert!(record.anomaly.unwrap().contains("conflicting late ack"), "the contradiction must be recorded");
	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Done, "a conflicting ack must not transition");
}

#[test]
fn test_an_ack_for_an_attempt_that_is_not_live_is_recorded_but_never_transitions() {
	// A forged or long-delayed token names an attempt nobody holds. Applying it would let any
	// caller finish work it never claimed; dropping it silently would erase the evidence that
	// someone tried. R5 requires exactly one of those: record, do not transition.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	let live = claim_one(&t, "w1");
	let forged = live.replace(":1:w1", ":7:w1");

	assert_eq!(ack(&t, &forged, "ok"), "stale");

	let (key, record) = attempts(&t, queue).into_iter().next().unwrap();
	assert_eq!(key.attempt, 7);
	assert!(record.anomaly.unwrap().contains("stale"));
	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Leased, "the real lease must be untouched");
	assert_eq!(state_of(&t, queue).attempt, 1);
}

#[test]
fn test_an_ack_after_the_item_was_already_finished_does_not_resurrect_it() {
	// Once an item is Done its state must be terminal. A late ack arriving on a fresh attempt
	// number would otherwise decrement in_flight below what is actually leased.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	let token = claim_one(&t, "w1");
	ack(&t, &token, "ok");
	let before = counters(&t, queue, 0);

	let other_attempt = token.replace(":1:w1", ":2:w1");
	assert_eq!(ack(&t, &other_attempt, "err"), "stale");

	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Done);
	assert_eq!(counters(&t, queue, 0), before);
}

/// Builds the ack row change the procedure would emit, so the interceptor's compare-and-set can be
/// driven directly. The procedure pre-filters on an advisory read, so a row change whose attempt no
/// longer matches is only producible when the state moves between that read and post-commit - real
/// under concurrency, unreachable single-threaded through `CALL queue::ack`.
fn apply_ack(t: &TestEngine, queue: QueueId, row: RowNumber, attempt: u32, transition: QueueAckTransition) -> u64 {
	apply_ack_transitions(
		t.inner().single(),
		queue,
		0,
		&[QueueRowAck {
			queue_id: queue,
			partition: 0,
			key_hash: None,
			row_number: row,
			attempt,
			transition,
		}],
	)
	.unwrap()
}

#[test]
fn test_the_interceptor_refuses_an_ack_whose_attempt_no_longer_matches_the_lease() {
	// The lease may have expired and been reissued while the ack was in flight. Applying the old
	// attempt's outcome would finish work the current holder is still doing, and decrement
	// in_flight for a lease that is genuinely still out.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	claim_one(&t, "w1");
	let row = states(&t, queue)[0].0.row;
	let before = counters(&t, queue, 0);

	let applied = apply_ack(&t, queue, row, 99, QueueAckTransition::Done);

	assert_eq!(applied, 0, "a mismatched attempt must apply no transition");
	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Leased);
	assert_eq!(counters(&t, queue, 0), before, "a refused ack must not move the counters");
}

#[test]
fn test_the_interceptor_refuses_an_ack_against_an_item_that_is_no_longer_leased() {
	// Same guard, other half: once the reaper or a prior ack moved the item out of Leased, a
	// second transition would double-decrement in_flight and drift the counters permanently.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	let token = claim_one(&t, "w1");
	let row = states(&t, queue)[0].0.row;
	ack(&t, &token, "ok");
	let before = counters(&t, queue, 0);

	let applied = apply_ack(&t, queue, row, 1, QueueAckTransition::Done);

	assert_eq!(applied, 0, "an item that is already Done must not transition again");
	assert_eq!(counters(&t, queue, 0), before);
}

#[test]
fn test_a_malformed_token_is_rejected_with_queue_003() {
	// Tokens come off the wire. A parser that accepted garbage would address an arbitrary item.
	// QUEUE_003 is the token-parser code specifically; QUEUE_001 is queue immutability, so
	// asserting the wrong one here would pass against an unrelated diagnostic.
	let t = engine_with_queue(ONE_PARTITION);

	let err = t.command_err(r#"CALL queue::ack("not-a-token", "ok", none)"#);

	assert!(err.contains("QUEUE_003"), "{err}");
}

#[test]
fn test_an_unknown_outcome_is_rejected() {
	// Silently coercing an unrecognised outcome to ok would mark failed work as complete.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let token = claim_one(&t, "w1");

	let err = t.command_err(&format!(r#"CALL queue::ack("{token}", "maybe", none)"#));

	assert!(err.contains("ok, err, dead"), "{err}");
}

#[test]
fn test_an_ack_is_rejected_outside_a_command_transaction() {
	// The transition rides on a row change that only a committing transaction produces; a
	// query-lane ack would report success while changing nothing.
	let t = engine_with_queue(ONE_PARTITION);

	let err = t.query_err(r#"CALL queue::ack("qt1:1:0:1:1:w1", "ok", none)"#);

	assert!(err.contains("must run in a command transaction"), "{err}");
}
