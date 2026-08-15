// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::{pod::EncodedPodRow, queue_attempt::EncodedQueueAttemptRow};
use reifydb_core::{
	interface::{
		catalog::{
			id::QueueId,
			queue::{
				QueueAttemptRecord, QueueItemState, QueueItemStatus, QueuePartitionCounters,
				decode_queue_attempt, decode_queue_item_state, decode_queue_partition_counters,
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
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, frame::frame::Frame};

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
			(
				QueueItemStateKey::decode(&item.key).unwrap(),
				decode_queue_item_state(EncodedPodRow::view(&item.bytes)).unwrap(),
			)
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
		.map(|stored| decode_queue_partition_counters(EncodedPodRow::view(&stored.bytes)))
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
		out.push((
			QueueAttemptKey::decode(&item.key).unwrap(),
			decode_queue_attempt(EncodedQueueAttemptRow::view(&item.bytes)).unwrap(),
		));
	}
	out
}

fn claim_one(t: &TestEngine, worker: &str) -> String {
	let frames = t.command(&format!(r#"CALL queue::claim("{worker}", "test::jobs", 1, duration::seconds(30))"#));
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

fn replay(t: &TestEngine, item: u64) -> Vec<Frame> {
	t.command(&format!(r#"CALL queue::replay("test::jobs", {item})"#))
}

fn utf8_of(frames: &[Frame], column: &str) -> String {
	match frames[0].columns.iter().find(|c| c.name == column).unwrap().data.get_value(0) {
		Value::Utf8(s) => s,
		other => panic!("{column} must be Utf8, got {other:?}"),
	}
}

fn kill(t: &TestEngine) {
	ack(t, &claim_one(t, "w1"), "dead");
}

const ONE_PARTITION: &str = "CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }";
const TWO_ATTEMPTS: &str =
	"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retry: { attempts: 2 } }";

#[test]
fn test_replay_returns_a_dead_item_to_the_ready_set() {
	// This is the whole purpose of the procedure: a dead item is unreachable by every other path in
	// the primitive, so without a working replay an operator who fixed the downstream cause has no
	// way to get the work to run. Both halves must hold - the state record flips to ready AND a due
	// entry appears, because claim scans the due index and never looks at state records on its own.
	// A replay that set only the status would silently produce an item that is ready forever and
	// never delivered.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	kill(&t);
	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Dead);
	let item = states(&t, queue)[0].0.row;

	replay(&t, item.0);

	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Ready);
	assert_eq!(dues(&t, queue).len(), 1, "a revived item needs a due entry or no claim will ever find it");
	assert_eq!(claimable(&t), 1, "the point of replay is that the item runs again");
}

#[test]
fn test_replay_reports_what_it_did() {
	// An operator drives this by hand, one item at a time. The result is the only confirmation they
	// get, so it must name the item that moved and the state it moved to rather than an empty
	// success.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	kill(&t);
	let item = states(&t, queue)[0].0.row;

	let frames = replay(&t, item.0);

	assert_eq!(utf8_of(&frames, "queue"), "test::jobs");
	assert_eq!(utf8_of(&frames, "state"), "ready");
	match frames[0].columns.iter().find(|c| c.name == "item").unwrap().data.get_value(0) {
		Value::Uint8(v) => assert_eq!(v, item.0),
		other => panic!("item must be Uint8, got {other:?}"),
	}
}

#[test]
fn test_replay_rebases_the_retry_budget_instead_of_rewinding_the_attempt_counter() {
	// budget_base is the entire mechanism that makes "a fresh budget" and "an intact history"
	// compatible. Attempt numbers key the attempt records, so rewinding attempt to 0 would collide
	// with the first life's records and destroy the audit trail; leaving budget_base alone would
	// hand back an item whose budget is already spent. Only moving budget_base up to the current
	// attempt does both.
	let t = engine_with_queue(TWO_ATTEMPTS);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	kill(&t);
	let item = states(&t, queue)[0].0.row;
	assert_eq!(state_of(&t, queue).attempt, 1);
	assert_eq!(state_of(&t, queue).budget_base, 0);

	replay(&t, item.0);

	let state = state_of(&t, queue);
	assert_eq!(state.attempt, 1, "the attempt counter must keep climbing or attempt keys collide");
	assert_eq!(state.budget_base, 1, "the new life's budget starts counting from the attempt it was revived at");
}

#[test]
fn test_a_replayed_item_gets_its_whole_budget_back_and_not_one_attempt() {
	// The behavioural half of budget_base, and the mutation this pins: with budget_base left at 0 a
	// replayed item would exhaust immediately on its very next failure, so replay would buy the
	// operator exactly one retry instead of the declared budget of two. The item must survive its
	// first post-replay failure and only die on the second.
	let t = engine_with_queue(TWO_ATTEMPTS);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);

	ack(&t, &claim_one(&t, "w1"), "err");
	t.mock_clock().advance_millis(10_000);
	ack(&t, &claim_one(&t, "w1"), "err");
	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Dead, "the first life spends both attempts");
	let item = states(&t, queue)[0].0.row;

	replay(&t, item.0);

	ack(&t, &claim_one(&t, "w1"), "err");
	assert_eq!(
		state_of(&t, queue).status,
		QueueItemStatus::Ready,
		"the third attempt is the first of the new life and must be retried, not buried"
	);

	t.mock_clock().advance_millis(60_000);
	ack(&t, &claim_one(&t, "w1"), "err");
	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Dead, "the new budget is finite too");
	assert_eq!(state_of(&t, queue).attempt, 4);
}

#[test]
fn test_replay_keeps_every_attempt_record_of_the_previous_life() {
	// Replay exists to re-run work that already failed, which is exactly the work whose history an
	// operator needs afterwards. Nothing may be overwritten or renumbered: the first life's records
	// stay readable and the new attempt lands on a fresh key beside them.
	let t = engine_with_queue(TWO_ATTEMPTS);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.command(&format!(r#"CALL queue::ack("{}", "dead", "gave up")"#, claim_one(&t, "worker-a")));
	let item = states(&t, queue)[0].0.row;

	replay(&t, item.0);
	ack(&t, &claim_one(&t, "worker-b"), "ok");

	// Key components are stored bitwise-inverted, so a forward scan of the attempt keyspace returns
	// the newest attempt first. Sort by attempt so this test pins the history rather than the scan
	// direction, which belongs to the key encoding and is exercised elsewhere.
	let mut recorded = attempts(&t, queue);
	recorded.sort_by_key(|(key, _)| key.attempt);

	assert_eq!(recorded.len(), 2, "the first life's record must survive replay");
	assert_eq!(recorded[0].0.attempt, 1);
	assert_eq!(recorded[0].1.worker, "worker-a");
	assert_eq!(recorded[0].1.response.as_deref(), Some("gave up"), "the original failure detail must be intact");
	assert_eq!(recorded[1].0.attempt, 2);
	assert_eq!(recorded[1].1.worker, "worker-b");
}

#[test]
fn test_replay_leaves_no_trace_of_the_previous_life_in_the_scheduling_state() {
	// lease_deadline and backoff_until both describe a life that has ended. Neither is load-bearing
	// once the status is ready - the reaper only looks at leased items and the next backoff is
	// computed from the attempt count - but a revived item that still advertises a lease deadline
	// and a backoff instant misreports itself to every introspection path, and the stale backoff
	// would be what the due entry is placed at.
	let t = engine_with_queue(TWO_ATTEMPTS);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(0);
	ack(&t, &claim_one(&t, "w1"), "err");
	t.mock_clock().advance_millis(10_000);
	ack(&t, &claim_one(&t, "w1"), "err");
	let dead = state_of(&t, queue);
	assert!(dead.backoff_until.is_some(), "the fixture must actually leave a backoff behind");
	let item = states(&t, queue)[0].0.row;

	replay(&t, item.0);

	let state = state_of(&t, queue);
	assert_eq!(state.lease_deadline, None);
	assert_eq!(state.backoff_until, None);
	assert_eq!(dues(&t, queue)[0].due, state.due(), "the due entry must name the instant state.due() reports");
}

#[test]
fn test_replay_preserves_the_caller_declared_not_before() {
	// not_before is the caller's scheduling instruction, not a retry artefact. Clearing it on replay
	// would let a revived item run before the instant the caller said it may.
	let t = engine_with_queue(ONE_PARTITION);
	t.mock_clock().set_millis(0);
	t.command(r#"INSERT test::jobs [{ id: 1 }] WITH { not_before: datetime::from_epoch_millis(5000) }"#);
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_millis(5_000);
	kill(&t);
	let item = states(&t, queue)[0].0.row;

	replay(&t, item.0);

	let state = state_of(&t, queue);
	assert_eq!(state.not_before.map(|n| n.to_nanos()), Some(5_000_000_000));
	assert_eq!(dues(&t, queue)[0].due.to_nanos(), 5_000_000_000);
}

#[test]
fn test_replay_returns_the_item_to_the_depth_counter() {
	// depth is what step 7 will report as backlog and what an operator watches to know work is
	// pending. A dead item is not backlog; a replayed one is. Skipping the increment would leave a
	// queue reporting an empty backlog while holding claimable work, and the following claim would
	// then drive depth negative if it were not saturating.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	kill(&t);
	assert_eq!(counters(&t, queue, 0).depth, 0, "a dead item is not backlog");
	assert_eq!(counters(&t, queue, 0).in_flight, 0);
	let item = states(&t, queue)[0].0.row;

	replay(&t, item.0);

	assert_eq!(counters(&t, queue, 0).depth, 1);
	assert_eq!(counters(&t, queue, 0).in_flight, 0, "replay must not fake a lease");

	claim_one(&t, "w1");
	assert_eq!(counters(&t, queue, 0).depth, 0);
	assert_eq!(counters(&t, queue, 0).in_flight, 1);
}

#[test]
fn test_replay_finds_an_item_in_any_partition() {
	// Items are spread across partitions by hash and the caller only knows the item number, never
	// the partition. A lookup that checked partition 0 only would work in every single-partition
	// test and fail against any real queue, so this fixture deliberately kills an item that does
	// not hash to partition 0.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 8 } }");
	for id in 1..=16 {
		t.command(&format!("INSERT test::jobs [{{ id: {id} }}]"));
	}
	let queue = queue_id(&t, "jobs");

	let placed = states(&t, queue);
	let (key, _) = placed
		.iter()
		.find(|(key, _)| key.partition != 0)
		.expect("16 items over 8 partitions must land somewhere other than partition 0");
	let (partition, item) = (key.partition, key.row);

	let token = t
		.command(&format!(r#"CALL queue::claim("w1", "test::jobs", 16, duration::seconds(30))"#))
		.first()
		.map(|frame| {
			let index = frame
				.row_numbers()
				.iter()
				.position(|row| *row == item)
				.expect("every inserted item must be claimable");
			match frame.columns.iter().find(|c| c.name == "token").unwrap().data.get_value(index) {
				Value::Utf8(t) => t,
				other => panic!("token must be Utf8, got {other:?}"),
			}
		})
		.unwrap();
	ack(&t, &token, "dead");

	replay(&t, item.0);

	let revived = states(&t, queue)
		.into_iter()
		.find(|(key, _)| key.row == item)
		.expect("the item must still have scheduling state");
	assert_eq!(revived.0.partition, partition, "replay must not move an item to another partition");
	assert_eq!(revived.1.status, QueueItemStatus::Ready);
	assert_eq!(counters(&t, queue, partition).depth, 1);
}

#[test]
fn test_replay_of_a_leased_item_is_refused_without_touching_it() {
	// A leased item is being worked on right now. Reviving it would put a second copy of the same
	// work into the ready set while the first holder is still running, which is the duplicate
	// delivery the whole lease mechanism exists to prevent.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	claim_one(&t, "w1");
	let item = states(&t, queue)[0].0.row;
	let before = state_of(&t, queue);

	let err = t.command_err(&format!(r#"CALL queue::replay("test::jobs", {})"#, item.0));

	assert!(err.contains("QUEUE_005"), "{err}");
	assert!(err.contains("leased"), "{err}");
	assert_eq!(state_of(&t, queue), before, "a refused replay must change nothing");
	assert_eq!(dues(&t, queue).len(), 0, "a refused replay must not add a due entry");
	assert_eq!(counters(&t, queue, 0).depth, 0);
}

#[test]
fn test_replay_of_a_finished_item_is_refused() {
	// Done is terminal by contract - an ok ack promises the work will never run again. Replaying it
	// would re-execute an effect the caller was told had completed exactly once.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	ack(&t, &claim_one(&t, "w1"), "ok");
	let item = states(&t, queue)[0].0.row;

	let err = t.command_err(&format!(r#"CALL queue::replay("test::jobs", {})"#, item.0));

	assert!(err.contains("QUEUE_005"), "{err}");
	assert_eq!(state_of(&t, queue).status, QueueItemStatus::Done);
}

#[test]
fn test_replaying_the_same_item_twice_is_refused_the_second_time() {
	// Replay is not idempotent and must not pretend to be: the second call finds a ready item. If it
	// succeeded anyway it would increment depth again and write a second due entry, so the item
	// would be counted twice and could be delivered twice.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	kill(&t);
	let item = states(&t, queue)[0].0.row;
	replay(&t, item.0);

	let err = t.command_err(&format!(r#"CALL queue::replay("test::jobs", {})"#, item.0));

	assert!(err.contains("QUEUE_005"), "{err}");
	assert!(err.contains("ready"), "{err}");
	assert_eq!(counters(&t, queue, 0).depth, 1, "the refused second replay must not count the item twice");
	assert_eq!(dues(&t, queue).len(), 1);
}

#[test]
fn test_replay_of_an_unknown_item_is_refused() {
	// The item number comes from a human reading a dashboard, so typos are the common case. It is
	// also the same answer an item swept by retention gives, because retention removes the
	// scheduling state - that is the documented end of an item's replay window, not a bug.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");

	let err = t.command_err(r#"CALL queue::replay("test::jobs", 9999)"#);

	assert!(err.contains("QUEUE_004"), "{err}");
}

#[test]
fn test_replay_of_an_unknown_queue_is_refused() {
	// Resolution happens before anything is probed. Falling through to the item lookup would report
	// a missing item for a queue that does not exist, sending the operator after the wrong problem.
	let t = engine_with_queue(ONE_PARTITION);

	let err = t.command_err(r#"CALL queue::replay("test::missing", 1)"#);

	assert!(err.contains("missing"), "{err}");
	assert!(!err.contains("QUEUE_004"), "a missing queue is not a missing item: {err}");
}

#[test]
fn test_replay_is_rejected_outside_a_command_transaction() {
	// The whole procedure is a durable write to the scheduling lane. Allowing it on the query lane
	// would report a revived item to the operator while changing nothing.
	let t = engine_with_queue(ONE_PARTITION);

	let err = t.query_err(r#"CALL queue::replay("test::jobs", 1)"#);

	assert!(err.contains("must run in a command transaction"), "{err}");
}

#[test]
fn test_replay_rejects_an_item_number_that_cannot_name_a_row() {
	// Row numbers start at 1. Accepting 0 or a negative would send a well-formed but meaningless key
	// into the partition probe and report "unknown item", hiding a caller bug behind a plausible
	// answer.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");

	let err = t.command_err(r#"CALL queue::replay("test::jobs", 0)"#);

	assert!(err.contains("positive row number"), "{err}");
}
