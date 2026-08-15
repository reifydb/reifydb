// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::{
		catalog::{
			id::QueueId,
			queue::{QueueItemState, decode_queue_item_state},
		},
		store::SingleVersionRange,
	},
	key::queue_schedule::QueueItemStateKey,
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, datetime::DateTime, frame::frame::Frame};

const ONE_PARTITION: &str = "CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }";

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

fn state_of(t: &TestEngine, queue: QueueId) -> QueueItemState {
	let store = t.inner().single().read_store();
	SingleVersionRange::range_batch(&store, QueueItemStateKey::queue_scan(queue), 1024)
		.unwrap()
		.items
		.iter()
		.map(|item| decode_queue_item_state(EncodedPodRow::view(&item.bytes)).unwrap())
		.next()
		.expect("the queue must hold exactly one item")
}

fn claim_one(t: &TestEngine, worker: &str) -> String {
	let frames = t.command(&format!(r#"CALL queue::claim("{worker}", "test::jobs", 1, duration::seconds(30))"#));
	match frames[0].columns.iter().find(|c| c.name == "token").unwrap().data.get_value(0) {
		Value::Utf8(t) => t,
		other => panic!("token must be Utf8, got {other:?}"),
	}
}

fn deadline_of(frames: &[Frame]) -> DateTime {
	match frames[0].columns.iter().find(|c| c.name == "deadline").unwrap().data.get_value(0) {
		Value::DateTime(d) => d,
		other => panic!("deadline must be DateTime, got {other:?}"),
	}
}

#[test]
fn test_extend_moves_the_deadline_of_a_live_lease() {
	// A worker holding a long-running task keeps its lease alive with this call. If the new
	// deadline never reached the state record the reaper would take the item back mid-execution
	// and hand the same work to someone else.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_nanos(1_000);
	let token = claim_one(&t, "w1");
	assert_eq!(state_of(&t, queue).lease_deadline, Some(DateTime::from_nanos(1_000 + 30 * 1_000_000_000)));

	t.mock_clock().set_nanos(20 * 1_000_000_000);
	let frames = t.command(&format!(r#"CALL queue::extend("{token}", duration::seconds(60))"#));

	let expected = DateTime::from_nanos(80 * 1_000_000_000);
	assert_eq!(deadline_of(&frames), expected);
	assert_eq!(state_of(&t, queue).lease_deadline, Some(expected), "the durable record must carry the extension");
}

#[test]
fn test_extend_never_shortens_a_deadline() {
	// Extend is monotonic on purpose. A worker that asks for a shorter window than it already
	// holds - a retry with a smaller ttl, or a clock that moved - must not accidentally hand its
	// own lease back early while it is still working.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");
	t.mock_clock().set_nanos(0);
	let token = claim_one(&t, "w1");
	let original = state_of(&t, queue).lease_deadline.unwrap();

	let frames = t.command(&format!(r#"CALL queue::extend("{token}", duration::seconds(5))"#));

	assert_eq!(deadline_of(&frames), original, "a shorter request must return the existing deadline");
	assert_eq!(state_of(&t, queue).lease_deadline, Some(original));
}

#[test]
fn test_extend_fails_hard_once_the_item_has_been_acked() {
	// The whole point of the contract: a worker whose lease is gone must be told, not quietly
	// allowed to carry on. Returning success here would leave it writing results for work that
	// has already been redelivered to somebody else.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let token = claim_one(&t, "w1");
	t.command(&format!(r#"CALL queue::ack("{token}", "ok", none)"#));

	let err = t.command_err(&format!(r#"CALL queue::extend("{token}", duration::seconds(60))"#));

	assert!(err.contains("QUEUE_002"), "{err}");
	assert!(err.contains("not leased"), "{err}");
}

#[test]
fn test_extend_fails_hard_for_an_attempt_that_is_no_longer_current() {
	// After a lease expires and the item is reissued, the old holder's token still parses. Only
	// the attempt number distinguishes it, so extending on a mismatch would let a zombie worker
	// hold a lease that belongs to its successor.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let token = claim_one(&t, "w1");
	let superseded = token.replace(":1:w1", ":2:w1");

	let err = t.command_err(&format!(r#"CALL queue::extend("{superseded}", duration::seconds(60))"#));

	assert!(err.contains("QUEUE_002"), "{err}");
	assert!(err.contains("later attempt"), "{err}");
}

#[test]
fn test_extend_fails_hard_for_an_item_with_no_scheduling_state() {
	// A well-formed token can name an item that never existed or whose queue was dropped. That
	// must be a hard error rather than a silent success, for the same abandon-the-task reason.
	let t = engine_with_queue(ONE_PARTITION);
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");

	let err =
		t.command_err(&format!(r#"CALL queue::extend("qt1:{}:0:9999:1:w1", duration::seconds(60))"#, queue.0));

	assert!(err.contains("QUEUE_002"), "{err}");
}

#[test]
fn test_extend_rejects_a_malformed_token() {
	// Same network-facing parser as ack; a forged token must never be resolved to an item.
	let t = engine_with_queue(ONE_PARTITION);

	let err = t.command_err(r#"CALL queue::extend("nonsense", duration::seconds(60))"#);

	assert!(err.contains("QUEUE_003"), "{err}");
}

#[test]
fn test_extend_is_rejected_outside_a_command_transaction() {
	// Extend writes to the single lane; a query-lane call would report a new deadline that was
	// never persisted, which is worse than failing.
	let t = engine_with_queue(ONE_PARTITION);

	let err = t.query_err(r#"CALL queue::extend("qt1:1:0:1:1:w1", duration::seconds(60))"#);

	assert!(err.contains("must run in a command transaction"), "{err}");
}
