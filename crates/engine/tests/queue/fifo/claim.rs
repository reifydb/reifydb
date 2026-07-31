// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeSet;

use reifydb_codec::row::bytes::EncodedBytes;
use reifydb_core::{
	interface::{
		catalog::{
			id::QueueId,
			queue::{
				QueueItemState, QueueItemStatus, QueuePartitionCounters, decode_queue_item_state,
				decode_queue_partition_counters,
			},
		},
		store::{SingleVersionGet, SingleVersionRange},
	},
	key::{
		EncodableKey,
		queue_schedule::{QueueDueKey, QueueItemStateKey, QueuePartitionKey},
	},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{single::write::SingleWriteTransaction, transaction::Transaction};
use reifydb_value::{
	util::cowvec::CowVec,
	value::{Value, datetime::DateTime, frame::frame::Frame, row_number::RowNumber},
};

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

fn totals(t: &TestEngine, queue: QueueId) -> QueuePartitionCounters {
	let store = t.inner().single().read_store();
	SingleVersionRange::range_batch(&store, QueuePartitionKey::queue_scan(queue), 1024)
		.unwrap()
		.items
		.iter()
		.map(|item| decode_queue_partition_counters(&item.bytes))
		.fold(QueuePartitionCounters::default(), |mut acc, c| {
			acc.depth += c.depth;
			acc.in_flight += c.in_flight;
			acc.blocked_keys += c.blocked_keys;
			acc
		})
}

fn with_partition<F>(t: &TestEngine, queue: QueueId, partition: u16, f: F)
where
	F: FnOnce(&mut SingleWriteTransaction<'_>),
{
	let single = t.inner().single();
	let lock_key = QueuePartitionKey::encoded(queue, partition);
	let mut tx = single
		.begin_command_ranged(
			[&lock_key],
			vec![
				QueueItemStateKey::partition_scan(queue, partition),
				QueueDueKey::partition_scan(queue, partition),
			],
		)
		.unwrap();
	f(&mut tx);
	tx.commit().unwrap();
}

/// Forges the exact situation the under-lock re-check exists for: the advisory due scan is
/// unsynchronized, so it can propose an item whose state record has already moved on. Writing a due
/// entry that disagrees with the state record reproduces that staleness deterministically, which a
/// single-threaded test otherwise cannot.
fn plant_stale_due_entry(t: &TestEngine, queue: QueueId, partition: u16, row: RowNumber, due: DateTime) {
	with_partition(t, queue, partition, |tx| {
		tx.set(&QueueDueKey::encoded(queue, partition, due, row), EncodedBytes(CowVec::new(vec![]))).unwrap();
	});
}

fn claim(t: &TestEngine, worker: &str, max_n: u32, ttl_seconds: u32) -> Vec<Frame> {
	t.command(&format!(r#"CALL queue::claim("{worker}", "test::jobs", {max_n}, duration::seconds({ttl_seconds}))"#))
}

fn column(frames: &[Frame], name: &str) -> Vec<Value> {
	let frame = frames.first().expect("claim must always return a frame");
	let column = frame.columns.iter().find(|c| c.name == name).unwrap_or_else(|| {
		panic!(
			"claim result has no column {name}; got {:?}",
			frame.columns.iter().map(|c| &c.name).collect::<Vec<_>>()
		)
	});
	(0..frame.row_count()).map(|i| column.data.get_value(i)).collect()
}

fn claimed_items(frames: &[Frame]) -> BTreeSet<u64> {
	column(frames, "item")
		.into_iter()
		.map(|v| match v {
			Value::Uint8(n) => n,
			other => panic!("item column must be Uint8, got {other:?}"),
		})
		.collect()
}

#[test]
fn test_a_claim_leases_every_due_item_and_never_hands_it_out_twice() {
	// This is the whole point of the primitive: an item is allocated to exactly one worker. The
	// second claim must come back empty, which can only happen if the lease write and the due
	// entry removal landed together. If either half regressed the same work would be delivered
	// twice and every downstream effect would run twice.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }]");

	let first = claim(&t, "w1", 10, 30);
	assert_eq!(TestEngine::row_count(&first), 3);

	let second = claim(&t, "w1", 10, 30);
	assert_eq!(TestEngine::row_count(&second), 0, "an item already leased must not be claimable again");

	let queue = queue_id(&t, "jobs");
	assert!(dues(&t, queue).is_empty(), "a leased item must leave no due entry behind");
	assert!(states(&t, queue).iter().all(|(_, s)| s.status == QueueItemStatus::Leased));
}

#[test]
fn test_a_claim_records_the_lease_on_the_item_state() {
	// The state record is what the reaper and every later transition read. An attempt that stays
	// at 0 would give the retry budget infinite lives; a missing deadline would make the lease
	// immortal and the item unrecoverable after a worker dies.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::jobs [{ id: 1 }]");
	t.mock_clock().set_nanos(5_000);

	claim(&t, "w1", 1, 30);

	let queue = queue_id(&t, "jobs");
	let (_, state) = states(&t, queue).into_iter().next().unwrap();

	assert_eq!(state.status, QueueItemStatus::Leased);
	assert_eq!(state.attempt, 1, "the first delivery is attempt 1");
	assert_eq!(state.lease_deadline, Some(DateTime::from_nanos(5_000 + 30 * 1_000_000_000)));
}

#[test]
fn test_a_claim_moves_the_item_from_depth_to_in_flight() {
	// depth and in_flight are disjoint by definition and are only ever maintained transitionally,
	// never recomputed. A claim that bumped in_flight without dropping depth would permanently
	// overstate the queue's backlog, and the drift never heals.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }]");
	assert_eq!(counters(&t, queue_id(&t, "jobs"), 0).depth, 3);

	claim(&t, "w1", 2, 30);

	let after = counters(&t, queue_id(&t, "jobs"), 0);
	assert_eq!(after.depth, 1, "two of three items are no longer waiting");
	assert_eq!(after.in_flight, 2);
}

#[test]
fn test_a_claim_never_exceeds_max_n() {
	// max_n is the worker's own concurrency budget. Over-delivering hands a worker more leases
	// than it can finish before the deadline, which turns into guaranteed lease expiry and
	// duplicate execution rather than throughput.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 4 } }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }, { id: 5 }]");

	let frames = claim(&t, "w1", 2, 30);

	assert_eq!(TestEngine::row_count(&frames), 2);
	assert_eq!(totals(&t, queue_id(&t, "jobs")).in_flight, 2);
}

#[test]
fn test_two_workers_never_receive_the_same_item() {
	// R1's finding was that table-based claiming collapses because two workers contend on the
	// same rows. Here allocation is the storage primitive's job: whatever the partition rotation
	// does, the intersection of two workers' claims must be empty.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 8 } }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }, { id: 4 }]");

	let first = claimed_items(&claim(&t, "worker-a", 2, 30));
	let second = claimed_items(&claim(&t, "worker-b", 2, 30));

	assert_eq!(first.len(), 2);
	assert_eq!(second.len(), 2);
	assert!(first.is_disjoint(&second), "{first:?} and {second:?} overlap");
}

#[test]
fn test_an_item_is_not_claimable_before_its_not_before() {
	// A delayed item that can be claimed early defeats the only scheduling guarantee the queue
	// makes to the caller. The due index alone is not enough - the state record is re-checked
	// under the lock, and this pins both halves.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command(r#"INSERT test::jobs [{ id: 1 }] WITH { not_before: datetime::from_epoch_millis(10000) }"#);
	t.mock_clock().set_millis(9_999);

	assert_eq!(TestEngine::row_count(&claim(&t, "w1", 10, 30)), 0, "not due yet");

	t.mock_clock().set_millis(10_000);

	assert_eq!(TestEngine::row_count(&claim(&t, "w1", 10, 30)), 1, "due exactly at not_before");
}

#[test]
fn test_a_stale_due_entry_cannot_release_an_item_that_is_already_leased() {
	// The due scan runs outside the partition lock, so under concurrency it can hand claim a
	// candidate another worker leased microseconds ago. The status re-check inside the lock is the
	// only thing that stops that from becoming a double delivery - the advisory filter cannot,
	// because the stale entry looks perfectly due.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::jobs [{ id: 1 }]");
	let queue = queue_id(&t, "jobs");

	claim(&t, "w1", 1, 30);
	let (key, leased) = states(&t, queue).into_iter().next().unwrap();
	plant_stale_due_entry(&t, queue, key.partition, key.row, DateTime::from_nanos(0));

	assert_eq!(TestEngine::row_count(&claim(&t, "w2", 10, 30)), 0, "a leased item must not be re-leased");
	assert_eq!(states(&t, queue)[0].1.attempt, leased.attempt, "a refused claim must not burn an attempt");
}

#[test]
fn test_a_stale_due_entry_cannot_lease_an_item_before_its_not_before() {
	// Same staleness, different guard: a due entry can outlive the not_before it was indexed under
	// once step 5 starts rewriting backoff times. The state record is authoritative, so an item
	// whose record says "not yet" must survive a due entry that says "now".
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command(r#"INSERT test::jobs [{ id: 1 }] WITH { not_before: datetime::from_epoch_millis(10000) }"#);
	t.mock_clock().set_millis(0);
	let queue = queue_id(&t, "jobs");
	let (key, _) = states(&t, queue).into_iter().next().unwrap();

	plant_stale_due_entry(&t, queue, key.partition, key.row, DateTime::from_nanos(0));

	assert_eq!(TestEngine::row_count(&claim(&t, "w1", 10, 30)), 0, "not_before must be honoured under the lock");
	assert_eq!(states(&t, queue)[0].1.status, QueueItemStatus::Ready);
}

#[test]
fn test_a_claim_returns_the_declared_payload_alongside_the_lease() {
	// A worker that receives a token but no payload cannot do the work. The trailing hidden
	// not_before field must stay hidden here for the same reason FROM hides it: it is not a
	// column the user declared.
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4, payload: Option(utf8) } WITH { fifo: { partitions: 1 } }",
	);
	t.command(r#"INSERT test::jobs [{ id: 7, payload: "work" }]"#);

	let frames = claim(&t, "w1", 1, 30);
	let names: Vec<&str> = frames[0].columns.iter().map(|c| c.name.as_str()).collect();

	assert_eq!(names, vec!["token", "item", "attempt", "deadline", "id", "payload"]);
	assert_eq!(column(&frames, "id")[0], Value::Int4(7));
	assert_eq!(column(&frames, "payload")[0], Value::Utf8("work".to_string()));
}

#[test]
fn test_the_token_addresses_the_item_that_was_leased() {
	// The token is the only thing ack and extend are given. If it named a different item or a
	// different attempt than the lease it was minted for, every subsequent transition would be
	// applied to the wrong row.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::jobs [{ id: 1 }]");

	let frames = claim(&t, "w1", 1, 30);
	let token = match &column(&frames, "token")[0] {
		Value::Utf8(t) => t.clone(),
		other => panic!("token must be Utf8, got {other:?}"),
	};

	let queue = queue_id(&t, "jobs");
	let (key, state) = states(&t, queue).into_iter().next().unwrap();

	assert_eq!(token, format!("qt1:{}:{}:{}:{}:w1", queue.0, key.partition, key.row.0, state.attempt));
}

#[test]
fn test_an_empty_claim_still_reports_the_full_schema() {
	// Workers poll an empty queue constantly. Collapsing to an empty frame would force every
	// client to special-case the no-work path instead of iterating zero rows.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");

	let frames = claim(&t, "w1", 10, 30);
	let names: Vec<&str> = frames[0].columns.iter().map(|c| c.name.as_str()).collect();

	assert_eq!(TestEngine::row_count(&frames), 0);
	assert_eq!(names, vec!["token", "item", "attempt", "deadline", "id"]);
}

#[test]
fn test_a_claim_is_rejected_outside_a_command_transaction() {
	// A query-lane claim would resolve and return leases while skipping the single-lane write
	// entirely, so the same items would be handed out again on the next call.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::jobs [{ id: 1 }]");

	let err = t.query_err(r#"CALL queue::claim("w1", "test::jobs", 1, duration::seconds(30))"#);

	assert!(err.contains("must run in a command transaction"), "{err}");
}

#[test]
fn test_a_claim_against_an_unknown_queue_reports_the_queue() {
	// Silently returning zero rows for a typo'd queue name would look exactly like an empty
	// queue, and a worker would poll forever against nothing.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");

	let err = t.command_err(r#"CALL queue::claim("w1", "test::missing", 1, duration::seconds(30))"#);

	assert!(err.contains("missing"), "{err}");
}

#[test]
fn test_a_claim_rejects_a_non_positive_max_n() {
	// max_n of zero or below is a caller bug, not a request for no work; answering it with an
	// empty result would hide the bug behind a queue that looks permanently idle.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");

	let err = t.command_err(r#"CALL queue::claim("w1", "test::jobs", 0, duration::seconds(30))"#);

	assert!(err.contains("max_n"), "{err}");
}

#[test]
fn test_a_claim_rejects_an_empty_worker_id() {
	// The worker id is embedded in every token this call mints and is what later identifies who
	// holds the lease. An empty id makes the whole audit trail anonymous.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");

	let err = t.command_err(r#"CALL queue::claim("", "test::jobs", 1, duration::seconds(30))"#);

	assert!(err.contains("worker id"), "{err}");
}
