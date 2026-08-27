// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
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
		queue_schedule::{QueueDueKey, QueueItemStateKey, QueueKeyActiveKey, QueuePartitionKey},
	},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{
	change::QueueAckTransition,
	queue::scheduling::{ExpiredLease, apply_reap_transition},
	transaction::Transaction,
};
use reifydb_value::value::{Value, frame::frame::Frame, row_number::RowNumber};

const KEYED: &str =
	"CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH { fifo: { partitions: 1, ordered_by: tenant } }";

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

fn status_of(t: &TestEngine, queue: QueueId, row: u64) -> QueueItemStatus {
	states(t, queue)
		.into_iter()
		.find(|(key, _)| key.row.0 == row)
		.unwrap_or_else(|| panic!("item {row} must have a state record"))
		.1
		.status
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

fn due_rows(t: &TestEngine, queue: QueueId) -> Vec<u64> {
	let mut rows: Vec<u64> = dues(t, queue).iter().map(|due| due.row.0).collect();
	rows.sort_unstable();
	rows
}

fn chains(t: &TestEngine, queue: QueueId) -> Vec<QueueKeyActiveKey> {
	let store = t.inner().single().read_store();
	SingleVersionRange::range_batch(&store, QueueKeyActiveKey::queue_scan(queue), 1024)
		.unwrap()
		.items
		.iter()
		.map(|item| QueueKeyActiveKey::decode(&item.key).unwrap())
		.collect()
}

fn counters(t: &TestEngine, queue: QueueId, partition: u16) -> QueuePartitionCounters {
	let store = t.inner().single().read_store();
	SingleVersionGet::get(&store, &QueuePartitionKey::encoded(queue, partition))
		.unwrap()
		.map(|stored| decode_queue_partition_counters(EncodedPodRow::view(&stored.bytes)))
		.unwrap_or_default()
}

fn claim(t: &TestEngine, worker: &str, max_n: u32) -> Vec<(u64, String)> {
	let frames =
		t.command(&format!(r#"CALL queue::claim("{worker}", "test::jobs", {max_n}, duration::seconds(30))"#));
	let frame: &Frame = frames.first().expect("claim must always return a frame");

	let items = frame.columns.iter().find(|c| c.name == "item").expect("claim must return an item column");
	let tokens = frame.columns.iter().find(|c| c.name == "token").expect("claim must return a token column");

	(0..frame.row_count())
		.map(|i| {
			let item = match items.data.get_value(i) {
				Value::Uint8(n) => n,
				other => panic!("item column must be Uint8, got {other:?}"),
			};
			let token = match tokens.data.get_value(i) {
				Value::Utf8(t) => t,
				other => panic!("token column must be Utf8, got {other:?}"),
			};
			(item, token)
		})
		.collect()
}

fn claimed_rows(t: &TestEngine, worker: &str, max_n: u32) -> Vec<u64> {
	let mut rows: Vec<u64> = claim(t, worker, max_n).into_iter().map(|(item, _)| item).collect();
	rows.sort_unstable();
	rows
}

fn claim_row(t: &TestEngine, worker: &str, row: u64) -> String {
	let claimed = claim(t, worker, 10);
	assert_eq!(claimed.len(), 1, "expected exactly item {row} to be claimable, got {claimed:?}");
	assert_eq!(claimed[0].0, row, "expected item {row} to be the exposed head of its key");
	claimed[0].1.clone()
}

fn ack(t: &TestEngine, token: &str) {
	t.command(&format!(r#"CALL queue::ack("{token}")"#));
}

fn fail(t: &TestEngine, token: &str) {
	t.command(&format!(r#"CALL queue::fail("{token}", none)"#));
}

fn replay(t: &TestEngine, row: u64) -> String {
	let frames = t.command(&format!(r#"CALL queue::replay("test::jobs", {row})"#));
	match frames[0].columns.iter().find(|c| c.name == "state").unwrap().data.get_value(0) {
		Value::Utf8(state) => state,
		other => panic!("state must be Utf8, got {other:?}"),
	}
}

fn reap(t: &TestEngine, queue: QueueId, row: u64, transition: QueueAckTransition) -> bool {
	let (_, state) = states(t, queue).into_iter().find(|(key, _)| key.row.0 == row).unwrap();
	let now = t.mock_clock().now();

	apply_reap_transition(
		t.inner().single(),
		queue,
		0,
		&ExpiredLease {
			row: RowNumber(row),
			attempt: state.attempt,
			key_hash: Some(state.key_hash),
			lease_deadline: state.lease_deadline.expect("a reaped item must hold a lease"),
		},
		&transition,
		now,
	)
	.unwrap()
}

#[test]
fn test_a_younger_sibling_is_never_claimable_while_its_head_is_pending() {
	let t = engine_with_queue(KEYED);
	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "a" }, { id: 3, tenant: "b" }]"#);
	let queue = queue_id(&t, "jobs");

	assert_eq!(status_of(&t, queue, 1), QueueItemStatus::Ready);
	assert_eq!(status_of(&t, queue, 2), QueueItemStatus::Parked, "the second item of tenant a must park");
	assert_eq!(status_of(&t, queue, 3), QueueItemStatus::Ready, "a different key is unaffected");
	assert_eq!(due_rows(&t, queue), vec![1, 3], "a parked item must not reach the due index");

	let claimed = claim(&t, "w1", 10);
	let rows: Vec<u64> = claimed.iter().map(|(item, _)| *item).collect();
	assert_eq!(rows, vec![1, 3], "a claim asking for everything must still not see the parked sibling");

	let head = claimed.iter().find(|(item, _)| *item == 1).unwrap().1.clone();
	ack(&t, &head);

	assert_eq!(claimed_rows(&t, "w1", 10), vec![2], "acking the head must release its successor");
}

#[test]
fn test_a_retrying_head_keeps_blocking_across_its_backoff() {
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH { fifo: { partitions: 1, ordered_by: tenant }, retry: { attempts: 5 } }",
	);
	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "a" }]"#);
	let queue = queue_id(&t, "jobs");

	fail(&t, &claim_row(&t, "w1", 1));

	assert_eq!(status_of(&t, queue, 1), QueueItemStatus::Ready, "the head is retried, not finished");
	assert_eq!(status_of(&t, queue, 2), QueueItemStatus::Parked, "its sibling must not be promoted");
	assert!(claim(&t, "w1", 10).is_empty(), "nothing of this key is deliverable during the backoff");

	t.mock_clock().advance_millis(10_000);

	let redelivered = claim(&t, "w1", 10);
	assert_eq!(redelivered.len(), 1, "only the head returns, never the sibling");
	assert_eq!(redelivered[0].0, 1);
	assert_eq!(status_of(&t, queue, 2), QueueItemStatus::Parked);
}

#[test]
fn test_a_reaped_head_keeps_blocking_its_younger_sibling() {
	let t = engine_with_queue(KEYED);
	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "a" }]"#);
	let queue = queue_id(&t, "jobs");

	claim_row(&t, "w1", 1);
	t.mock_clock().advance_millis(60_000);

	assert!(reap(
		&t,
		queue,
		1,
		QueueAckTransition::Retry {
			backoff_until: t.mock_clock().now(),
		}
	));

	assert_eq!(status_of(&t, queue, 1), QueueItemStatus::Ready);
	assert_eq!(status_of(&t, queue, 2), QueueItemStatus::Parked, "a lost attempt must not release the key");
	assert_eq!(claimed_rows(&t, "w1", 10), vec![1], "the head comes back, the sibling stays put");
}

#[test]
fn test_a_head_delayed_by_not_before_blocks_its_younger_sibling() {
	let t = engine_with_queue(KEYED);
	t.command(
		r#"INSERT test::jobs [{ id: 1, tenant: "a" }] WITH { not_before: datetime::from_epoch_millis(60000) }"#,
	);
	t.command(r#"INSERT test::jobs [{ id: 2, tenant: "a" }]"#);
	let queue = queue_id(&t, "jobs");

	assert_eq!(status_of(&t, queue, 2), QueueItemStatus::Parked);
	assert!(claim(&t, "w1", 10).is_empty(), "the younger sibling must not overtake a delayed head");

	t.mock_clock().set_millis(60_000);

	ack(&t, &claim_row(&t, "w1", 1));

	assert_eq!(claimed_rows(&t, "w1", 10), vec![2], "and only then does the sibling follow");
}

#[test]
fn test_a_dead_head_releases_the_key_to_its_successor() {
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH { fifo: { partitions: 1, ordered_by: tenant }, retry: { attempts: 1 } }",
	);
	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "a" }]"#);
	let queue = queue_id(&t, "jobs");

	fail(&t, &claim_row(&t, "w1", 1));

	assert_eq!(status_of(&t, queue, 1), QueueItemStatus::Dead, "the budget of one attempt is spent");
	assert_eq!(status_of(&t, queue, 2), QueueItemStatus::Ready, "its successor must take over the key");
	assert_eq!(due_rows(&t, queue), vec![2]);
	assert_eq!(claimed_rows(&t, "w1", 10), vec![2]);
}

#[test]
fn test_a_dead_head_leaves_no_chain_entry_behind() {
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH { fifo: { partitions: 1, ordered_by: tenant }, retry: { attempts: 1 } }",
	);
	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "a" }]"#);
	let queue = queue_id(&t, "jobs");
	assert_eq!(chains(&t, queue).len(), 2, "both pending items belong to the key's chain");

	fail(&t, &claim_row(&t, "w1", 1));
	assert_eq!(chains(&t, queue).iter().map(|entry| entry.row.0).collect::<Vec<_>>(), vec![2]);

	ack(&t, &claim_row(&t, "w2", 2));
	assert!(chains(&t, queue).is_empty(), "a drained key must leave no chain entries");
}

#[test]
fn test_a_replayed_item_parks_behind_the_sibling_that_took_its_key() {
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH { fifo: { partitions: 1, ordered_by: tenant }, retry: { attempts: 1 } }",
	);
	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "a" }]"#);
	let queue = queue_id(&t, "jobs");

	fail(&t, &claim_row(&t, "w1", 1));
	assert_eq!(status_of(&t, queue, 1), QueueItemStatus::Dead);

	assert_eq!(replay(&t, 1), "parked", "the key is occupied, so replay must report a parked item");
	assert_eq!(status_of(&t, queue, 1), QueueItemStatus::Parked);
	assert_eq!(due_rows(&t, queue), vec![2], "a replayed item must not add a second due entry for its key");
	assert_eq!(counters(&t, queue, 0).blocked_keys, 1, "the key is blocked again");

	ack(&t, &claim_row(&t, "w2", 2));

	assert_eq!(claimed_rows(&t, "w2", 10), vec![1], "the replayed item runs after the sibling that overtook it");
}

#[test]
fn test_a_replayed_item_takes_an_empty_key_immediately() {
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH { fifo: { partitions: 1, ordered_by: tenant }, retry: { attempts: 1 } }",
	);
	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }]"#);
	let queue = queue_id(&t, "jobs");

	fail(&t, &claim_row(&t, "w1", 1));
	assert_eq!(status_of(&t, queue, 1), QueueItemStatus::Dead);

	assert_eq!(replay(&t, 1), "ready");
	assert_eq!(due_rows(&t, queue), vec![1]);
	assert_eq!(counters(&t, queue, 0).blocked_keys, 0);
	assert_eq!(claimed_rows(&t, "w1", 10), vec![1]);
}

#[test]
fn test_an_unkeyed_queue_writes_no_chain_entries_and_never_parks() {
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 }, retry: { attempts: 5 } }",
	);
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }]");
	let queue = queue_id(&t, "jobs");

	assert_eq!(claimed_rows(&t, "w1", 10), vec![1, 2, 3], "every item of an unkeyed queue is exposed at once");

	let claimed = claim(&t, "w1", 10);
	assert!(claimed.is_empty(), "all three are already leased");

	t.mock_clock().advance_millis(60_000);
	assert!(reap(
		&t,
		queue,
		1,
		QueueAckTransition::Retry {
			backoff_until: t.mock_clock().now(),
		}
	));

	for (_, token) in claim(&t, "w2", 10) {
		ack(&t, &token);
	}

	assert!(chains(&t, queue).is_empty(), "an unkeyed queue must never write a chain entry");
	assert!(
		states(&t, queue).iter().all(|(_, state)| state.status != QueueItemStatus::Parked),
		"an unkeyed queue must never park an item"
	);
	assert_eq!(counters(&t, queue, 0).blocked_keys, 0);
}

#[test]
fn test_blocked_keys_counts_keys_that_hold_a_waiting_sibling() {
	let t = engine_with_queue(KEYED);
	let queue = queue_id(&t, "jobs");

	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }]"#);
	assert_eq!(counters(&t, queue, 0).blocked_keys, 0, "a key with one item blocks nothing");

	t.command(r#"INSERT test::jobs [{ id: 2, tenant: "a" }]"#);
	assert_eq!(counters(&t, queue, 0).blocked_keys, 1, "the second item of a key makes it blocked");

	t.command(r#"INSERT test::jobs [{ id: 3, tenant: "a" }]"#);
	assert_eq!(counters(&t, queue, 0).blocked_keys, 1, "a third item does not block the key twice");

	t.command(r#"INSERT test::jobs [{ id: 4, tenant: "b" }, { id: 5, tenant: "b" }]"#);
	assert_eq!(counters(&t, queue, 0).blocked_keys, 2, "a second blocked key counts separately");

	let claimed = claim(&t, "w1", 10);
	assert_eq!(claimed.len(), 2, "exactly one head per key is exposed at a time");
	let head_of = |item: u64| claimed.iter().find(|(row, _)| *row == item).expect("head must be claimed").1.clone();

	ack(&t, &head_of(1));
	assert_eq!(counters(&t, queue, 0).blocked_keys, 2, "tenant a still has two items behind its new head");

	ack(&t, &head_of(4));
	assert_eq!(
		counters(&t, queue, 0).blocked_keys,
		1,
		"tenant b's last item is now its head, so only tenant a stays blocked"
	);

	loop {
		let batch = claim(&t, "w1", 10);
		if batch.is_empty() {
			break;
		}
		for (_, token) in batch {
			ack(&t, &token);
		}
	}
	assert_eq!(counters(&t, queue, 0).blocked_keys, 0, "draining both keys must return the counter to zero");
	assert_eq!(counters(&t, queue, 0).depth, 0);
}
