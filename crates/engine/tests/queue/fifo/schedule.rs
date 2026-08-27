// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

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
		queue_schedule::{QueueDueKey, QueueItemStateKey, QueuePartitionKey},
	},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::datetime::DateTime;

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

fn counter_rows(t: &TestEngine, queue: QueueId) -> Vec<(QueuePartitionKey, QueuePartitionCounters)> {
	let store = t.inner().single().read_store();
	SingleVersionRange::range_batch(&store, QueuePartitionKey::queue_scan(queue), 1024)
		.unwrap()
		.items
		.iter()
		.map(|item| {
			(
				QueuePartitionKey::decode(&item.key).unwrap(),
				decode_queue_partition_counters(EncodedPodRow::view(&item.bytes)),
			)
		})
		.collect()
}

#[test]
fn test_an_insert_creates_a_ready_state_a_due_entry_and_depth() {
	// The post-commit handoff is the only writer of scheduling state, so this asserts the whole
	// contract at once: without the state record no worker can ever claim the item, without the
	// due entry no scan will find it, and without the depth bump the queue reports itself empty
	// while holding work.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::jobs [{ id: 1 }]");

	let queue = queue_id(&t, "jobs");

	let states = states(&t, queue);
	assert_eq!(states.len(), 1, "one item must leave exactly one state record");
	let (key, state) = &states[0];
	assert_eq!(key.partition, 0);
	assert_eq!(state.status, QueueItemStatus::Ready);
	assert_eq!(state.attempt, 0);
	assert_eq!(state.not_before, None);
	assert_eq!(state.lease_deadline, None);
	assert_eq!(state.backoff_until, None);

	let dues = dues(&t, queue);
	assert_eq!(dues.len(), 1, "one item must leave exactly one due entry");
	assert_eq!(dues[0].due, DateTime::from_nanos(0), "an item with no not_before is due at epoch");
	assert_eq!(dues[0].row, key.row, "the due entry must point at the state record's item");

	assert_eq!(counters(&t, queue, 0).depth, 1);
	assert_eq!(counters(&t, queue, 0).in_flight, 0, "nothing is claimed yet");
}

#[test]
fn test_not_before_reaches_the_due_index_and_the_state_record() {
	// A delayed item that lands at epoch would be delivered immediately, which is exactly the
	// guarantee not_before exists to make. Both the index key and the record must carry it: the
	// index decides when it is scanned, the record is what hydration and the reaper read back.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command(r#"INSERT test::jobs [{ id: 1 }] WITH { not_before: datetime::from_epoch_millis(1700000000000) }"#);

	let queue = queue_id(&t, "jobs");
	let expected = DateTime::from_nanos(1_700_000_000_000 * 1_000_000);

	assert_eq!(dues(&t, queue)[0].due, expected);
	assert_eq!(states(&t, queue)[0].1.not_before, Some(expected));
}

#[test]
fn test_a_multi_partition_insert_counts_each_partition_separately() {
	// Claims are per-partition, so a depth that is summed globally or attributed to the wrong
	// partition would send workers to look for work where none is indexed.
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH { fifo: { partitions: 8, ordered_by: tenant } }",
	);
	t.command(
		r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "b" }, { id: 3, tenant: "c" }, { id: 4, tenant: "a" }]"#,
	);

	let queue = queue_id(&t, "jobs");

	let states = states(&t, queue);
	let dues = dues(&t, queue);
	assert_eq!(states.len(), 4, "every item needs its own state record");
	assert_eq!(
		dues.len(),
		3,
		"the second item of tenant a parks behind its sibling instead of entering the due index"
	);

	let mut per_partition: BTreeMap<u16, u64> = BTreeMap::new();
	for (key, _) in &states {
		*per_partition.entry(key.partition).or_default() += 1;
	}

	let rows = counter_rows(&t, queue);
	assert_eq!(rows.len(), per_partition.len(), "a counter row exists only for partitions that took work");
	for (key, counters) in rows {
		assert_eq!(
			counters.depth, per_partition[&key.partition],
			"partition {} must count exactly its own items",
			key.partition
		);
	}

	assert_eq!(per_partition.values().sum::<u64>(), 4);
}

#[test]
fn test_an_aborted_insert_leaves_no_scheduling_state() {
	// Single-lane writes commit immediately and survive an aborted command, which is precisely
	// why admission is deferred to post-commit. A regression that admits at statement time would
	// leave a ready item pointing at a row that was rolled back, and a worker would claim work
	// that does not exist.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 1 } }");

	let err = t.command_err(r#"INSERT test::jobs [{ id: 1 }]; INSERT test::jobs [{ id: "not-a-number" }]"#);
	assert!(!err.is_empty(), "the malformed statement must fault");

	let queue = queue_id(&t, "jobs");

	assert_eq!(t.query("FROM test::jobs")[0].rows().count(), 0, "the whole command must roll back");
	assert!(states(&t, queue).is_empty(), "an aborted insert must leave no state record");
	assert!(dues(&t, queue).is_empty(), "an aborted insert must leave no due entry");
	assert_eq!(counters(&t, queue, 0), QueuePartitionCounters::default());
}

#[test]
fn test_drop_queue_wipes_the_scheduling_keyspace_of_that_queue_only() {
	// Queue ids are reused after a drop, so scheduling records left behind would be re-adopted by
	// whatever queue takes the id next, and hydration would re-admit items of a queue that no
	// longer exists.
	let t = engine_with_queue("CREATE QUEUE test::a { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.admin("CREATE QUEUE test::b { id: int4 } WITH { fifo: { partitions: 1 } }");
	t.command("INSERT test::a [{ id: 1 }, { id: 2 }]");
	t.command("INSERT test::b [{ id: 1 }]");

	let dropped = queue_id(&t, "a");
	let kept = queue_id(&t, "b");
	assert_eq!(states(&t, dropped).len(), 2);

	t.admin("DROP QUEUE test::a");

	assert!(states(&t, dropped).is_empty(), "state records must not outlive the queue");
	assert!(dues(&t, dropped).is_empty(), "due entries must not outlive the queue");
	assert!(counter_rows(&t, dropped).is_empty(), "counter rows must not outlive the queue");

	assert_eq!(states(&t, kept).len(), 1, "the surviving queue must keep its scheduling state");
	assert_eq!(counters(&t, kept, 0).depth, 1);
}

#[test]
fn test_a_keyed_queue_records_one_key_hash_per_ordered_by_value() {
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH { fifo: { partitions: 1, ordered_by: tenant } }",
	);
	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "b" }, { id: 3, tenant: "a" }]"#);

	let queue = queue_id(&t, "jobs");
	let by_row: BTreeMap<u64, u64> =
		states(&t, queue).iter().map(|(key, state)| (key.row.0, state.key_hash)).collect();

	assert_eq!(by_row.len(), 3);
	assert_eq!(by_row[&1], by_row[&3], "two items of tenant a must share one key hash");
	assert_ne!(by_row[&1], by_row[&2], "tenant b must hash into its own chain");
}

#[test]
fn test_an_unkeyed_queue_stores_no_key_hash() {
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: { partitions: 4 } }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }]");

	let queue = queue_id(&t, "jobs");

	for (key, state) in states(&t, queue) {
		assert_eq!(state.key_hash, 0, "item {} of an unkeyed queue must carry no key hash", key.row.0);
	}
}
