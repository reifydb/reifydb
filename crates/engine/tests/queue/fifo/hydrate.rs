// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::BTreeMap, slice::from_ref, sync::Arc};

use reifydb_codec::{key::encoded::EncodedKeyRange, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::{
		catalog::queue::{
			Queue, QueueItemState, QueueItemStatus, QueuePartitionCounters, decode_queue_item_state,
			decode_queue_partition_counters, encode_queue_item_state, encode_queue_partition_counters,
		},
		store::{SingleVersionGet, SingleVersionRange, SingleVersionRow},
	},
	key::{
		any::TaggedKey,
		queue::{QueueDueKey, QueueItemStateKey, QueueKeyActiveKey, QueuePartitionKey},
	},
};
use reifydb_engine::queue::hydrate::hydrate_queues;
use reifydb_runtime::context::{RuntimeContext, transaction::TransactionContext};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{
	queue::scheduling::queue_partition_counters, single::write::SingleWriteTransaction, transaction::Transaction,
};
use reifydb_value::value::{datetime::DateTime, partition::Partition, row_number::RowNumber};

fn system_ctx() -> Arc<TransactionContext> {
	Arc::new(TransactionContext::system(RuntimeContext::testing(0, 0)))
}

fn engine_with_queue(declaration: &str) -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin(declaration);
	t
}

fn find_queue(t: &TestEngine, name: &str) -> Queue {
	let catalog = t.inner().catalog();
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
	catalog.find_queue_by_name(&mut txn, namespace.id(), name).unwrap().unwrap()
}

const SCAN_LIMIT: u64 = 8192;

fn scan(t: &TestEngine, range: EncodedKeyRange) -> Vec<SingleVersionRow> {
	let store = t.inner().single().read_store();
	let batch = SingleVersionRange::range_batch(&store, &system_ctx(), range, SCAN_LIMIT).unwrap();
	assert!(!batch.has_more, "the scan limit is too small to observe the whole keyspace under test");
	batch.items
}

fn states(t: &TestEngine, queue: &Queue) -> BTreeMap<RowNumber, (Partition, QueueItemState)> {
	scan(t, QueueItemStateKey::queue_scan(queue.id).encode())
		.iter()
		.map(|item| {
			let key = QueueItemStateKey::decode(&item.key).unwrap();
			(key.row, (key.partition, decode_queue_item_state(EncodedPodRow::view(&item.bytes)).unwrap()))
		})
		.collect()
}

fn dues(t: &TestEngine, queue: &Queue) -> BTreeMap<RowNumber, QueueDueKey> {
	scan(t, QueueDueKey::queue_scan(queue.id).encode())
		.iter()
		.map(|item| {
			let key = QueueDueKey::decode(&item.key).unwrap();
			(key.row, key)
		})
		.collect()
}

fn partitions(t: &TestEngine, queue: &Queue) -> Vec<Partition> {
	queue_partition_counters(&system_ctx(), t.inner().single(), queue.id)
		.unwrap()
		.into_iter()
		.map(|(partition, _)| partition)
		.collect()
}

fn counters(t: &TestEngine, queue: &Queue, partition: Partition) -> QueuePartitionCounters {
	let store = t.inner().single().read_store();
	SingleVersionGet::get(&store, &system_ctx(), &[QueuePartitionKey::encoded(queue.id, partition)])
		.unwrap()
		.pop()
		.flatten()
		.map(|stored| decode_queue_partition_counters(EncodedPodRow::view(&stored.bytes)))
		.unwrap_or_default()
}

fn total_depth(t: &TestEngine, queue: &Queue) -> u64 {
	partitions(t, queue).into_iter().map(|partition| counters(t, queue, partition).depth).sum()
}

fn keys_in(t: &TestEngine, range: EncodedKeyRange) -> Vec<TaggedKey> {
	scan(t, range).iter().map(|item| TaggedKey::decode(&item.key).unwrap()).collect()
}

fn with_partition<F>(t: &TestEngine, queue: &Queue, partition: Partition, f: F)
where
	F: FnOnce(&mut SingleWriteTransaction<'_>),
{
	let single = t.inner().single();
	let lock_key = QueuePartitionKey::encoded(queue.id, partition);
	let mut tx = single
		.begin_command_ranged(
			&system_ctx(),
			[&lock_key],
			vec![
				QueueItemStateKey::partition_scan(queue.id, partition).encode(),
				QueueDueKey::partition_scan(queue.id, partition).encode(),
				QueueKeyActiveKey::partition_scan(queue.id, partition).encode(),
			],
		)
		.unwrap();
	f(&mut tx);
	tx.commit().unwrap();
}

fn crash_before_handoff(t: &TestEngine, queue: &Queue) {
	for partition in partitions(t, queue) {
		let state_keys = keys_in(t, QueueItemStateKey::partition_scan(queue.id, partition).encode());
		let due_keys = keys_in(t, QueueDueKey::partition_scan(queue.id, partition).encode());
		let chain_keys = keys_in(t, QueueKeyActiveKey::partition_scan(queue.id, partition).encode());
		if state_keys.is_empty() && due_keys.is_empty() {
			continue;
		}

		with_partition(t, queue, partition, |tx| {
			for key in state_keys.iter().chain(due_keys.iter()).chain(chain_keys.iter()) {
				tx.remove(from_ref(key)).unwrap();
			}
			tx.remove(from_ref(&QueuePartitionKey::new(queue.id, partition))).unwrap();
		});
	}
}

fn forget_item(t: &TestEngine, queue: &Queue, row: RowNumber) {
	let (partition, _) = states(t, queue)[&row];
	let due = dues(t, queue)[&row].clone();
	let mut counters = counters(t, queue, partition);
	counters.depth -= 1;

	with_partition(t, queue, partition, |tx| {
		tx.remove(from_ref(&QueueItemStateKey::new(queue.id, partition, row))).unwrap();
		tx.remove(from_ref(&due)).unwrap();
		tx.set(vec![(QueuePartitionKey::new(queue.id, partition), encode_queue_partition_counters(&counters))])
			.unwrap();
	});
}

#[test]
fn test_hydration_recreates_a_lost_handoff() {
	// Post-commit interceptors are never replayed after a crash, so an item whose enqueue
	// committed while the handoff was lost has no scheduling state at all. Without hydration that
	// work is durable but permanently invisible: the row is queryable and nobody will ever claim
	// it.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }]");

	let queue = find_queue(&t, "jobs");
	crash_before_handoff(&t, &queue);
	assert!(states(&t, &queue).is_empty(), "the crash image must have no scheduling state left");

	assert_eq!(hydrate_queues(t.inner()).unwrap(), 3);

	let states = states(&t, &queue);
	assert_eq!(states.len(), 3);
	for (partition, state) in states.values() {
		assert_eq!(*partition, Queue::UNPARTITIONED);
		assert_eq!(state.status, QueueItemStatus::Ready, "recovered items must be claimable, not parked");
		assert_eq!(state.attempt, 0);
	}
	assert_eq!(dues(&t, &queue).len(), 3);
	assert_eq!(total_depth(&t, &queue), 3);
}

#[test]
fn test_hydration_recovers_not_before_from_the_stored_row() {
	// not_before lives in the item row precisely so a crash cannot lose it. If hydration admitted
	// crash-window items at epoch instead, a job scheduled for next week would run at the next
	// boot, which is the one failure mode a delayed enqueue must never have.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command(r#"INSERT test::jobs [{ id: 1 }] WITH { not_before: datetime::from_epoch_millis(1700000000000) }"#);

	let queue = find_queue(&t, "jobs");
	let expected = DateTime::from_nanos(1_700_000_000_000 * 1_000_000);
	crash_before_handoff(&t, &queue);

	assert_eq!(hydrate_queues(t.inner()).unwrap(), 1);

	let states = states(&t, &queue);
	let (_, state) = states.values().next().unwrap();
	assert_eq!(state.not_before, Some(expected), "the recovered state must carry the stored not_before");
	assert_eq!(dues(&t, &queue).values().next().unwrap().due, expected, "and so must the due index entry");
}

#[test]
fn test_hydration_admits_only_the_items_that_are_missing() {
	// Create-if-absent is what makes hydration safe to run beside a live interceptor. If it
	// re-admitted an item that already had state, the depth counter would double-count and the
	// due index would hold two entries for one item, delivering it twice.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }]");

	let queue = find_queue(&t, "jobs");
	let lost = *states(&t, &queue).keys().next().unwrap();
	forget_item(&t, &queue, lost);
	assert_eq!(total_depth(&t, &queue), 2);

	assert_eq!(hydrate_queues(t.inner()).unwrap(), 1, "only the forgotten item may be admitted");

	assert_eq!(states(&t, &queue).len(), 3);
	assert_eq!(dues(&t, &queue).len(), 3, "the surviving items must not gain a second due entry");
	assert_eq!(total_depth(&t, &queue), 3);
}

#[test]
fn test_hydration_is_idempotent() {
	// Hydration runs on every boot, including boots that never crashed. A second pass that
	// admitted anything would inflate depth on every restart until the queue reported work it
	// does not hold.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }, { id: 3 }]");

	let queue = find_queue(&t, "jobs");
	let before = states(&t, &queue);

	assert_eq!(hydrate_queues(t.inner()).unwrap(), 0, "a healthy store must admit nothing");
	assert_eq!(hydrate_queues(t.inner()).unwrap(), 0);

	assert_eq!(states(&t, &queue), before);
	assert_eq!(dues(&t, &queue).len(), 3);
	assert_eq!(total_depth(&t, &queue), 3);
}

#[test]
fn test_hydration_does_not_re_admit_an_item_that_reached_a_terminal_status() {
	// An acked item keeps its state record and loses its due entry. Hydration keys off the state
	// record alone, so a rule that looked for a missing due entry instead would resurrect
	// finished work on every boot - the exact duplicate-delivery ack exists to prevent.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");
	t.command("INSERT test::jobs [{ id: 1 }, { id: 2 }]");

	let queue = find_queue(&t, "jobs");
	let done = *states(&t, &queue).keys().next().unwrap();
	let due = dues(&t, &queue)[&done].clone();
	let mut counters = counters(&t, &queue, Queue::UNPARTITIONED);
	counters.depth -= 1;

	with_partition(&t, &queue, Queue::UNPARTITIONED, |tx| {
		let mut state = QueueItemState::ready(None);
		state.status = QueueItemStatus::Done;
		tx.set(vec![(
			QueueItemStateKey::new(queue.id, Queue::UNPARTITIONED, done),
			encode_queue_item_state(&state),
		)])
		.unwrap();
		tx.remove(from_ref(&due)).unwrap();
		tx.set(vec![(
			QueuePartitionKey::new(queue.id, Queue::UNPARTITIONED),
			encode_queue_partition_counters(&counters),
		)])
		.unwrap();
	});

	assert_eq!(hydrate_queues(t.inner()).unwrap(), 0, "a terminal item must not be re-admitted");

	assert_eq!(states(&t, &queue)[&done].1.status, QueueItemStatus::Done, "its status must not be reset");
	assert!(!dues(&t, &queue).contains_key(&done), "and it must not reappear in the due index");
	assert_eq!(total_depth(&t, &queue), 1);
}

#[test]
fn test_hydration_recovers_a_queue_that_outgrows_one_scan_batch() {
	// The item scan and the per-partition admit both page at 1024 entries. A queue that fits in a
	// single batch never moves the cursor, so this is the only test that fails if paging stalls or
	// drops the tail - and a real queue would then come back from a crash with its first batch
	// recovered and every later item permanently unclaimable.
	let t = engine_with_queue("CREATE QUEUE test::jobs { id: int4 } WITH { fifo: {} }");

	const ITEMS: usize = 1100;
	for chunk in 0..11 {
		let rows: Vec<String> = (0..100).map(|i| format!("{{ id: {} }}", chunk * 100 + i)).collect();
		t.command(&format!("INSERT test::jobs [{}]", rows.join(", ")));
	}

	let queue = find_queue(&t, "jobs");
	assert_eq!(states(&t, &queue).len(), ITEMS);

	crash_before_handoff(&t, &queue);
	assert!(states(&t, &queue).is_empty());

	assert_eq!(
		hydrate_queues(t.inner()).unwrap(),
		ITEMS as u64,
		"every item must be admitted, not just the first batch"
	);

	assert_eq!(states(&t, &queue).len(), ITEMS);
	assert_eq!(dues(&t, &queue).len(), ITEMS);
	assert_eq!(total_depth(&t, &queue), ITEMS as u64);
}

#[test]
fn test_hydration_recomputes_the_original_partition_of_an_ordered_item() {
	// Partition assignment must be recomputable from the stored row alone. If hydration placed a
	// recovered item in a different partition, per-key FIFO would break: two items of one key
	// would sit in two partitions and could be worked concurrently and out of order.
	let t = engine_with_queue(
		"CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH { partition: { by: { tenant } }, fifo: { ordered_by: tenant } }",
	);
	t.command(
		r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "b" }, { id: 3, tenant: "c" }, { id: 4, tenant: "a" }]"#,
	);

	let queue = find_queue(&t, "jobs");
	let before: BTreeMap<RowNumber, Partition> =
		states(&t, &queue).iter().map(|(row, (p, _))| (*row, *p)).collect();
	crash_before_handoff(&t, &queue);

	assert_eq!(hydrate_queues(t.inner()).unwrap(), 4);

	let after: BTreeMap<RowNumber, Partition> = states(&t, &queue).iter().map(|(row, (p, _))| (*row, *p)).collect();
	assert_eq!(after, before, "every recovered item must land in the partition it was enqueued to");

	let dues = dues(&t, &queue);
	assert_eq!(dues.len(), 3, "the second item of tenant a is parked behind its sibling, so it has no due entry");
	for (row, partition) in &before {
		let Some(due) = dues.get(row) else {
			continue;
		};
		assert_eq!(due.partition, *partition, "the due entry must follow the item's partition");
	}
}
