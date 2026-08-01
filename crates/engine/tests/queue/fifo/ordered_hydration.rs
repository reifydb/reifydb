// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::bytes::RowBuilder,
};
use reifydb_core::{
	interface::{
		catalog::queue::{
			Queue, QueueItemState, QueueItemStatus, QueuePartitionCounters, decode_queue_item_state,
			decode_queue_partition_counters, encode_queue_partition_counters,
		},
		store::{SingleVersionGet, SingleVersionRange, SingleVersionRow},
	},
	key::{
		EncodableKey,
		queue_schedule::{QueueDueKey, QueueItemStateKey, QueueKeyActiveKey, QueuePartitionKey},
	},
};
use reifydb_engine::queue::hydrate::hydrate_queues;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::{single::write::SingleWriteTransaction, transaction::Transaction};
use reifydb_value::value::row_number::RowNumber;

const KEYED: &str =
	"CREATE QUEUE test::jobs { id: int4, tenant: utf8 } WITH { fifo: { partitions: 1, ordered_by: tenant } }";

const SCAN_LIMIT: u64 = 8192;

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

fn scan(t: &TestEngine, range: EncodedKeyRange) -> Vec<SingleVersionRow> {
	let store = t.inner().single().read_store();
	let batch = SingleVersionRange::range_batch(&store, range, SCAN_LIMIT).unwrap();
	assert!(!batch.has_more, "the scan limit is too small to observe the whole keyspace under test");
	batch.items
}

fn states(t: &TestEngine, queue: &Queue) -> BTreeMap<RowNumber, QueueItemState> {
	scan(t, QueueItemStateKey::queue_scan(queue.id))
		.iter()
		.map(|item| {
			let key = QueueItemStateKey::decode(&item.key).unwrap();
			(key.row, decode_queue_item_state(&item.bytes).unwrap())
		})
		.collect()
}

fn statuses(t: &TestEngine, queue: &Queue) -> BTreeMap<u64, QueueItemStatus> {
	states(t, queue).into_iter().map(|(row, state)| (row.0, state.status)).collect()
}

fn due_rows(t: &TestEngine, queue: &Queue) -> Vec<u64> {
	let mut rows: Vec<u64> = scan(t, QueueDueKey::queue_scan(queue.id))
		.iter()
		.map(|item| QueueDueKey::decode(&item.key).unwrap().row.0)
		.collect();
	rows.sort_unstable();
	rows
}

fn chain_rows(t: &TestEngine, queue: &Queue) -> Vec<u64> {
	let mut rows: Vec<u64> = scan(t, QueueKeyActiveKey::queue_scan(queue.id))
		.iter()
		.map(|item| QueueKeyActiveKey::decode(&item.key).unwrap().row.0)
		.collect();
	rows.sort_unstable();
	rows
}

fn counters(t: &TestEngine, queue: &Queue, partition: u16) -> QueuePartitionCounters {
	let store = t.inner().single().read_store();
	SingleVersionGet::get(&store, &QueuePartitionKey::encoded(queue.id, partition))
		.unwrap()
		.map(|stored| decode_queue_partition_counters(&stored.bytes))
		.unwrap_or_default()
}

fn keys_in(t: &TestEngine, range: EncodedKeyRange) -> Vec<EncodedKey> {
	scan(t, range).into_iter().map(|item| item.key).collect()
}

fn with_partition<F>(t: &TestEngine, queue: &Queue, partition: u16, f: F)
where
	F: FnOnce(&mut SingleWriteTransaction<'_>),
{
	let single = t.inner().single();
	let lock_key = QueuePartitionKey::encoded(queue.id, partition);
	let mut tx = single
		.begin_command_ranged(
			[&lock_key],
			vec![
				QueueItemStateKey::partition_scan(queue.id, partition),
				QueueDueKey::partition_scan(queue.id, partition),
				QueueKeyActiveKey::partition_scan(queue.id, partition),
			],
		)
		.unwrap();
	f(&mut tx);
	tx.commit().unwrap();
}

fn crash_before_handoff(t: &TestEngine, queue: &Queue) {
	for partition in 0..queue.partitions() {
		let keys: Vec<EncodedKey> = keys_in(t, QueueItemStateKey::partition_scan(queue.id, partition))
			.into_iter()
			.chain(keys_in(t, QueueDueKey::partition_scan(queue.id, partition)))
			.chain(keys_in(t, QueueKeyActiveKey::partition_scan(queue.id, partition)))
			.collect();
		if keys.is_empty() {
			continue;
		}

		with_partition(t, queue, partition, |tx| {
			for key in &keys {
				tx.remove(key).unwrap();
			}
			tx.remove(&QueuePartitionKey::encoded(queue.id, partition)).unwrap();
		});
	}
}

fn forget_items(t: &TestEngine, queue: &Queue, rows: &[u64], blocked_delta: u64) {
	let doomed: Vec<EncodedKey> = keys_in(t, QueueItemStateKey::partition_scan(queue.id, 0))
		.into_iter()
		.filter(|key| rows.contains(&QueueItemStateKey::decode(key).unwrap().row.0))
		.chain(keys_in(t, QueueDueKey::partition_scan(queue.id, 0))
			.into_iter()
			.filter(|key| rows.contains(&QueueDueKey::decode(key).unwrap().row.0)))
		.chain(keys_in(t, QueueKeyActiveKey::partition_scan(queue.id, 0))
			.into_iter()
			.filter(|key| rows.contains(&QueueKeyActiveKey::decode(key).unwrap().row.0)))
		.collect();

	let mut counters = counters(t, queue, 0);
	counters.depth -= rows.len() as u64;
	counters.blocked_keys -= blocked_delta;

	with_partition(t, queue, 0, |tx| {
		for key in &doomed {
			tx.remove(key).unwrap();
		}
		tx.set(&QueuePartitionKey::encoded(queue.id, 0), encode_queue_partition_counters(&counters).freeze_bytes())
			.unwrap();
	});
}

fn claim(t: &TestEngine, worker: &str, max_n: u32) -> usize {
	TestEngine::row_count(
		&t.command(&format!(r#"CALL queue::claim("{worker}", "test::jobs", {max_n}, duration::seconds(30))"#)),
	)
}

#[test]
fn test_hydration_exposes_the_oldest_item_of_a_key_not_the_newest() {
	let t = engine_with_queue(KEYED);
	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "a" }, { id: 3, tenant: "a" }]"#);
	let queue = find_queue(&t, "jobs");
	crash_before_handoff(&t, &queue);

	assert_eq!(hydrate_queues(t.inner()).unwrap(), 3);

	assert_eq!(statuses(&t, &queue)[&1], QueueItemStatus::Ready, "the oldest item must be the exposed head");
	assert_eq!(statuses(&t, &queue)[&2], QueueItemStatus::Parked);
	assert_eq!(statuses(&t, &queue)[&3], QueueItemStatus::Parked);
	assert_eq!(due_rows(&t, &queue), vec![1]);
}

#[test]
fn test_hydration_parks_recovered_items_behind_an_occupied_key() {
	let t = engine_with_queue(KEYED);
	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "b" }]"#);
	let queue = find_queue(&t, "jobs");

	t.command(r#"INSERT test::jobs [{ id: 3, tenant: "a" }, { id: 4, tenant: "c" }]"#);
	forget_items(&t, &queue, &[3, 4], 1);

	assert_eq!(hydrate_queues(t.inner()).unwrap(), 2, "only the two forgotten items are re-admitted");

	assert_eq!(statuses(&t, &queue)[&1], QueueItemStatus::Ready, "the untouched head keeps the key");
	assert_eq!(statuses(&t, &queue)[&3], QueueItemStatus::Parked, "its recovered sibling must park behind it");
	assert_eq!(statuses(&t, &queue)[&4], QueueItemStatus::Ready, "an empty key is exposed as usual");
	assert_eq!(due_rows(&t, &queue), vec![1, 2, 4]);
}

#[test]
fn test_hydration_of_an_untouched_queue_changes_nothing() {
	let t = engine_with_queue(KEYED);
	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "a" }, { id: 3, tenant: "b" }]"#);
	let queue = find_queue(&t, "jobs");
	assert_eq!(claim(&t, "w1", 1), 1, "one head is leased across the restart");

	let before_states = states(&t, &queue);
	let before_dues = due_rows(&t, &queue);
	let before_chain = chain_rows(&t, &queue);
	let before_counters = counters(&t, &queue, 0);

	assert_eq!(hydrate_queues(t.inner()).unwrap(), 0, "nothing is missing, so nothing is admitted");

	assert_eq!(states(&t, &queue), before_states);
	assert_eq!(due_rows(&t, &queue), before_dues);
	assert_eq!(chain_rows(&t, &queue), before_chain);
	assert_eq!(counters(&t, &queue, 0), before_counters);
}

#[test]
fn test_hydration_rebuilds_the_chain_of_every_recovered_item() {
	let t = engine_with_queue(KEYED);
	t.command(r#"INSERT test::jobs [{ id: 1, tenant: "a" }, { id: 2, tenant: "a" }, { id: 3, tenant: "b" }]"#);
	let queue = find_queue(&t, "jobs");
	crash_before_handoff(&t, &queue);
	assert!(chain_rows(&t, &queue).is_empty(), "the crash image has no chain left");

	assert_eq!(hydrate_queues(t.inner()).unwrap(), 3);

	assert_eq!(chain_rows(&t, &queue), vec![1, 2, 3], "every pending item is back in its key's chain");
	assert_eq!(counters(&t, &queue, 0).depth, 3);
	assert_eq!(counters(&t, &queue, 0).blocked_keys, 1, "tenant a blocks, tenant b does not");
}
