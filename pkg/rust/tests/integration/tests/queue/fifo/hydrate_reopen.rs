// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{
	core::{
		interface::{
			catalog::{id::QueueId, queue::decode_queue_partition_counters},
			store::{SingleVersionGet, SingleVersionRange},
		},
		key::queue_schedule::{QueueDueKey, QueueItemStateKey, QueuePartitionKey},
	},
	testing::db::{TempDbPath, TestDb},
	transaction::transaction::Transaction,
};
use reifydb_codec::key::encoded::{EncodedKey, EncodedKeyRange};
use reifydb_test_harness::engine::AsEngine;
use reifydb_value::value::identity::IdentityId;

fn queue_id(db: &TestDb, name: &str) -> QueueId {
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut query = engine.begin_query(IdentityId::root()).unwrap();
	let mut txn = Transaction::Query(&mut query);
	let namespace = catalog.find_namespace_by_name(&mut txn, "p").unwrap().unwrap();
	catalog.find_queue_by_name(&mut txn, namespace.id(), name).unwrap().unwrap().id
}

fn keys(db: &TestDb, range: EncodedKeyRange) -> Vec<EncodedKey> {
	let store = db.engine().single().read_store();
	SingleVersionRange::range_batch(&store, range, 1024)
		.unwrap()
		.items
		.iter()
		.map(|item| item.key.clone())
		.collect()
}

fn depth(db: &TestDb, queue: QueueId, partition: u16) -> u64 {
	let store = db.engine().single().read_store();
	SingleVersionGet::get(&store, &QueuePartitionKey::encoded(queue, partition))
		.unwrap()
		.map(|stored| decode_queue_partition_counters(&stored.bytes).depth)
		.unwrap_or_default()
}

fn wipe_scheduling(db: &TestDb, queue: QueueId) {
	let state_keys = keys(db, QueueItemStateKey::partition_scan(queue, 0));
	let due_keys = keys(db, QueueDueKey::partition_scan(queue, 0));

	let single = db.engine().single();
	let lock_key = QueuePartitionKey::encoded(queue, 0);
	let mut tx = single
		.begin_command_ranged(
			[&lock_key],
			vec![QueueItemStateKey::partition_scan(queue, 0), QueueDueKey::partition_scan(queue, 0)],
		)
		.unwrap();
	for key in state_keys.iter().chain(due_keys.iter()) {
		tx.remove(key).unwrap();
	}
	tx.remove(&lock_key).unwrap();
	tx.commit().unwrap();
}

#[test]
fn scheduling_state_is_rebuilt_after_a_sqlite_reopen() {
	// Engine-level hydration tests call hydrate_queues directly and so cannot notice that the
	// Bootloader never calls it. This is the only test that fails if the boot wiring is dropped,
	// and without that wiring every item whose handoff a crash swallowed stays unclaimable
	// forever.
	let path = TempDbPath::new("queue_hydrate_reopen");

	{
		let mut db = TestDb::sqlite_at(&path);
		db.admin("create namespace p; create queue p::jobs { id: int4 } with { fifo: { partitions: 1 } };");
		db.command("insert p::jobs [{ id: 1 }, { id: 2 }];");

		let queue = queue_id(&db, "jobs");
		assert_eq!(keys(&db, QueueItemStateKey::partition_scan(queue, 0)).len(), 2);

		wipe_scheduling(&db, queue);
		assert!(keys(&db, QueueItemStateKey::partition_scan(queue, 0)).is_empty());

		db.stop();
	}

	let mut db = TestDb::sqlite_at(&path);
	let queue = queue_id(&db, "jobs");

	assert_eq!(keys(&db, QueueItemStateKey::partition_scan(queue, 0)).len(), 2, "boot must re-admit both items");
	assert_eq!(keys(&db, QueueDueKey::partition_scan(queue, 0)).len(), 2);
	assert_eq!(depth(&db, queue, 0), 2);

	db.stop();
}

#[test]
fn a_reopen_over_intact_state_changes_nothing() {
	// Hydration runs on every boot, so the healthy path has to be a no-op. A second admission
	// would add a duplicate due entry and inflate depth on each restart, and the queue would
	// deliver the same item twice.
	let path = TempDbPath::new("queue_hydrate_reopen_intact");

	{
		let mut db = TestDb::sqlite_at(&path);
		db.admin("create namespace p; create queue p::jobs { id: int4 } with { fifo: { partitions: 1 } };");
		db.command("insert p::jobs [{ id: 1 }, { id: 2 }];");
		db.stop();
	}

	let mut db = TestDb::sqlite_at(&path);
	let queue = queue_id(&db, "jobs");

	assert_eq!(keys(&db, QueueItemStateKey::partition_scan(queue, 0)).len(), 2, "one state record per item");
	assert_eq!(keys(&db, QueueDueKey::partition_scan(queue, 0)).len(), 2, "one due entry per item");
	assert_eq!(depth(&db, queue, 0), 2, "depth must not be inflated by the boot pass");

	db.stop();
}
