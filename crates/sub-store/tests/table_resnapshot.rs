// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "column")]

use std::{path::Path, sync::Arc};

use reifydb::{
	embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_core::interface::catalog::column_snapshot::ColumnSnapshot;
use reifydb_runtime::io::fs::{Open, memory::MemoryFs, testing::NoFaults as FsNoFaults};
use reifydb_store_column::{device::BlockKey, snapshot::ColumnBlock, store::ColumnStore, testing::NoFaults};
use reifydb_sub_store::subsystem::{StorageConfig, StorageSubsystem};
use reifydb_test_harness::fixture::column::{MEMORY_ROOT, block_path, memory_store};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{duration::Duration, identity::IdentityId};

fn snapshot_count(db: &TestDb, name: &str) -> usize {
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut txn = engine.begin_query(IdentityId::system()).expect("begin query");
	let mut tx = Transaction::Query(&mut txn);
	let namespace = catalog.find_namespace_by_name(&mut tx, "test").expect("find namespace").expect("namespace");
	let table = catalog.find_table_by_name(&mut tx, namespace.id(), name).expect("find table").expect("table");
	catalog.list_column_snapshots_for_table(&mut tx, table.id).expect("list table snapshots").len()
}

fn table_blocks(db: &TestDb, store: &ColumnStore, name: &str) -> Vec<Arc<ColumnBlock>> {
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut txn = engine.begin_query(IdentityId::system()).expect("begin query");
	let mut tx = Transaction::Query(&mut txn);
	let namespace = catalog.find_namespace_by_name(&mut tx, "test").expect("find namespace").expect("namespace");
	let table = catalog.find_table_by_name(&mut tx, namespace.id(), name).expect("find table").expect("table");
	catalog.list_column_snapshots_for_table(&mut tx, table.id)
		.expect("list table snapshots")
		.into_iter()
		.map(|snapshot| {
			Arc::new(
				store.open(&BlockKey::of(&snapshot))
					.expect("open block")
					.expect("a cataloged snapshot must have a block file")
					.read(None)
					.expect("read block"),
			)
		})
		.collect()
}

#[test]
fn an_unchanged_table_is_not_snapshotted_again_on_every_tick() {
	// The actor's own snapshot commit moves the version, so without a per-table check blocks grow without bound.
	let config = StorageConfig {
		table_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_tick_interval: Duration::from_milliseconds(50).unwrap(),
		..StorageConfig::default()
	};
	let mut db = TestDb::from(db_embedded::memory().with_storage_config(config).build().expect("build"));
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::t { id: int4 }");
	db.command("INSERT test::t [{id: 1}]");

	let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
	let store = storage.block_store().clone();
	poll_until(
		|| table_blocks(&db, &store, "t").into_iter().find(|b| b.len() == 1),
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.expect("a 1-row block did not materialize within 5 seconds");
	let settled = snapshot_count(&db, "t");

	let _ = poll_until(|| None::<()>, Duration::from_milliseconds(500).unwrap().to_std());

	assert_eq!(snapshot_count(&db, "t"), settled, "ten idle ticks must not add snapshots of an unchanged table");
	db.stop();
}

fn table_snapshots(db: &TestDb, name: &str) -> Vec<ColumnSnapshot> {
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut txn = engine.begin_query(IdentityId::system()).expect("begin query");
	let mut tx = Transaction::Query(&mut txn);
	let namespace = catalog.find_namespace_by_name(&mut tx, "test").expect("find namespace").expect("namespace");
	let table = catalog.find_table_by_name(&mut tx, namespace.id(), name).expect("find table").expect("table");
	catalog.list_column_snapshots_for_table(&mut tx, table.id).expect("list table snapshots")
}

#[test]
fn a_new_table_snapshot_drops_the_previous_row_and_its_file() {
	// Otherwise every table change leaves a stale catalog row and block file behind forever.
	let config = StorageConfig {
		table_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_tick_interval: Duration::from_milliseconds(50).unwrap(),
		..StorageConfig::default()
	};
	let memory = MemoryFs::new();
	let store = memory_store(memory.clone(), Arc::new(FsNoFaults), Arc::new(NoFaults)).expect("store");
	let mut db = TestDb::from(
		db_embedded::memory()
			.with_storage_config(config)
			.with_column_store(store.clone())
			.build()
			.expect("build"),
	);
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::t { id: int4 }");
	db.command("INSERT test::t [{id: 1}]");
	let first = poll_until(
		|| table_snapshots(&db, "t").into_iter().find(|snapshot| snapshot.row_count == 1),
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.expect("a 1-row snapshot did not materialize within 5 seconds");
	let first_path = block_path(Path::new(MEMORY_ROOT), &BlockKey::of(&first));
	assert!(memory.open(&first_path).is_ok(), "the first snapshot must have its file");

	db.command("INSERT test::t [{id: 2}]");
	let second = poll_until(
		|| table_snapshots(&db, "t").into_iter().find(|snapshot| snapshot.row_count == 2),
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.expect("a 2-row snapshot did not materialize within 5 seconds");

	let snapshots = table_snapshots(&db, "t");
	assert_eq!(snapshots.iter().map(|s| s.id).collect::<Vec<_>>(), vec![second.id], "only the latest row may stay");
	poll_until(|| memory.open(&first_path).is_err().then_some(()), Duration::from_seconds(5).unwrap().to_std())
		.expect("the previous snapshot's file must be deleted after the commit");
	let block = store
		.open(&BlockKey::of(&second))
		.expect("open latest block")
		.expect("the latest snapshot must keep its file")
		.read(None)
		.expect("read latest block");
	assert_eq!(block.len(), 2, "the latest block must hold both rows");
	db.stop();
}
