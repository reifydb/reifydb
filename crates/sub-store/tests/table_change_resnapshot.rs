// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "column")]

use std::collections::BTreeMap;

use reifydb::{
	WithSubsystem, embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_column::reader::SnapshotReader;
use reifydb_store_column::ColumnStore;
use reifydb_sub_store::{
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, duration::Duration, identity::IdentityId};

fn db() -> TestDb {
	let config = StorageConfig {
		table_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_tick_interval: Duration::from_milliseconds(50).unwrap(),
		..StorageConfig::default()
	};
	let db = TestDb::from(
		db_embedded::memory()
			.with_subsystem(Box::new(StorageSubsystemFactory::new(config)))
			.build()
			.expect("build"),
	);
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::t { id: int4, v: int4 }");
	db
}

fn block_store(db: &TestDb) -> ColumnStore {
	db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered").block_store()
}

fn latest_rows(db: &TestDb, store: &ColumnStore) -> Option<BTreeMap<i32, i32>> {
	// Entries are read before the catalog so a block put after its snapshot commit is never missed.
	let entries = store.entries();
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut txn = engine.begin_query(IdentityId::system()).expect("begin query");
	let mut tx = Transaction::Query(&mut txn);
	let namespace = catalog.find_namespace_by_name(&mut tx, "test").expect("find namespace").expect("namespace");
	let table = catalog.find_table_by_name(&mut tx, namespace.id(), "t").expect("find table").expect("table");
	let latest = catalog
		.list_column_snapshots_for_table(&mut tx, table.id)
		.expect("list table snapshots")
		.into_iter()
		.map(|snapshot| snapshot.id)
		.max()?;
	let block = entries.into_iter().find(|(id, _)| *id == latest).map(|(_, block)| block)?;

	let mut rows = BTreeMap::new();
	if block.is_empty() {
		return Some(rows);
	}
	let mut reader = SnapshotReader::new(block, 100);
	let batch = reader.next().expect("batch present").expect("read batch");
	for row in 0..batch.row_count() {
		let id = match batch.column("id").expect("id column").data().get_value(row) {
			Value::Int4(v) => v,
			other => panic!("row {row}: expected Int4 id, got {other:?}"),
		};
		let v = match batch.column("v").expect("v column").data().get_value(row) {
			Value::Int4(v) => v,
			other => panic!("row {row}: expected Int4 v, got {other:?}"),
		};
		rows.insert(id, v);
	}
	Some(rows)
}

fn await_latest(db: &TestDb, store: &ColumnStore, expected: &[(i32, i32)], what: &str) {
	let expected: BTreeMap<i32, i32> = expected.iter().copied().collect();
	let seen = poll_until(
		|| latest_rows(db, store).filter(|rows| *rows == expected),
		Duration::from_seconds(5).unwrap().to_std(),
	);
	assert!(
		seen.is_some(),
		"{what}: the latest snapshot of test::t never held {expected:?}; last seen {:?}",
		latest_rows(db, store)
	);
}

#[test]
fn an_update_only_change_is_snapshotted_again() {
	// Without updates counting as a change, the latest block keeps the old value while the row count stays equal.
	let mut db = db();
	db.command("INSERT test::t [{id: 1, v: 1}, {id: 2, v: 2}]");
	let store = block_store(&db);
	await_latest(&db, &store, &[(1, 1), (2, 2)], "after the insert");

	db.command("UPDATE test::t { v: 10 } FILTER { id == 1 }");

	await_latest(&db, &store, &[(1, 10), (2, 2)], "after an update-only change");
	db.stop();
}

#[test]
fn a_delete_only_change_is_snapshotted_again() {
	// Without removals counting as a change, a deleted row lives on in the latest block.
	let mut db = db();
	db.command("INSERT test::t [{id: 1, v: 1}, {id: 2, v: 2}]");
	let store = block_store(&db);
	await_latest(&db, &store, &[(1, 1), (2, 2)], "after the insert");

	db.command("DELETE test::t FILTER { id == 1 }");

	await_latest(&db, &store, &[(2, 2)], "after a delete-only change");
	db.stop();
}

#[test]
fn an_insert_after_the_first_snapshot_is_snapshotted_again() {
	// Without later commits raising the recorded change version, rows added after the first snapshot never land.
	let mut db = db();
	db.command("INSERT test::t [{id: 1, v: 1}]");
	let store = block_store(&db);
	await_latest(&db, &store, &[(1, 1)], "after the first insert");

	db.command("INSERT test::t [{id: 2, v: 2}]");
	await_latest(&db, &store, &[(1, 1), (2, 2)], "after the second insert");

	db.command("INSERT test::t [{id: 3, v: 3}]");
	await_latest(&db, &store, &[(1, 1), (2, 2), (3, 3)], "after the third insert");
	db.stop();
}
