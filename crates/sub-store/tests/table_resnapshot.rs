// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "column")]

use reifydb::{
	WithSubsystem, embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_sub_store::{
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};
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

#[test]
fn an_unchanged_table_is_not_snapshotted_again_on_every_tick() {
	// The actor's own snapshot commit moves the version, so without a per-table check blocks grow without bound.
	let config = StorageConfig {
		table_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_tick_interval: Duration::from_milliseconds(50).unwrap(),
		..StorageConfig::default()
	};
	let mut db = TestDb::from(
		db_embedded::memory()
			.with_subsystem(Box::new(StorageSubsystemFactory::new(config)))
			.build()
			.expect("build"),
	);
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::t { id: int4 }");
	db.command("INSERT test::t [{id: 1}]");

	let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
	let store = storage.block_store().clone();
	poll_until(
		|| store.entries().into_iter().find(|(_, b)| b.len() == 1),
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.expect("a 1-row block did not materialize within 5 seconds");
	let settled = snapshot_count(&db, "t");

	let _ = poll_until(|| None::<()>, Duration::from_milliseconds(500).unwrap().to_std());

	assert_eq!(snapshot_count(&db, "t"), settled, "ten idle ticks must not add snapshots of an unchanged table");
	db.stop();
}
