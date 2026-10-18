// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "column")]

use std::{path::Path, sync::Arc};

use reifydb::{
	embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_core::interface::catalog::column_block::ColumnBlock;
use reifydb_runtime::io::fs::{Open, memory::MemoryFs, testing::NoFaults as FsNoFaults};
use reifydb_store_column::{block::BlockData, device::BlockFileKey, store::ColumnStore, testing::NoFaults};
use reifydb_sub_store::subsystem::{StorageConfig, StorageSubsystem};
use reifydb_test_harness::fixture::column::{MEMORY_ROOT, block_path, memory_store};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{duration::Duration, identity::IdentityId};

fn block_count(db: &TestDb, name: &str) -> usize {
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut txn = engine.begin_query(IdentityId::system()).expect("begin query");
	let mut tx = Transaction::Query(&mut txn);
	let namespace = catalog.find_namespace_by_name(&mut tx, "test").expect("find namespace").expect("namespace");
	let table = catalog.find_table_by_name(&mut tx, namespace.id(), name).expect("find table").expect("table");
	catalog.list_column_blocks_for_table(&mut tx, table.id).expect("list table blocks").len()
}

fn table_block_data(db: &TestDb, store: &ColumnStore, name: &str) -> Vec<Arc<BlockData>> {
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut txn = engine.begin_query(IdentityId::system()).expect("begin query");
	let mut tx = Transaction::Query(&mut txn);
	let namespace = catalog.find_namespace_by_name(&mut tx, "test").expect("find namespace").expect("namespace");
	let table = catalog.find_table_by_name(&mut tx, namespace.id(), name).expect("find table").expect("table");
	catalog.list_column_blocks_for_table(&mut tx, table.id)
		.expect("list table blocks")
		.into_iter()
		.map(|block| {
			Arc::new(
				store.open(&BlockFileKey::of(&block))
					.expect("open block")
					.expect("a cataloged block must have a block file")
					.read(None)
					.expect("read block"),
			)
		})
		.collect()
}

#[test]
fn an_unchanged_table_is_not_rematerialized_again_on_every_tick() {
	// The actor's own block commit moves the version, so without a per-table check blocks grow without bound.
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
		|| table_block_data(&db, &store, "t").into_iter().find(|b| b.len() == 1),
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.expect("a 1-row block did not materialize within 5 seconds");
	let settled = block_count(&db, "t");

	let _ = poll_until(|| None::<()>, Duration::from_milliseconds(500).unwrap().to_std());

	assert_eq!(block_count(&db, "t"), settled, "ten idle ticks must not add blocks of an unchanged table");
	db.stop();
}

fn table_blocks(db: &TestDb, name: &str) -> Vec<ColumnBlock> {
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut txn = engine.begin_query(IdentityId::system()).expect("begin query");
	let mut tx = Transaction::Query(&mut txn);
	let namespace = catalog.find_namespace_by_name(&mut tx, "test").expect("find namespace").expect("namespace");
	let table = catalog.find_table_by_name(&mut tx, namespace.id(), name).expect("find table").expect("table");
	catalog.list_column_blocks_for_table(&mut tx, table.id).expect("list table blocks")
}

#[test]
fn a_new_table_block_drops_the_previous_row_and_its_file() {
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
		|| table_blocks(&db, "t").into_iter().find(|block| block.row_count == 1),
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.expect("a 1-row block did not materialize within 5 seconds");
	let first_path = block_path(Path::new(MEMORY_ROOT), &BlockFileKey::of(&first));
	assert!(memory.open(&first_path).is_ok(), "the first block must have its file");

	db.command("INSERT test::t [{id: 2}]");
	let second = poll_until(
		|| table_blocks(&db, "t").into_iter().find(|block| block.row_count == 2),
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.expect("a 2-row block did not materialize within 5 seconds");

	let blocks = table_blocks(&db, "t");
	assert_eq!(blocks.iter().map(|s| s.id).collect::<Vec<_>>(), vec![second.id], "only the latest row may stay");
	poll_until(|| memory.open(&first_path).is_err().then_some(()), Duration::from_seconds(5).unwrap().to_std())
		.expect("the previous block's file must be deleted after the commit");
	let data = store
		.open(&BlockFileKey::of(&second))
		.expect("open latest block")
		.expect("the latest block must keep its file")
		.read(None)
		.expect("read latest block");
	assert_eq!(data.len(), 2, "the latest block must hold both rows");
	db.stop();
}
