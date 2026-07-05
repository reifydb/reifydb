// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]

use std::{sync::Arc, time::Duration};

use reifydb::{Params, WithSubsystem, embedded as db_embedded};
use reifydb_column::reader::SnapshotReader;
use reifydb_sqlite::SqliteConfig;
use reifydb_sub_store::{
	column::{block_store::ColumnBlockStore, persistent::sqlite::SqliteColumnStore},
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};
use reifydb_value::value::Value;

mod common;
use common::poll_until;

// A shared /dev/shm-backed column.db survives across two independent opens, which lets us
// simulate a process restart: the first database writes blocks, a fresh tier reads them back.
#[test]
fn materialized_columns_persist_to_disk_and_reload_after_restart() {
	let (column_cfg, _guard) = SqliteConfig::in_memory();

	// Phase 1: a running database materializes a table; the column tier persists each block to disk.
	{
		let storage_config = StorageConfig {
			table_tick_interval: Duration::from_millis(50),
			series_tick_interval: Duration::from_millis(50),
			..StorageConfig::default()
		};
		let factory = StorageSubsystemFactory::new(storage_config).with_column_sqlite(Some(column_cfg.clone()));

		let mut db = db_embedded::memory().with_subsystem(Box::new(factory)).build().expect("build");

		db.admin_as_root("CREATE NAMESPACE test", Params::None).expect("create namespace");
		db.admin_as_root("CREATE TABLE test::t { id: int4, name: utf8 }", Params::None).expect("create table");
		db.command_as_root(
			"INSERT test::t [{id: 1, name: \"alpha\"}, {id: 2, name: \"bravo\"}, {id: 3, name: \"charlie\"}]",
			Params::None,
		)
		.expect("insert");

		let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
		let block_store = storage.block_store().clone();

		// persist() runs before the catalog commit, which runs before the cache put, so a block
		// visible in the cache is already durable in column.db.
		poll_until(
			|| block_store.entries().into_iter().map(|(_, b)| b).find(|b| b.len() == 3),
			Duration::from_secs(5),
		)
		.expect("a 3-row block did not materialize within 5 seconds");

		db.stop().expect("stop");
	}

	// Phase 2: a fresh tier + block store reload the block straight from column.db, with no
	// running database and no re-materialization.
	let tier = Arc::new(SqliteColumnStore::new(column_cfg));
	let persisted = tier.load_all().expect("load_all");
	assert!(!persisted.is_empty(), "column.db must contain a persisted block after materialization");

	let reloaded = ColumnBlockStore::with_persistent(Some(tier));
	reloaded.warm().expect("warm from column.db");

	let block = reloaded
		.entries()
		.into_iter()
		.map(|(_, b)| b)
		.find(|b| b.len() == 3)
		.expect("reloaded block store must contain the 3-row block from disk");

	let mut reader = SnapshotReader::new(block, 100);
	let batch = reader.next().expect("batch present").expect("read batch");
	assert_eq!(batch.row_count(), 3);

	let id_col = batch.column("id").expect("id column");
	let name_col = batch.column("name").expect("name column");
	let mut rows: Vec<(i32, String)> = Vec::new();
	for i in 0..3 {
		let id = match id_col.data().get_value(i) {
			Value::Int4(v) => v,
			other => panic!("row {i}: expected Int4, got {other:?}"),
		};
		let name = match name_col.data().get_value(i) {
			Value::Utf8(s) => s,
			other => panic!("row {i}: expected Utf8, got {other:?}"),
		};
		rows.push((id, name));
	}
	rows.sort();
	assert_eq!(
		rows,
		vec![(1, "alpha".to_string()), (2, "bravo".to_string()), (3, "charlie".to_string())],
		"values must survive the disk round trip"
	);
}
