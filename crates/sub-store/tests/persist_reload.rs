// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]

use std::sync::Arc;

use reifydb::testing::db::{TestDb, poll_until};
use reifydb::{WithSubsystem, embedded as db_embedded};
use reifydb_column::reader::SnapshotReader;
use reifydb_sqlite::SqliteConfig;
use reifydb_sub_store::{
	column::{block_store::ColumnBlockStore, persistent::sqlite::SqliteColumnStore},
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};
use reifydb_value::value::{Value, duration::Duration};

#[test]
fn materialized_columns_persist_to_disk_and_reload_after_restart() {
	// A shared column.db surviving two independent opens is what simulates a process restart: the
	// first database writes blocks, a fresh tier reads them back.
	let (column_cfg, _guard) = SqliteConfig::in_memory();

	{
		let storage_config = StorageConfig {
			table_tick_interval: Duration::from_milliseconds(50).unwrap(),
			series_tick_interval: Duration::from_milliseconds(50).unwrap(),
			..StorageConfig::default()
		};
		let factory = StorageSubsystemFactory::new(storage_config).with_column_sqlite(Some(column_cfg.clone()));

		let mut db =
			TestDb::from(db_embedded::memory().with_subsystem(Box::new(factory)).build().expect("build"));

		db.admin("CREATE NAMESPACE test");
		db.admin("CREATE TABLE test::t { id: int4, name: utf8 }");
		db.command(
			"INSERT test::t [{id: 1, name: \"alpha\"}, {id: 2, name: \"bravo\"}, {id: 3, name: \"charlie\"}]",
		);

		let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
		let block_store = storage.block_store().clone();

		// The actor persists before the catalog commit and puts into the cache last, so a block
		// visible in the cache is already durable.
		poll_until(
			|| block_store.entries().into_iter().map(|(_, b)| b).find(|b| b.len() == 3),
			Duration::from_seconds(5).unwrap().to_std(),
		)
		.expect("a 3-row block did not materialize within 5 seconds");

		db.stop();
	}

	// The reload runs with no database and no re-materialization, so only disk state can satisfy it.
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
