// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(all(feature = "column", reifydb_target = "host"))]

use std::sync::Arc;

use reifydb::{
	embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_core::interface::catalog::column_snapshot::ColumnSnapshot;
use reifydb_store_column::{device::BlockKey, reader::SnapshotReader, snapshot::ColumnBlock, store::ColumnStore};
use reifydb_sub_store::subsystem::{StorageConfig, StorageSubsystem};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{Value, duration::Duration, identity::IdentityId, system_columns::column_view};

fn table_blocks(db: &TestDb, store: &ColumnStore, name: &str) -> Vec<(ColumnSnapshot, Arc<ColumnBlock>)> {
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
			let block = Arc::new(
				store.open(&BlockKey::of(&snapshot))
					.expect("open block")
					.expect("a cataloged snapshot must have a block file")
					.read(None)
					.expect("read block"),
			);
			(snapshot, block)
		})
		.collect()
}

#[test]
fn materialized_columns_persist_to_disk_and_reload_after_restart() {
	// Two opens of one column dir simulate a restart: the first writes blocks, a fresh store reads them back.
	let column_dir = tempfile::tempdir().expect("create column dir");

	let snapshot = {
		let storage_config = StorageConfig {
			table_tick_interval: Duration::from_milliseconds(50).unwrap(),
			series_tick_interval: Duration::from_milliseconds(50).unwrap(),
			..StorageConfig::default()
		};
		let store = ColumnStore::host(column_dir.path().to_path_buf()).expect("open column dir");

		let mut db = TestDb::from(
			db_embedded::memory()
				.with_storage_config(storage_config)
				.with_column_store(store)
				.build()
				.expect("build"),
		);

		db.admin("CREATE NAMESPACE test");
		db.admin("CREATE TABLE test::t { id: int4, name: utf8 }");
		db.command(
			"INSERT test::t [{id: 1, name: \"alpha\"}, {id: 2, name: \"bravo\"}, {id: 3, name: \"charlie\"}]",
		);

		let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
		let block_store = storage.block_store().clone();

		let (snapshot, _) = poll_until(
			|| table_blocks(&db, &block_store, "t").into_iter().find(|(_, b)| b.len() == 3),
			Duration::from_seconds(5).unwrap().to_std(),
		)
		.expect("a 3-row block did not materialize within 5 seconds");

		db.stop();
		snapshot
	};

	// The reload runs with no database and no re-materialization, so only disk state can satisfy it.
	let reloaded = ColumnStore::host(column_dir.path().to_path_buf()).expect("reopen column dir");
	let block = Arc::new(
		reloaded.open(&BlockKey::of(&snapshot))
			.expect("open block")
			.expect("reloaded block store must contain the 3-row block from disk")
			.read(None)
			.expect("read block"),
	);

	let mut reader = SnapshotReader::new(block, 100, reloaded.session().clone());
	let batch = reader.next().expect("batch present").expect("read batch");
	assert_eq!(batch.num_rows(), 3);

	let id_col = column_view(&batch, "id").expect("id view").expect("id column");
	let name_col = column_view(&batch, "name").expect("name view").expect("name column");
	let mut rows: Vec<(i32, String)> = Vec::new();
	for i in 0..3 {
		let id = match id_col.get_value(i) {
			Value::Int4(v) => v,
			other => panic!("row {i}: expected Int4, got {other:?}"),
		};
		let name = match name_col.get_value(i) {
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
