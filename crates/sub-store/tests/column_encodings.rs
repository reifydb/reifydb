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

const ROWS: usize = 3;

const INSERT: &str = "INSERT test::t [{id: 1, tag: \"same\", note: none},\
	 {id: 2, tag: \"same\", note: none},\
	 {id: 3, tag: \"same\", note: none}]";

fn encoding_of(block: &ColumnBlock, name: &str) -> String {
	let (_, chunks) = block.column_by_name(name).unwrap_or_else(|| panic!("column {name} missing from block"));
	assert_eq!(
		chunks.chunks.len(),
		1,
		"{name} materialized as {} chunks, so a single-chunk encoding assertion would not describe the whole column",
		chunks.chunks.len()
	);
	chunks.chunks[0].encoding_id().to_string()
}

fn assert_encodings(block: &ColumnBlock, stage: &str) -> Vec<String> {
	let tag = encoding_of(block, "tag");
	let note = encoding_of(block, "note");
	let id = encoding_of(block, "id");
	assert_eq!(tag, "vortex.constant", "{stage}: a column holding one repeated value must compress to constant");
	assert_eq!(note, "vortex.constant", "{stage}: a column holding only none must compress to a constant none");
	assert_ne!(id, "vortex.constant", "{stage}: a varying column must not be encoded blindly as constant");
	vec![tag, note, id]
}

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
fn constant_and_all_none_columns_keep_their_encoding_across_a_restart() {
	// A stage that silently canonicalizes still returns the right values, so every stage asserts its encodings.
	let column_dir = tempfile::tempdir().expect("create column dir");

	let (snapshot, compressed) = {
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
		db.admin("CREATE TABLE test::t { id: int4, tag: utf8, note: Option(utf8) }");
		db.command(INSERT);

		let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
		let block_store = storage.block_store().clone();

		let (snapshot, block) = poll_until(
			|| table_blocks(&db, &block_store, "t").into_iter().find(|(_, b)| b.len() == ROWS),
			Duration::from_seconds(5).unwrap().to_std(),
		)
		.expect("a 3-row block did not materialize within 5 seconds");

		let compressed = assert_encodings(&block, "after compression");

		db.stop();
		(snapshot, compressed)
	};

	// A fresh tier can only answer from disk, so equal encodings prove persist wrote the encoded tree.
	let reloaded = ColumnStore::host(column_dir.path().to_path_buf()).expect("reopen column dir");
	let block = Arc::new(
		reloaded.open(&BlockKey::of(&snapshot))
			.expect("open block")
			.expect("reloaded block store must contain the 3-row block from disk")
			.read(None)
			.expect("read block"),
	);

	let reloaded_encodings = assert_encodings(&block, "after reload");
	assert_eq!(reloaded_encodings, compressed, "persist must not re-encode or canonicalize any column");

	let mut reader = SnapshotReader::new(block, 100, reloaded.session().clone());
	let batch = reader.next().expect("batch present").expect("read batch");
	assert_eq!(batch.num_rows(), ROWS);

	let ids = column_view(&batch, "id").expect("id view").expect("id column");
	let tags = column_view(&batch, "tag").expect("tag view").expect("tag column");
	let notes = column_view(&batch, "note").expect("note view").expect("note column");

	let mut seen: Vec<i32> = Vec::new();
	for i in 0..ROWS {
		match ids.get_value(i) {
			Value::Int4(v) => seen.push(v),
			other => panic!("row {i}: id expected Int4, got {other:?}"),
		}
		match tags.get_value(i) {
			Value::Utf8(s) => assert_eq!(s, "same", "row {i}: constant column decoded to the wrong value"),
			other => panic!("row {i}: tag expected Utf8, got {other:?}"),
		}
		assert!(
			matches!(notes.get_value(i), Value::None { .. }),
			"row {i}: an all-none column must decode back to none, never to a default value"
		);
	}
	seen.sort();
	assert_eq!(seen, vec![1, 2, 3], "the varying column must survive the round trip unchanged");
}
