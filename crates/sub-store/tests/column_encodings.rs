// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]

use std::sync::Arc;

use reifydb::{
	WithSubsystem, embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_store_column::{
	persistent::sqlite::SqliteColumnStore, reader::SnapshotReader, snapshot::ColumnBlock, store::ColumnStore,
};
use reifydb_sub_store::{
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};
use reifydb_value::value::{Value, duration::Duration, system_columns::column_view};

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

#[test]
fn constant_and_all_none_columns_keep_their_encoding_across_a_restart() {
	// A stage that silently canonicalizes still returns the right values, so every stage asserts its encodings.
	let (column_cfg, _guard) = SqliteConfig::in_memory();

	let compressed = {
		let storage_config = StorageConfig {
			table_tick_interval: Duration::from_milliseconds(50).unwrap(),
			series_tick_interval: Duration::from_milliseconds(50).unwrap(),
			..StorageConfig::default()
		};
		let factory = StorageSubsystemFactory::new(storage_config).with_column_sqlite(Some(column_cfg.clone()));
		let mut db =
			TestDb::from(db_embedded::memory().with_subsystem(Box::new(factory)).build().expect("build"));

		db.admin("CREATE NAMESPACE test");
		db.admin("CREATE TABLE test::t { id: int4, tag: utf8, note: Option(utf8) }");
		db.command(INSERT);

		let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
		let block_store = storage.block_store().clone();

		let block = poll_until(
			|| block_store.entries().into_iter().map(|(_, b)| b).find(|b| b.len() == ROWS),
			Duration::from_seconds(5).unwrap().to_std(),
		)
		.expect("a 3-row block did not materialize within 5 seconds");

		let compressed = assert_encodings(&block, "after compression");

		db.stop();
		compressed
	};

	// A fresh tier can only answer from disk, so equal encodings prove persist wrote the encoded tree.
	let tier = Arc::new(SqliteColumnStore::new(column_cfg));
	let reloaded = ColumnStore::with_persistent(Some(tier));
	reloaded.warm().expect("warm from column.db");

	let block = reloaded
		.entries()
		.into_iter()
		.map(|(_, b)| b)
		.find(|b| b.len() == ROWS)
		.expect("reloaded block store must contain the 3-row block from disk");

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
