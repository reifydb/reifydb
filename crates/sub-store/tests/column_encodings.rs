// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]

use std::sync::Arc;

use reifydb::{
	WithSubsystem, embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_column::{reader::SnapshotReader, snapshot::ColumnBlock};
use reifydb_core::value::column::encoding::EncodingId;
use reifydb_sqlite::SqliteConfig;
use reifydb_store_column::{persistent::sqlite::SqliteColumnStore, store::ColumnStore};
use reifydb_sub_store::{
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};
use reifydb_value::value::{Value, duration::Duration};

const ROWS: usize = 3;

const INSERT: &str = "INSERT test::t [{id: 1, tag: \"same\", note: none},\
	 {id: 2, tag: \"same\", note: none},\
	 {id: 3, tag: \"same\", note: none}]";

fn encoding_of(block: &ColumnBlock, name: &str) -> EncodingId {
	let (_, chunks) = block.column_by_name(name).unwrap_or_else(|| panic!("column {name} missing from block"));
	assert_eq!(
		chunks.chunks.len(),
		1,
		"{name} materialized as {} chunks, so a single-chunk encoding assertion would not describe the whole column",
		chunks.chunks.len()
	);
	chunks.chunks[0].encoding()
}

fn assert_encodings(block: &ColumnBlock, stage: &str) {
	assert_eq!(
		encoding_of(block, "tag"),
		EncodingId::CONSTANT,
		"{stage}: a column holding one repeated value must be CONSTANT"
	);
	assert_eq!(
		encoding_of(block, "note"),
		EncodingId::ALL_NONE,
		"{stage}: a column holding only none must be ALL_NONE"
	);
	assert_eq!(
		encoding_of(block, "id"),
		EncodingId::CANONICAL_FIXED,
		"{stage}: a varying column must stay canonical, otherwise the compressor is encoding blindly"
	);
}

#[test]
fn constant_and_all_none_columns_keep_their_encoding_across_a_restart() {
	// The loop under test is compress -> persist -> load -> read, and every stage is asserted
	// separately: a stage that silently canonicalizes still returns the right values, so a
	// values-only test would pass while the column store saved nothing.
	// tag repeats one value so CONSTANT fires, note is entirely none so ALL_NONE fires, and id
	// varies so it must stay canonical - without id the test could not tell compression from
	// blanket encoding.
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
		db.admin("CREATE TABLE test::t { id: int4, tag: utf8, note: Option(utf8) }");
		db.command(INSERT);

		let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
		let block_store = storage.block_store().clone();

		let block = poll_until(
			|| block_store.entries().into_iter().map(|(_, b)| b).find(|b| b.len() == ROWS),
			Duration::from_seconds(5).unwrap().to_std(),
		)
		.expect("a 3-row block did not materialize within 5 seconds");

		assert_encodings(&block, "after compression");

		db.stop();
	}

	// Nothing re-materializes below: a fresh tier over the same file can only answer from disk,
	// so surviving encodings prove persist wrote the arm rather than canonicalizing on the way out.
	let tier = Arc::new(SqliteColumnStore::new(column_cfg));
	let reloaded = ColumnStore::with_persistent(Some(tier));
	reloaded.warm().expect("warm from column.db");

	let block = reloaded
		.entries()
		.into_iter()
		.map(|(_, b)| b)
		.find(|b| b.len() == ROWS)
		.expect("reloaded block store must contain the 3-row block from disk");

	assert_encodings(&block, "after reload");

	let mut reader = SnapshotReader::new(block, 100);
	let batch = reader.next().expect("batch present").expect("read batch");
	assert_eq!(batch.row_count(), ROWS);

	let ids = batch.column("id").expect("id column");
	let tags = batch.column("tag").expect("tag column");
	let notes = batch.column("note").expect("note column");

	let mut seen: Vec<i32> = Vec::new();
	for i in 0..ROWS {
		match ids.data().get_value(i) {
			Value::Int4(v) => seen.push(v),
			other => panic!("row {i}: id expected Int4, got {other:?}"),
		}
		match tags.data().get_value(i) {
			Value::Utf8(s) => assert_eq!(s, "same", "row {i}: constant column decoded to the wrong value"),
			other => panic!("row {i}: tag expected Utf8, got {other:?}"),
		}
		assert!(
			matches!(notes.data().get_value(i), Value::None { .. }),
			"row {i}: an all-none column must decode back to none, never to a default value"
		);
	}
	seen.sort();
	assert_eq!(seen, vec![1, 2, 3], "the canonical column must survive the round trip unchanged");
}
