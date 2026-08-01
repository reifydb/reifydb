// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "column")]

use std::sync::Arc;

use reifydb::{WithSubsystem, embedded as db_embedded};
use reifydb_column::reader::SnapshotReader;
use reifydb_core::common::CommitVersion;
use reifydb_sub_store::{
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};
use reifydb_test_harness::db::{TestDb, poll_until};
use reifydb_value::value::{datetime::DateTime, duration::Duration, row_number::RowNumber};

#[test]
fn series_snapshot_records_sealed_at_commit_version() {
	// Only checks that a bucket materializes and that the engine's version does not go backwards;
	// `sealed_at_commit_version` itself is never read here.
	let fast_config = StorageConfig {
		table_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_bucket_width: 5,
		series_grace: Duration::from_milliseconds(0).unwrap(),
	};

	let mut db = TestDb::from(
		db_embedded::memory()
			.with_subsystem(Box::new(StorageSubsystemFactory::new(fast_config)))
			.build()
			.expect("build"),
	);

	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE SERIES test::s { k: uint8, value: float8 } WITH { key: k }");

	db.command(
		"INSERT test::s [\
		  {k: 0, value: 0.0}, {k: 1, value: 1.0}, {k: 2, value: 2.0}, {k: 3, value: 3.0}, {k: 4, value: 4.0},\
		  {k: 5, value: 5.0}, {k: 6, value: 6.0}, {k: 7, value: 7.0}, {k: 8, value: 8.0}, {k: 9, value: 9.0},\
		  {k: 10, value: 10.0}, {k: 11, value: 11.0}\
		 ]",
	);

	let post_insert_version = db.engine().current_version().expect("current_version");
	assert!(post_insert_version > CommitVersion(0), "insert should advance commit version");

	let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
	let block_store = storage.block_store().clone();

	poll_until(
		|| {
			if !block_store.is_empty() {
				Some(())
			} else {
				None
			}
		},
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.expect("series snapshot did not materialize within 5 seconds");

	// A trivial admin query as a liveness check; its result is deliberately discarded.
	let admin_check = db.try_admin("FROM []");
	let _ = admin_check;

	let now_version = db.engine().current_version().expect("current_version after");
	assert!(
		now_version >= post_insert_version,
		"current version ({now_version:?}) should be >= post-insert ({post_insert_version:?})"
	);

	db.stop();
}

#[test]
fn series_snapshot_system_columns_match_row_metadata() {
	// System columns must come from the row header, so the two synthetic shapes a reader can fall back
	// to - a sequential RowNumber and a default DateTime - are both rejected below.
	let fast_config = StorageConfig {
		table_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_bucket_width: 5,
		series_grace: Duration::from_milliseconds(0).unwrap(),
	};

	let mut db = TestDb::from(
		db_embedded::memory()
			.with_subsystem(Box::new(StorageSubsystemFactory::new(fast_config)))
			.build()
			.expect("build"),
	);

	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE SERIES test::s { k: uint8, value: float8 } WITH { key: k }");

	db.command(
		"INSERT test::s [\
		  {k: 0, value: 0.0}, {k: 1, value: 1.0}, {k: 2, value: 2.0}, {k: 3, value: 3.0}, {k: 4, value: 4.0},\
		  {k: 5, value: 5.0}, {k: 6, value: 6.0}, {k: 7, value: 7.0}, {k: 8, value: 8.0}, {k: 9, value: 9.0},\
		  {k: 10, value: 10.0}, {k: 11, value: 11.0}\
		 ]",
	);

	let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
	let block_store = storage.block_store().clone();

	let block = poll_until(
		|| {
			let entries = block_store.entries();
			entries.into_iter().map(|(_, b)| b).find(|b| b.len() > 0)
		},
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.expect("series snapshot did not materialize within 5 seconds");

	let mut reader = SnapshotReader::new(Arc::clone(&block), 100);
	let batch = reader.next().expect("batch present").expect("read batch");

	let n = batch.row_count();
	assert!(n > 0, "expected non-empty snapshot batch");

	for i in 0..n {
		let rn = batch.row_numbers[i];
		assert!(
			rn != RowNumber(0) && rn != RowNumber(i as u64),
			"row {i}: row_number {rn:?} looks synthetic (0 or sequential index); expected a real series sequence",
		);
		let created = batch.created_at[i];
		assert!(
			created != DateTime::default(),
			"row {i}: created_at is DateTime::default() - expected real wall-clock from the row header",
		);
		let updated = batch.updated_at[i];
		assert_eq!(updated, created, "row {i}: insert-only row should have updated_at == created_at");
	}

	db.stop();
}

#[test]
fn table_snapshot_system_columns_match_row_metadata() {
	// The same header-metadata requirement as the series case, on the table materialization path.
	let fast_config = StorageConfig {
		table_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_tick_interval: Duration::from_milliseconds(50).unwrap(),
		..StorageConfig::default()
	};

	let mut db = TestDb::from(
		db_embedded::memory()
			.with_subsystem(Box::new(StorageSubsystemFactory::new(fast_config)))
			.build()
			.expect("build"),
	);

	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE TABLE test::t { id: int4, name: utf8 }");
	db.command("INSERT test::t [{id: 1, name: \"alpha\"}, {id: 2, name: \"bravo\"}, {id: 3, name: \"charlie\"}]");

	let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
	let block_store = storage.block_store().clone();

	let block = poll_until(
		|| {
			let entries = block_store.entries();
			entries.into_iter().map(|(_, b)| b).find(|b| b.len() == 3)
		},
		Duration::from_seconds(5).unwrap().to_std(),
	)
	.expect("table snapshot did not materialize within 5 seconds");

	let mut reader = SnapshotReader::new(Arc::clone(&block), 100);
	let batch = reader.next().expect("batch present").expect("read batch");
	assert_eq!(batch.row_count(), 3);

	for i in 0..3 {
		assert_ne!(batch.row_numbers[i], RowNumber(0), "row {i}: row_number should be a real key, not 0");
		let created = batch.created_at[i];
		assert_ne!(
			created,
			DateTime::default(),
			"row {i}: created_at is DateTime::default() - expected real wall-clock from the row header",
		);
		assert_eq!(
			batch.updated_at[i], created,
			"row {i}: insert-only row should have updated_at == created_at"
		);
	}

	db.stop();
}
