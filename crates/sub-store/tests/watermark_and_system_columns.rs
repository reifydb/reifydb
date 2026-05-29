// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "column")]

use std::{sync::Arc, time::Duration};

use reifydb::{Params, WithSubsystem, embedded as db_embedded};
use reifydb_column::{reader::SnapshotReader, snapshot::SnapshotSource};
use reifydb_core::common::CommitVersion;
use reifydb_sub_store::{
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};
use reifydb_type::value::{datetime::DateTime, row_number::RowNumber};

mod common;
use common::poll_until;

// Closing a series bucket should record the materializer's read version as
// `sealed_at_commit_version`. This is the watermark a delta scan filters
// against in plan-3, so it must be both populated and bounded above by the
// engine's current commit version at assertion time.
#[test]
fn series_snapshot_records_sealed_at_commit_version() {
	let fast_config = StorageConfig {
		table_tick_interval: Duration::from_millis(50),
		series_tick_interval: Duration::from_millis(50),
		series_bucket_width: 5,
		series_grace: Duration::from_millis(0),
	};

	let mut db = db_embedded::memory()
		.with_subsystem(Box::new(StorageSubsystemFactory::new(fast_config)))
		.build()
		.expect("build");
	db.start().expect("start");

	db.admin_as_root("CREATE NAMESPACE test", Params::None).expect("create namespace");
	db.admin_as_root("CREATE SERIES test::s { k: uint8, value: float8 } WITH { key: k }", Params::None)
		.expect("create series");

	db.command_as_root(
		"INSERT test::s [\
		  {k: 0, value: 0.0}, {k: 1, value: 1.0}, {k: 2, value: 2.0}, {k: 3, value: 3.0}, {k: 4, value: 4.0},\
		  {k: 5, value: 5.0}, {k: 6, value: 6.0}, {k: 7, value: 7.0}, {k: 8, value: 8.0}, {k: 9, value: 9.0},\
		  {k: 10, value: 10.0}, {k: 11, value: 11.0}\
		 ]",
		Params::None,
	)
	.expect("insert");

	let post_insert_version = db.engine().current_version().expect("current_version");
	assert!(post_insert_version > CommitVersion(0), "insert should advance commit version");

	let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
	let registry = storage.registry();

	let snap = poll_until(
		|| registry.list().into_iter().find(|s| s.name == "s").and_then(|meta| registry.get(&meta.id)),
		Duration::from_secs(5),
	)
	.expect("series snapshot did not materialize within 5 seconds");

	let sealed = match &snap.source {
		SnapshotSource::Series {
			sealed_at_commit_version,
			..
		} => *sealed_at_commit_version,
		other => panic!("expected SnapshotSource::Series, got {other:?}"),
	};

	assert!(
		sealed >= post_insert_version,
		"sealed_at_commit_version ({sealed:?}) must be at least post-insert version ({post_insert_version:?})"
	);
	let now_version = db.engine().current_version().expect("current_version after");
	assert!(
		sealed <= now_version,
		"sealed_at_commit_version ({sealed:?}) must not exceed current ({now_version:?})"
	);

	db.stop().expect("stop");
}

// System columns on a series snapshot must come from the row's real header
// metadata, not synthetic placeholders. Pre-plan-1, the reader synthesized
// `RowNumber(i)` and `DateTime::default()` (nanos=0) - both pinned in the
// assertions below to make a regression obvious.
#[test]
fn series_snapshot_system_columns_match_row_metadata() {
	let fast_config = StorageConfig {
		table_tick_interval: Duration::from_millis(50),
		series_tick_interval: Duration::from_millis(50),
		series_bucket_width: 5,
		series_grace: Duration::from_millis(0),
	};

	let mut db = db_embedded::memory()
		.with_subsystem(Box::new(StorageSubsystemFactory::new(fast_config)))
		.build()
		.expect("build");
	db.start().expect("start");

	db.admin_as_root("CREATE NAMESPACE test", Params::None).expect("create namespace");
	db.admin_as_root("CREATE SERIES test::s { k: uint8, value: float8 } WITH { key: k }", Params::None)
		.expect("create series");

	db.command_as_root(
		"INSERT test::s [\
		  {k: 0, value: 0.0}, {k: 1, value: 1.0}, {k: 2, value: 2.0}, {k: 3, value: 3.0}, {k: 4, value: 4.0},\
		  {k: 5, value: 5.0}, {k: 6, value: 6.0}, {k: 7, value: 7.0}, {k: 8, value: 8.0}, {k: 9, value: 9.0},\
		  {k: 10, value: 10.0}, {k: 11, value: 11.0}\
		 ]",
		Params::None,
	)
	.expect("insert");

	let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
	let registry = storage.registry();

	let snap = poll_until(
		|| {
			registry.list()
				.into_iter()
				.find(|s| s.name == "s" && s.row_count > 0)
				.and_then(|meta| registry.get(&meta.id))
		},
		Duration::from_secs(5),
	)
	.expect("series snapshot did not materialize within 5 seconds");

	let mut reader = SnapshotReader::new(Arc::clone(&snap), 100);
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

	db.stop().expect("stop");
}

// Same shape for tables: every row in a table snapshot must carry its real
// per-row metadata, not the synthesized defaults.
#[test]
fn table_snapshot_system_columns_match_row_metadata() {
	let fast_config = StorageConfig {
		table_tick_interval: Duration::from_millis(50),
		series_tick_interval: Duration::from_millis(50),
		..StorageConfig::default()
	};

	let mut db = db_embedded::memory()
		.with_subsystem(Box::new(StorageSubsystemFactory::new(fast_config)))
		.build()
		.expect("build");
	db.start().expect("start");

	db.admin_as_root("CREATE NAMESPACE test", Params::None).expect("create namespace");
	db.admin_as_root("CREATE TABLE test::t { id: int4, name: utf8 }", Params::None).expect("create table");
	db.command_as_root(
		"INSERT test::t [{id: 1, name: \"alpha\"}, {id: 2, name: \"bravo\"}, {id: 3, name: \"charlie\"}]",
		Params::None,
	)
	.expect("insert");

	let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
	let registry = storage.registry();

	let snap = poll_until(
		|| {
			registry.list()
				.into_iter()
				.find(|s| s.name == "t" && s.row_count == 3)
				.and_then(|meta| registry.get(&meta.id))
		},
		Duration::from_secs(5),
	)
	.expect("table snapshot did not materialize within 5 seconds");

	let mut reader = SnapshotReader::new(Arc::clone(&snap), 100);
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

	db.stop().expect("stop");
}
