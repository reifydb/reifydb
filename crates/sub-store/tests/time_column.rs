// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "column")]

use std::{collections::BTreeSet, sync::Arc};

use reifydb::{
	WithSubsystem, embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_column::{
	compress::{CompressConfig, Compressor},
	reader::SnapshotReader,
	snapshot::ColumnBlock,
};
use reifydb_core::{
	common::{CommitVersion, TimeSource},
	interface::catalog::id::ColumnSnapshotId,
	value::column::{ColumnWithName, columns::Columns},
};
use reifydb_store_column::ColumnStore;
use reifydb_sub_store::{
	column::actor::batches::{column_block_from_batches, system_column_schema},
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{
	Value, datetime::DateTime, duration::Duration, identity::IdentityId, row_number::RowNumber,
	system_columns::SystemColumns, value_type::ValueType,
};

enum Object {
	Table,
	Series,
}

fn db() -> TestDb {
	let config = StorageConfig {
		table_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_tick_interval: Duration::from_milliseconds(50).unwrap(),
		series_bucket_width: 5,
		series_grace: Duration::from_milliseconds(0).unwrap(),
	};
	let db = TestDb::from(
		db_embedded::memory()
			.with_subsystem(Box::new(StorageSubsystemFactory::new(config)))
			.build()
			.expect("build"),
	);
	db.admin("CREATE NAMESPACE test");
	db
}

fn at(second: u64) -> DateTime {
	DateTime::from_ymd_hms(2020, 1, 1, 0, 0, second as u32).unwrap()
}

fn owned_blocks(db: &TestDb, store: &ColumnStore, object: &Object, name: &str) -> Vec<Arc<ColumnBlock>> {
	// Entries are read before the catalog so a block put after its snapshot commit is never missed.
	let entries = store.entries();
	let engine = db.engine();
	let catalog = engine.catalog();
	let mut txn = engine.begin_query(IdentityId::system()).expect("begin query");
	let mut tx = Transaction::Query(&mut txn);
	let namespace = catalog.find_namespace_by_name(&mut tx, "test").expect("find namespace").expect("namespace");
	let owned: Vec<ColumnSnapshotId> = match object {
		Object::Table => {
			let table = catalog
				.find_table_by_name(&mut tx, namespace.id(), name)
				.expect("find table")
				.expect("table");
			catalog.list_column_snapshots_for_table(&mut tx, table.id).expect("list table snapshots")
		}
		Object::Series => {
			let series = catalog
				.find_series_by_name(&mut tx, namespace.id(), name)
				.expect("find series")
				.expect("series");
			catalog.list_column_snapshots_for_series(&mut tx, series.id).expect("list series snapshots")
		}
	}
	.into_iter()
	.map(|snapshot| snapshot.id)
	.collect();
	entries.into_iter().filter(|(id, _)| owned.contains(id)).map(|(_, block)| block).collect()
}

fn schema_names(block: &ColumnBlock) -> Vec<&str> {
	block.schema.iter().map(|(n, _, _)| n.as_str()).collect()
}

fn datetime_at(columns: &Columns, name: &str, row: usize) -> DateTime {
	match columns.column(name).expect("column").data().get_value(row) {
		Value::DateTime(v) => v,
		other => panic!("row {row}: expected DateTime in {name}, got {other:?}"),
	}
}

#[test]
fn a_timed_table_block_carries_time_and_a_timeless_one_does_not() {
	// A timeless block given #time holds a zero-row column; a timed block without it loses the declared clock.
	let mut db = db();
	db.admin("CREATE TABLE test::timed { id: int4, at: datetime } WITH { time: event(at) }");
	db.admin("CREATE TABLE test::timeless { id: int4, at: datetime }");
	for table in ["timed", "timeless"] {
		db.command(&format!(
			"INSERT test::{table} [{{id: 1, at: @2020-01-01T00:00:01Z}}, {{id: 2, at: @2020-01-01T00:00:02Z}}, \
			 {{id: 3, at: @2020-01-01T00:00:03Z}}]"
		));
	}

	let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
	let store = storage.block_store().clone();
	let timeout = Duration::from_seconds(5).unwrap().to_std();

	for (name, timed) in [("timed", true), ("timeless", false)] {
		let blocks = poll_until(
			|| {
				let blocks = owned_blocks(&db, &store, &Object::Table, name);
				blocks.iter().any(|b| b.len() == 3).then_some(blocks)
			},
			timeout,
		)
		.unwrap_or_else(|| panic!("a 3-row block of test::{name} did not materialize within 5 seconds"));

		let expected_schema: &[&str] = if timed {
			&["id", "at", "#rownum", "#created_at", "#updated_at", "#time", "#commit_version"]
		} else {
			&["id", "at", "#rownum", "#created_at", "#updated_at", "#commit_version"]
		};
		for block in &blocks {
			assert_eq!(schema_names(block), expected_schema, "test::{name}");
		}

		let block = blocks.into_iter().find(|b| b.len() == 3).expect("3-row block");
		let mut reader = SnapshotReader::new(block, 100);
		let batch = reader.next().expect("batch present").expect("read batch");
		assert!(reader.next().is_none(), "reader should yield a single batch for 3 rows");
		assert_eq!(batch.row_count(), 3);

		if !timed {
			assert!(batch.time().is_empty(), "test::{name} must read back with no #time, not a filled one");
			continue;
		}
		assert_eq!(batch.time().len(), 3, "test::{name} must read back one #time per row");
		let mut ids = BTreeSet::new();
		for row in 0..3 {
			let id = match batch.column("id").expect("id column").data().get_value(row) {
				Value::Int4(v) => v,
				other => panic!("row {row}: expected Int4, got {other:?}"),
			};
			assert_eq!(datetime_at(&batch, "at", row), at(id as u64), "row {row}: populator value");
			assert_eq!(
				batch.time()[row],
				at(id as u64),
				"row {row}: #time must be the event time of its own row"
			);
			ids.insert(id);
		}
		assert_eq!(ids, BTreeSet::from([1, 2, 3]));
	}

	db.stop();
}

#[test]
fn a_timed_series_block_carries_time_and_a_timeless_one_does_not() {
	// The series schema is built on a separate path, so without this pin it can regress while tables stay green.
	let mut db = db();
	db.admin(
		"CREATE SERIES test::timed { k: uint8, at: datetime, value: float8 } WITH { key: k, time: event(at) }",
	);
	db.admin("CREATE SERIES test::timeless { k: uint8, at: datetime, value: float8 } WITH { key: k }");
	let rows: Vec<String> =
		(0u64..=11).map(|k| format!("{{k: {k}, at: @2020-01-01T00:00:{k:02}Z, value: {k}.0}}")).collect();
	for series in ["timed", "timeless"] {
		db.command(&format!("INSERT test::{series} [{}]", rows.join(", ")));
	}

	let storage = db.subsystem::<StorageSubsystem>().expect("StorageSubsystem registered");
	let store = storage.block_store().clone();
	let timeout = Duration::from_seconds(5).unwrap().to_std();

	for (name, timed) in [("timed", true), ("timeless", false)] {
		let blocks = poll_until(
			|| {
				let blocks = owned_blocks(&db, &store, &Object::Series, name);
				(blocks.len() >= 2).then_some(blocks)
			},
			timeout,
		)
		.unwrap_or_else(|| panic!("two buckets of test::{name} did not materialize within 5 seconds"));

		let expected_schema: &[&str] = if timed {
			&["k", "at", "value", "#rownum", "#created_at", "#updated_at", "#time", "#commit_version"]
		} else {
			&["k", "at", "value", "#rownum", "#created_at", "#updated_at", "#commit_version"]
		};
		let mut keys = BTreeSet::new();
		for block in blocks {
			assert_eq!(schema_names(&block), expected_schema, "test::{name}");
			assert!(block.len() > 0, "test::{name}: a closed bucket must hold rows");

			let len = block.len();
			let mut reader = SnapshotReader::new(block, 100);
			let batch = reader.next().expect("batch present").expect("read batch");
			assert!(reader.next().is_none(), "reader should yield a single batch per bucket");
			assert_eq!(batch.row_count(), len);

			if timed {
				assert_eq!(batch.time().len(), len, "test::{name} must read back one #time per row");
			} else {
				assert!(
					batch.time().is_empty(),
					"test::{name} must read back with no #time, not a filled one"
				);
			}
			for row in 0..len {
				let k = match batch.column("k").expect("k column").data().get_value(row) {
					Value::Uint8(v) => v,
					other => panic!("row {row}: expected Uint8, got {other:?}"),
				};
				assert_eq!(datetime_at(&batch, "at", row), at(k), "row {row}: populator value");
				if timed {
					assert_eq!(
						batch.time()[row],
						at(k),
						"row {row}: #time must be the event time of its row"
					);
				}
				assert!(keys.insert(k), "duplicate key {k} across buckets of test::{name}");
			}
		}
		for k in 0u64..=9 {
			assert!(
				keys.contains(&k),
				"test::{name}: key {k} from closed buckets [0,5) and [5,10) is missing"
			);
		}
	}

	db.stop();
}

#[test]
fn a_timed_block_refuses_a_batch_without_time() {
	// Writing it would pair a zero-row #time with full columns, silently in builds without assertions.
	let created = DateTime::from_ymd_hms(2020, 1, 1, 0, 0, 0).unwrap();
	let batch = Columns::with_system(
		vec![ColumnWithName::int4("id", [1, 2])],
		SystemColumns::new(
			vec![RowNumber(1), RowNumber(2)],
			Vec::new(),
			vec![created; 2],
			vec![created; 2],
			Vec::new(),
		),
	);
	let mut schema = vec![("id".to_string(), ValueType::Int4)];
	schema.extend(system_column_schema(&TimeSource::Processing));

	let err = column_block_from_batches(
		schema,
		vec![batch],
		CommitVersion(1),
		&Compressor::new(CompressConfig::default()),
	)
	.err()
	.expect("a timed block must not be built from a batch with no #time");

	assert_eq!(err.diagnostic().code, "SCOL_004");
}

#[test]
fn a_timeless_block_refuses_a_batch_that_carries_time() {
	// Dropping the stamps would hide that the rows and the object's time declaration disagree.
	let created = DateTime::from_ymd_hms(2020, 1, 1, 0, 0, 0).unwrap();
	let batch = Columns::with_system(
		vec![ColumnWithName::int4("id", [1, 2])],
		SystemColumns::new(
			vec![RowNumber(1), RowNumber(2)],
			Vec::new(),
			vec![created; 2],
			vec![created; 2],
			vec![created; 2],
		),
	);
	let mut schema = vec![("id".to_string(), ValueType::Int4)];
	schema.extend(system_column_schema(&TimeSource::None));

	let err = column_block_from_batches(
		schema,
		vec![batch],
		CommitVersion(1),
		&Compressor::new(CompressConfig::default()),
	)
	.err()
	.expect("a timeless block must not be built from a batch that carries #time");

	assert_eq!(err.diagnostic().code, "SCOL_004");
}
