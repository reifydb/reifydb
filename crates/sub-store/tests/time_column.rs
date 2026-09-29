// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#![cfg(feature = "column")]

use std::{collections::BTreeSet, sync::Arc};

use arrow_array::RecordBatch;
use reifydb::{
	WithSubsystem, embedded as db_embedded,
	testing::db::{TestDb, poll_until},
};
use reifydb_core::{
	common::{CommitVersion, TimeSource},
	interface::catalog::id::ColumnSnapshotId,
	value::{batch, column::factory},
};
use reifydb_store_column::{
	compress::Compressor, reader::SnapshotReader, session::new_session, snapshot::ColumnBlock, store::ColumnStore,
};
use reifydb_sub_store::{
	column::actor::batches::{column_block_from_batches, system_column_schema},
	factory::StorageSubsystemFactory,
	subsystem::{StorageConfig, StorageSubsystem},
};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::{
	Value,
	datetime::DateTime,
	duration::Duration,
	identity::IdentityId,
	system_columns::{SystemColumn, column_view, time, with_system_column},
	value_type::ValueType,
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
		..StorageConfig::default()
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

fn datetime_at(columns: &RecordBatch, name: &str, row: usize) -> DateTime {
	match column_view(columns, name).expect("column view").expect("column").get_value(row) {
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
		let mut reader = SnapshotReader::new(block, 100, store.session().clone());
		let batch = reader.next().expect("batch present").expect("read batch");
		assert!(reader.next().is_none(), "reader should yield a single batch for 3 rows");
		assert_eq!(batch.num_rows(), 3);

		if !timed {
			assert!(
				time(&batch).expect("#time").is_empty(),
				"test::{name} must read back with no #time, not a filled one"
			);
			continue;
		}
		assert_eq!(time(&batch).expect("#time").len(), 3, "test::{name} must read back one #time per row");
		let mut ids = BTreeSet::new();
		for row in 0..3 {
			let id = match column_view(&batch, "id").expect("id view").expect("id column").get_value(row) {
				Value::Int4(v) => v,
				other => panic!("row {row}: expected Int4, got {other:?}"),
			};
			assert_eq!(datetime_at(&batch, "at", row), at(id as u64), "row {row}: populator value");
			assert_eq!(
				time(&batch).expect("#time")[row],
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
			assert!(!block.is_empty(), "test::{name}: a closed bucket must hold rows");

			let len = block.len();
			let mut reader = SnapshotReader::new(block, 100, store.session().clone());
			let batch = reader.next().expect("batch present").expect("read batch");
			assert!(reader.next().is_none(), "reader should yield a single batch per bucket");
			assert_eq!(batch.num_rows(), len);

			if timed {
				assert_eq!(
					time(&batch).expect("#time").len(),
					len,
					"test::{name} must read back one #time per row"
				);
			} else {
				assert!(
					time(&batch).expect("#time").is_empty(),
					"test::{name} must read back with no #time, not a filled one"
				);
			}
			for row in 0..len {
				let k = match column_view(&batch, "k")
					.expect("k view")
					.expect("k column")
					.get_value(row)
				{
					Value::Uint8(v) => v,
					other => panic!("row {row}: expected Uint8, got {other:?}"),
				};
				assert_eq!(datetime_at(&batch, "at", row), at(k), "row {row}: populator value");
				if timed {
					assert_eq!(
						time(&batch).expect("#time")[row],
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

fn stamped(created: DateTime, timed: bool) -> RecordBatch {
	let columns = batch::batch(vec![factory::int4("id", [1, 2])]).expect("id batch");
	let columns = with_system_column(columns, SystemColumn::RowNumbers, factory::uint8("#rownum", [1u64, 2]).1)
		.expect("#rownum");
	let columns =
		with_system_column(columns, SystemColumn::CreatedAt, factory::datetime("#created_at", [created; 2]).1)
			.expect("#created_at");
	let columns =
		with_system_column(columns, SystemColumn::UpdatedAt, factory::datetime("#updated_at", [created; 2]).1)
			.expect("#updated_at");
	if !timed {
		return columns;
	}
	with_system_column(columns, SystemColumn::Time, factory::datetime("#time", [created; 2]).1).expect("#time")
}

#[test]
fn a_timed_block_refuses_a_batch_without_time() {
	// Writing it would pair a zero-row #time with full columns, silently in builds without assertions.
	let created = DateTime::from_ymd_hms(2020, 1, 1, 0, 0, 0).unwrap();
	let batch = stamped(created, false);
	let mut schema = vec![("id".to_string(), ValueType::Int4)];
	schema.extend(system_column_schema(&TimeSource::Processing, false));

	let err = column_block_from_batches(schema, vec![batch], CommitVersion(1), &Compressor::new(new_session()))
		.err()
		.expect("a timed block must not be built from a batch with no #time");

	assert_eq!(err.diagnostic().code, "SCOL_004");
}

#[test]
fn a_timeless_block_refuses_a_batch_that_carries_time() {
	// Dropping the stamps would hide that the rows and the object's time declaration disagree.
	let created = DateTime::from_ymd_hms(2020, 1, 1, 0, 0, 0).unwrap();
	let batch = stamped(created, true);
	let mut schema = vec![("id".to_string(), ValueType::Int4)];
	schema.extend(system_column_schema(&TimeSource::None, false));

	let err = column_block_from_batches(schema, vec![batch], CommitVersion(1), &Compressor::new(new_session()))
		.err()
		.expect("a timeless block must not be built from a batch that carries #time");

	assert_eq!(err.diagnostic().code, "SCOL_004");
}
