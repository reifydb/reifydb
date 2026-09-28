// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::ops::Bound;

use reifydb::{
	WithSubsystem,
	core::{
		interface::catalog::storage::StorageId,
		key::{any::TaggedKey, series::PartitionedSeriesRowKeyRange},
	},
	embedded,
	testing::db::TestDb,
	transaction::{multi::RangeScope, transaction::Transaction},
};
use reifydb_value::value::{duration::Duration, identity::IdentityId, partition::Partition};

fn make_db() -> TestDb {
	let db = TestDb::from(embedded::memory().with_flow(|f| f).build().expect("build memory db"));
	db.admin("CREATE NAMESPACE test");
	db.admin("CREATE DICTIONARY test::pools FOR utf8 AS uint4");
	db
}

fn storage(db: &TestDb, name: &str) -> StorageId {
	let catalog = db.engine().catalog();
	let mut query = db.engine().begin_query(IdentityId::system()).expect("begin query");
	let mut txn = Transaction::Query(&mut query);
	let namespace =
		catalog.find_namespace_by_name(&mut txn, "test").expect("namespace").expect("namespace exists").id();
	if let Some(view) = catalog.find_view_by_name(&mut txn, namespace, name).expect("view") {
		return view.storage_id();
	}
	if let Some(table) = catalog.find_table_by_name(&mut txn, namespace, name).expect("table") {
		return StorageId::table(table.id);
	}
	if let Some(series) = catalog.find_series_by_name(&mut txn, namespace, name).expect("series") {
		return StorageId::series(series.id);
	}
	let ringbuffer =
		catalog.find_ringbuffer_by_name(&mut txn, namespace, name).expect("ringbuffer").expect("object exists");
	StorageId::ringbuffer(ringbuffer.id)
}

fn stored_partitions(db: &TestDb, name: &str) -> Vec<Partition> {
	let storage = storage(db, name);
	let mut query = db.engine().begin_query(IdentityId::system()).expect("begin query");
	let mut txn = Transaction::Query(&mut query);
	let mut out: Vec<Partition> = txn
		.range_partitioned_row(storage, Bound::Unbounded, Bound::Unbounded, RangeScope::All, 1024)
		.expect("range partitioned rows")
		.map(|row| row.expect("row").key.partition.0)
		.collect();
	for row in txn
		.range(PartitionedSeriesRowKeyRange::full_scan(storage), RangeScope::All, 1024)
		.expect("range series")
	{
		match row.expect("series row").key {
			TaggedKey::PartitionedSeriesRow(key) => out.push(key.partition),
			other => panic!("partitioned series scan yielded {other:?}"),
		}
	}
	out
}

#[test]
fn a_table_backed_view_places_a_dictionary_partition_value_where_its_source_table_does() {
	// A view must hash the dictionary id like its source table, otherwise one value lives in two partitions and a
	// lookup can match only one.
	let db = make_db();
	db.admin("CREATE TABLE test::src { pool: utf8 with { dictionary: test::pools }, n: int4 } \
		 WITH { partition: { by: { pool } } }");
	db.admin("CREATE DEFERRED VIEW test::v { pool: utf8 with { dictionary: test::pools }, n: int4 } \
		 WITH { partition: { by: { pool } } } AS { FROM test::src }");
	db.command("INSERT test::src [{ pool: 'aa', n: 1 }]");
	assert_eq!(
		db.await_row_count("FROM test::v", 1, Duration::from_seconds_const(5)),
		1,
		"precondition: the view row materializes"
	);

	let source = stored_partitions(&db, "src");
	assert_eq!(source.len(), 1, "precondition: the source row sits in one partition");
	assert_eq!(stored_partitions(&db, "v"), source, "the view row must share its source's partition");
}

#[test]
fn a_series_view_places_a_dictionary_partition_value_where_its_source_series_does() {
	// A view must hash the dictionary id like its source series, otherwise one value lives in two partitions and a
	// lookup can match only one.
	let db = make_db();
	db.admin("CREATE SERIES test::src { ts: int8, pool: utf8 with { dictionary: test::pools }, n: int4 } \
		 WITH { key: ts, partition: { by: { pool } } }");
	db.admin(
		"CREATE DEFERRED SERIES VIEW test::v { ts: int8, pool: utf8 with { dictionary: test::pools }, n: int4 } \
		 WITH { key: ts, partition: { by: { pool } } } AS { FROM test::src }",
	);
	db.command("INSERT test::src [{ ts: 1, pool: 'aa', n: 1 }]");
	assert_eq!(
		db.await_row_count("FROM test::v", 1, Duration::from_seconds_const(5)),
		1,
		"precondition: the view row materializes"
	);

	let source = stored_partitions(&db, "src");
	assert_eq!(source.len(), 1, "precondition: the source row sits in one partition");
	assert_eq!(stored_partitions(&db, "v"), source, "the view row must share its source's partition");
}

#[test]
fn a_ringbuffer_view_places_a_dictionary_partition_value_where_its_source_ringbuffer_does() {
	// A view must hash the dictionary id like its source ringbuffer, otherwise one value lives in two partitions
	// and a lookup can match only one.
	let db = make_db();
	db.admin("CREATE RINGBUFFER test::src { pool: utf8 with { dictionary: test::pools }, n: int4 } \
		 WITH { capacity: 10, partition: { by: { pool } } }");
	db.admin("CREATE DEFERRED RINGBUFFER VIEW test::v { pool: utf8 with { dictionary: test::pools }, n: int4 } \
		 WITH { capacity: 10, partition: { by: { pool } } } AS { FROM test::src }");
	db.command("INSERT test::src [{ pool: 'aa', n: 1 }]");
	assert_eq!(
		db.await_row_count("FROM test::v", 1, Duration::from_seconds_const(5)),
		1,
		"precondition: the view row materializes"
	);

	let source = stored_partitions(&db, "src");
	assert_eq!(source.len(), 1, "precondition: the source row sits in one partition");
	assert_eq!(stored_partitions(&db, "v"), source, "the view row must share its source's partition");
}
