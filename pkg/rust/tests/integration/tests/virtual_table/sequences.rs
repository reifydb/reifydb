// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{RuntimeConfig, embedded, testing::db::TestDb};
use reifydb_test_harness::assert::column_values;
use reifydb_value::value::Value;

fn new_db() -> TestDb {
	TestDb::from(embedded::memory().with_runtime_config(RuntimeConfig::default().seeded(0)).build().expect("build"))
}

fn counter(db: &TestDb, name: &str) -> u64 {
	let frames = db.query(&format!("FROM system::sequences FILTER {{ name == \"{name}\" }} MAP {{ value }}"));
	let values = column_values(frames.first().expect("system::sequences returned no frame"), "value");
	match values.as_slice() {
		[Value::Uint8(value)] => *value,
		other => panic!("expected exactly one uint8 value for sequence {name}, found {other:?}"),
	}
}

#[test]
fn the_namespace_counter_advances_when_a_namespace_is_created() {
	// The read path once used a layout and a store the writer never wrote to, so this reported 0 forever.
	let db = new_db();

	let before = counter(&db, "namespace");
	db.admin("CREATE NAMESPACE alpha");
	let after = counter(&db, "namespace");

	assert!(after > before, "namespace counter must advance past {before}, reported {after}");
}

#[test]
fn every_counter_that_issued_an_id_reports_a_non_zero_value() {
	// Reporting 0 while ids are issued correctly is exactly the failure this vtable is supposed to expose.
	let db = new_db();

	db.admin("CREATE NAMESPACE alpha");
	db.admin("CREATE TABLE alpha::items { id: int4 }");

	assert!(counter(&db, "namespace") > 0, "namespace ids were issued, so its counter cannot read 0");
	assert!(counter(&db, "source") > 0, "a table was created, so the source counter cannot read 0");
	assert!(counter(&db, "column") > 0, "a column was created, so the column counter cannot read 0");
}

#[test]
fn the_reported_counter_covers_every_declared_sequence() {
	// A sequence missing from the listing hides its counter entirely rather than reporting it wrongly.
	let db = new_db();

	let frames = db.query("FROM system::sequences");
	let names = column_values(frames.first().expect("system::sequences returned no frame"), "name");

	assert_eq!(names.len(), 24, "every declared sequence must be listed, found {}", names.len());
}
