// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::db::TestDb;

#[test]
fn create_migration_propagates_to_materialized_cache() {
	let db = TestDb::memory();

	db.admin("create migration 'm1' { create namespace demo };");
	db.admin("migrate;");

	let cat = db.catalog();
	let mat = cat.cache();
	let migration = mat.find_migration_by_name("m1").unwrap();
	assert_eq!(migration.name, "m1");
	assert!(migration.body.contains("create namespace demo"));
}
