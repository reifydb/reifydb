// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::db::TestDb;

#[test]
fn create_namespace_propagates_to_materialized_cache() {
	let db = TestDb::memory();

	db.admin("create namespace demo");

	let ns = db.catalog().cache().find_namespace_by_name("demo").unwrap();
	assert_eq!(ns.name(), "demo");
}
