// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::core::common::CommitVersion;
use reifydb_test_harness::db::TestDb;

#[test]
fn create_role_propagates_to_materialized_cache() {
	let db = TestDb::memory();

	db.admin("create role analyst");

	let role = db.catalog().cache().find_role_by_name_at("analyst", CommitVersion(u64::MAX)).unwrap();
	assert_eq!(role.name, "analyst");
}
