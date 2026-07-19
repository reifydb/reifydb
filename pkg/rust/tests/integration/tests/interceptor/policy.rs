// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::db::TestDb;

#[test]
fn create_policy_propagates_to_materialized_cache() {
	let db = TestDb::memory();

	db.admin("create namespace demo");
	db.admin("create table demo::t { id: uint8 }");
	db.admin("create table policy demo_policy on demo::t { from: { filter { true } } }");

	let policies = db.catalog().cache().list_all_policies();
	assert_eq!(policies.len(), 1);
	assert_eq!(policies[0].name.as_deref(), Some("demo_policy"));
	assert_eq!(policies[0].target_namespace.as_deref(), Some("demo"));
	assert_eq!(policies[0].target_shape.as_deref(), Some("t"));
	assert!(policies[0].enabled);
}
