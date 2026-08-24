// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::testing::db::TestDb;

const SEED_CALL_POLICIES: [&str; 5] = [
	"system_call_rql_tokenize",
	"system_call_rql_ast",
	"system_call_rql_logical",
	"system_call_rql_explain",
	"system_call_graphql_explain",
];

#[test]
fn create_policy_propagates_to_materialized_cache() {
	let db = TestDb::memory();

	db.admin("create namespace demo");
	db.admin("create table demo::t { id: uint8 }");
	db.admin("create table policy demo_policy on demo::t { from: { filter { true } } }");

	let policies = db.catalog().cache().list_all_policies();

	let matching: Vec<_> = policies.iter().filter(|p| p.name.as_deref() == Some("demo_policy")).collect();
	assert_eq!(matching.len(), 1, "the new policy must reach the cache exactly once");
	assert_eq!(matching[0].target_namespace.as_deref(), Some("demo"));
	assert_eq!(matching[0].target_object.as_deref(), Some("t"));
	assert!(matching[0].enabled);

	for seed in SEED_CALL_POLICIES {
		// The bootstrap seeds these, so a cache that only holds the newest policy would drop them.
		assert!(
			policies.iter().any(|p| p.name.as_deref() == Some(seed)),
			"bootstrap seed policy {seed} is missing from the materialized cache"
		);
	}
}
