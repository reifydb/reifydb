// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use super::common::{admin, fresh_db};

#[test]
fn create_policy_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create namespace demo");
	admin(&db, "create table demo::t { id: uint8 }");
	admin(&db, "create table policy demo_policy on demo::t { from: { filter { true } } }");

	let policies = db.catalog().materialized().list_all_policies();
	assert_eq!(policies.len(), 1);
	assert_eq!(policies[0].name.as_deref(), Some("demo_policy"));
	assert_eq!(policies[0].target_namespace.as_deref(), Some("demo"));
	assert_eq!(policies[0].target_shape.as_deref(), Some("t"));
	assert!(policies[0].enabled);
}
