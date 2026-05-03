// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::core::common::CommitVersion;

use super::common::{admin, fresh_db};

#[test]
fn grant_role_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create user alice");
	admin(&db, "create role analyst");
	admin(&db, "grant analyst to alice");

	let cat = db.catalog();
	let mat = cat.cache();
	let alice = mat.find_identity_by_name_at("alice", CommitVersion(u64::MAX)).unwrap();
	let analyst = mat.find_role_by_name_at("analyst", CommitVersion(u64::MAX)).unwrap();
	let granted = mat.find_granted_roles_at(alice.id, CommitVersion(u64::MAX));
	assert_eq!(granted.len(), 1);
	assert_eq!(granted[0].identity, alice.id);
	assert_eq!(granted[0].role_id, analyst.id);
}
