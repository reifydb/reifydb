// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::core::common::CommitVersion;

use super::common::{admin, fresh_db};

#[test]
fn create_role_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create role analyst");

	let role = db.catalog().materialized().find_role_by_name_at("analyst", CommitVersion(u64::MAX)).unwrap();
	assert_eq!(role.name, "analyst");
}
