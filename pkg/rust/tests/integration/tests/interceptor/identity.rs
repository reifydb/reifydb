// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::core::common::CommitVersion;
use reifydb_test_harness::db::TestDb;

#[test]
fn create_user_propagates_to_materialized_cache() {
	let db = TestDb::memory();

	db.admin("create user alice");

	let identity = db.catalog().cache().find_identity_by_name_at("alice", CommitVersion(u64::MAX)).unwrap();
	assert_eq!(identity.name, "alice");
}
