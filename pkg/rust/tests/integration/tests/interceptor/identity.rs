// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::core::common::CommitVersion;

use super::common::{admin, fresh_db};

#[test]
fn create_user_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create user alice");

	let identity =
		db.engine().catalog().materialized.find_identity_by_name_at("alice", CommitVersion(u64::MAX)).unwrap();
	assert_eq!(identity.name, "alice");
}
