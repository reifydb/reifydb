// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use reifydb::core::common::CommitVersion;

use super::common::{admin, fresh_db};

#[test]
fn create_user_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create user alice");

	let identity = db.catalog().cache().find_identity_by_name_at("alice", CommitVersion(u64::MAX)).unwrap();
	assert_eq!(identity.name, "alice");
}
