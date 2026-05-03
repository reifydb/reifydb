// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb::core::common::CommitVersion;

use super::common::{admin, fresh_db};

#[test]
fn create_authentication_propagates_to_materialized_cache() {
	let db = fresh_db();

	admin(&db, "create user alice");
	admin(&db, "create authentication for alice { method: token; token: 'secret' }");

	let mat = &db.engine().catalog().materialized;
	let alice = mat.find_identity_by_name_at("alice", CommitVersion(u64::MAX)).unwrap();
	let auths = mat.list_authentications_by_method_at("token", CommitVersion(u64::MAX));
	assert_eq!(auths.len(), 1);
	assert_eq!(auths[0].identity, alice.id);
	assert_eq!(auths[0].method, "token");
}
