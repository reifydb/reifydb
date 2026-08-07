// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::testing::db::TestDb;
use reifydb_test_harness::{
	auth::{AuthResponseAssert, auth_service},
	fixture::identity::identity,
};

#[test]
fn password_identity_authenticates_through_the_service() {
	let db = TestDb::memory();

	// Provisioning and authentication go through separate paths, so only driving both proves
	// the harness writes what the auth stack reads.
	let alice = identity("alice").password("secret").create(&db);

	let service = auth_service(&db).build();
	let (identity, token) = service
		.authenticate(
			"password",
			HashMap::from([
				("identifier".to_string(), "alice".to_string()),
				("password".to_string(), "secret".to_string()),
			]),
		)
		.unwrap()
		.expect_authenticated();

	assert_eq!(identity, alice.id);
	assert!(!token.is_empty());
}

#[test]
fn wrong_password_fails_closed() {
	let db = TestDb::memory();
	identity("alice").password("secret").create(&db);

	let service = auth_service(&db).build();
	service.authenticate(
		"password",
		HashMap::from([
			("identifier".to_string(), "alice".to_string()),
			("password".to_string(), "wrong".to_string()),
		]),
	)
	.unwrap()
	.expect_failed("invalid credentials");
}
