// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
#![cfg(feature = "database")]

use std::collections::HashMap;

use reifydb_test_harness::{
	auth::{AuthResponseAssert, auth_service},
	db::TestDb,
	fixture::identity::identity,
};

#[test]
fn password_identity_authenticates_through_the_service() {
	let db = TestDb::memory();

	// An identity provisioned by the builder must be authenticable through a service the
	// builder also wires: proof that the harness's provisioning matches what the auth
	// stack expects end to end.
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
	service
		.authenticate(
			"password",
			HashMap::from([
				("identifier".to_string(), "alice".to_string()),
				("password".to_string(), "wrong".to_string()),
			]),
		)
		.unwrap()
		.expect_failed("invalid credentials");
}
