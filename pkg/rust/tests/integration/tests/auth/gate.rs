// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::testing::db::TestDb;
use reifydb_test_harness::auth::{AuthResponseAssert, auth_service};

fn password_credentials() -> HashMap<String, String> {
	HashMap::from([("identifier".to_string(), "alice".to_string()), ("password".to_string(), "secret".to_string())])
}

#[test]
fn a_registered_but_undeclared_method_is_not_enabled() {
	// Without the gate every registered provider would accept logins the app never switched on.
	let db = TestDb::memory();
	let service = auth_service(&db).build();

	service.authenticate("password", password_credentials()).unwrap().expect_method_not_enabled("password");
}

#[test]
fn an_unregistered_method_is_unknown_rather_than_not_enabled() {
	// The provider check must run first, otherwise a typo reads as a switched-off method.
	let db = TestDb::memory();
	let service = auth_service(&db).build();

	let err = service.authenticate("nosuch", password_credentials()).unwrap_err();
	assert_eq!(err.diagnostic().code, "AU_005");
}

#[test]
fn dropping_a_method_switches_its_login_off() {
	let db = TestDb::memory();
	db.admin("CREATE USER alice");
	db.admin("CREATE AUTHENTICATION METHOD password WITH { ttl: forever }");
	db.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'secret' }");
	let service = auth_service(&db).build();
	service.authenticate("password", password_credentials()).unwrap().expect_authenticated();

	db.admin("DROP AUTHENTICATION FOR alice { method: password }");
	db.admin("DROP AUTHENTICATION METHOD password");

	service.authenticate("password", password_credentials()).unwrap().expect_method_not_enabled("password");
}
