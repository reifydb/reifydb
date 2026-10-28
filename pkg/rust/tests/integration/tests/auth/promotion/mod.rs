// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod github;
mod password;
mod solana;

use std::collections::HashMap;

use reifydb::{IdentityId, auth::service::AuthResponse, testing::db::TestDb, transaction::transaction::Transaction};
use reifydb_test_harness::auth::AuthResponseAssert;
use reifydb_value::value::identity::IdentityKind;

fn promotion_db(promote_guest: bool) -> TestDb {
	let db = TestDb::memory();
	db.admin("CREATE AUTHENTICATION METHOD guest WITH { ttl: forever }");
	db.admin(&format!(
		"CREATE AUTHENTICATION METHOD password WITH {{ ttl: forever, promote_guest: {promote_guest} }}"
	));
	db.admin(&format!(
		"CREATE AUTHENTICATION METHOD solana WITH {{ ttl: forever, promote_guest: {promote_guest} }}"
	));
	db
}

fn guest_login(db: &TestDb) -> (IdentityId, String) {
	db.auth_service().authenticate("guest", HashMap::new()).unwrap().expect_authenticated()
}

fn password_login(db: &TestDb, caller: IdentityId, identifier: &str, password: &str) -> AuthResponse {
	db.auth_service()
		.authenticate_as(
			caller,
			"password",
			HashMap::from([
				("identifier".to_string(), identifier.to_string()),
				("password".to_string(), password.to_string()),
			]),
		)
		.unwrap()
}

fn name_and_kind(db: &TestDb, identity: IdentityId) -> (String, IdentityKind) {
	let mut txn = db.engine().begin_query(IdentityId::system()).unwrap();
	let found = db.engine().catalog().find_identity(&mut Transaction::Query(&mut txn), identity).unwrap().unwrap();
	(found.name.clone(), found.resolved_kind())
}

fn session_of(db: &TestDb, token: &str) -> Option<IdentityId> {
	db.auth_service().validate_token(token).unwrap().map(|session| session.identity)
}
