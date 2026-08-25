// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{testing::db::TestDb, value::value::identity::IdentityId};
use reifydb_test_harness::auth::{AuthResponseAssert, password_credentials};

fn setup_user_and_login(db: &TestDb) -> (IdentityId, String) {
	db.admin("CREATE USER alice");
	db.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }");

	db.auth_service()
		.authenticate("password", password_credentials("alice", "alice-pass"))
		.unwrap()
		.expect_authenticated()
}

#[test]
fn test_disabled_identity_token_is_rejected() {
	// Disabling only locks an account out if validation refuses the session tokens already handed out.
	let mut db = TestDb::memory();
	let (identity, token) = setup_user_and_login(&db);

	assert!(db.auth_service().validate_token(&token).unwrap().is_some(), "a freshly minted token must validate");

	let mut txn = db.engine().begin_admin(IdentityId::root()).unwrap();
	db.catalog().disable_identity(&mut txn, identity).unwrap();
	txn.commit().unwrap();

	assert!(
		db.auth_service().validate_token(&token).unwrap().is_none(),
		"a token minted before the identity was disabled must stop validating"
	);

	db.stop();
}

#[test]
fn test_reenabled_identity_token_validates_again() {
	// Rejection must live at validation, never in a revoke-on-disable sweep that would strand live sessions.
	let mut db = TestDb::memory();
	let (identity, token) = setup_user_and_login(&db);

	let mut txn = db.engine().begin_admin(IdentityId::root()).unwrap();
	db.catalog().disable_identity(&mut txn, identity).unwrap();
	txn.commit().unwrap();

	assert!(db.auth_service().validate_token(&token).unwrap().is_none(), "the disabled account must be locked out");

	let mut txn = db.engine().begin_admin(IdentityId::root()).unwrap();
	db.catalog().enable_identity(&mut txn, identity).unwrap();
	txn.commit().unwrap();

	let validated = db
		.auth_service()
		.validate_token(&token)
		.unwrap()
		.expect("re-enabling must restore the existing session");
	assert_eq!(validated.identity, identity, "the restored token must still resolve to the same account");

	db.stop();
}
