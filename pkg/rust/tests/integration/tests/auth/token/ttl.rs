// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Clock, MockClock, RuntimeConfig, embedded, testing::db::TestDb};
use reifydb_test_harness::auth::{AuthResponseAssert, password_credentials};

fn create_db_with_mock_clock(mock: &MockClock, ttl: &str) -> TestDb {
	let config = RuntimeConfig {
		clock: Clock::Mock(mock.clone()),
		..Default::default()
	};

	let db = TestDb::from(embedded::memory().with_runtime_config(config).build().unwrap());
	db.admin(&format!("CREATE AUTHENTICATION METHOD password WITH {{ ttl: {ttl} }}"));
	db
}

fn setup_user_and_login(db: &TestDb) -> String {
	db.admin("CREATE USER alice");
	db.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }");

	db.auth_service()
		.authenticate("password", password_credentials("alice", "alice-pass"))
		.unwrap()
		.expect_authenticated()
		.1
}

#[test]
fn test_token_valid_before_ttl() {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let mut db = create_db_with_mock_clock(&mock, "60s");

	let token = setup_user_and_login(&db);

	mock.advance_secs(59);

	assert!(db.auth_service().validate_token(&token).unwrap().is_some(), "Token should still be valid before TTL");

	db.stop();
}

#[test]
fn test_token_expires_after_ttl() {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let mut db = create_db_with_mock_clock(&mock, "60s");

	let token = setup_user_and_login(&db);

	mock.advance_secs(61);

	assert!(db.auth_service().validate_token(&token).unwrap().is_none(), "Token should be expired after TTL");

	db.stop();
}

#[test]
fn test_token_no_ttl_never_expires() {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let mut db = create_db_with_mock_clock(&mock, "forever");

	let token = setup_user_and_login(&db);

	mock.advance_secs(10 * 365 * 24 * 60 * 60);

	assert!(db.auth_service().validate_token(&token).unwrap().is_some(), "Token with no TTL should never expire");

	db.stop();
}

#[test]
fn test_cleanup_removes_expired_tokens() {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let mut db = create_db_with_mock_clock(&mock, "60s");

	let token = setup_user_and_login(&db);

	assert!(db.auth_service().validate_token(&token).unwrap().is_some());

	mock.advance_secs(61);

	db.auth_service().cleanup_expired().unwrap();

	assert!(db.auth_service().validate_token(&token).unwrap().is_none(), "Expired token should be cleaned up");

	db.stop();
}
