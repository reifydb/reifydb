// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::{Clock, MockClock, RuntimeConfig, embedded, value::value::duration::Duration};
use reifydb_test_harness::{
	auth::{AuthResponseAssert, password_credentials},
	db::TestDb,
};

fn create_db_with_mock_clock(mock: &MockClock, session_ttl: Duration) -> TestDb {
	let mut config = RuntimeConfig::default();
	config.clock = Clock::Mock(mock.clone());

	TestDb::from(
		embedded::memory()
			.with_runtime_config(config)
			.with_auth(move |a| a.session_ttl(session_ttl))
			.build()
			.unwrap(),
	)
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
	let ttl = Duration::from_seconds(60).unwrap();
	let mut db = create_db_with_mock_clock(&mock, ttl);

	let token = setup_user_and_login(&db);

	// Advance to just before TTL expires (59 seconds)
	mock.advance_secs(59);

	assert!(db.auth_service().validate_token(&token).is_some(), "Token should still be valid before TTL");

	db.stop();
}

#[test]
fn test_token_expires_after_ttl() {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let ttl = Duration::from_seconds(60).unwrap();
	let mut db = create_db_with_mock_clock(&mock, ttl);

	let token = setup_user_and_login(&db);

	// Advance past TTL (61 seconds)
	mock.advance_secs(61);

	assert!(db.auth_service().validate_token(&token).is_none(), "Token should be expired after TTL");

	db.stop();
}

#[test]
fn test_token_no_ttl_never_expires() {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let mut config = RuntimeConfig::default();
	config.clock = Clock::Mock(mock.clone());

	let mut db = TestDb::from(
		embedded::memory().with_runtime_config(config).with_auth(|a| a.no_session_ttl()).build().unwrap(),
	);

	let token = setup_user_and_login(&db);

	// Advance by 10 years
	mock.advance_secs(10 * 365 * 24 * 60 * 60);

	assert!(db.auth_service().validate_token(&token).is_some(), "Token with no TTL should never expire");

	db.stop();
}

#[test]
fn test_cleanup_removes_expired_tokens() {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let ttl = Duration::from_seconds(60).unwrap();
	let mut db = create_db_with_mock_clock(&mock, ttl);

	let token = setup_user_and_login(&db);

	// Token valid before expiry
	assert!(db.auth_service().validate_token(&token).is_some());

	// Advance past TTL
	mock.advance_secs(61);

	// Cleanup expired tokens
	db.auth_service().cleanup_expired();

	// Token should be gone from the database entirely
	assert!(db.auth_service().validate_token(&token).is_none(), "Expired token should be cleaned up");

	db.stop();
}
