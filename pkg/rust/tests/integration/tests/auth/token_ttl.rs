// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::time::Duration;

use reifydb::{Clock, MockClock, Params, SharedRuntime, SharedRuntimeConfig, auth::service::AuthResponse, embedded};

fn create_db_with_mock_clock(mock: &MockClock, session_ttl: Duration) -> reifydb::Database {
	let mut config = SharedRuntimeConfig::default();
	config.clock = Clock::Mock(mock.clone());
	let runtime = SharedRuntime::from_config(config);

	embedded::memory().with_runtime(runtime).with_auth(move |a| a.session_ttl(session_ttl)).build().unwrap()
}

fn setup_user_and_login(db: &mut reifydb::Database) -> String {
	db.start().unwrap();

	db.admin_as_root("CREATE USER alice", Params::None).unwrap();
	db.admin_as_root("CREATE AUTHENTICATION FOR alice { method: password; password: 'alice-pass' }", Params::None)
		.unwrap();

	let mut credentials = std::collections::HashMap::new();
	credentials.insert("identifier".to_string(), "alice".to_string());
	credentials.insert("password".to_string(), "alice-pass".to_string());

	match db.auth_service().authenticate("password", credentials).unwrap() {
		AuthResponse::Authenticated {
			token,
			..
		} => token,
		other => panic!("Expected Authenticated, got {:?}", other),
	}
}

#[test]
fn test_token_valid_before_ttl() {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let ttl = Duration::from_secs(60);
	let mut db = create_db_with_mock_clock(&mock, ttl);

	let token = setup_user_and_login(&mut db);

	// Advance to just before TTL expires (59 seconds)
	mock.advance_secs(59);

	let result = db.auth_service().validate_token(&token);
	assert!(result.is_some(), "Token should still be valid before TTL");

	db.stop().unwrap();
}

#[test]
fn test_token_expires_after_ttl() {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let ttl = Duration::from_secs(60);
	let mut db = create_db_with_mock_clock(&mock, ttl);

	let token = setup_user_and_login(&mut db);

	// Advance past TTL (61 seconds)
	mock.advance_secs(61);

	let result = db.auth_service().validate_token(&token);
	assert!(result.is_none(), "Token should be expired after TTL");

	db.stop().unwrap();
}

#[test]
fn test_token_no_ttl_never_expires() {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let mut config = SharedRuntimeConfig::default();
	config.clock = Clock::Mock(mock.clone());
	let runtime = SharedRuntime::from_config(config);

	let mut db = embedded::memory().with_runtime(runtime).with_auth(|a| a.no_session_ttl()).build().unwrap();

	let token = setup_user_and_login(&mut db);

	// Advance by 10 years
	mock.advance_secs(10 * 365 * 24 * 60 * 60);

	let result = db.auth_service().validate_token(&token);
	assert!(result.is_some(), "Token with no TTL should never expire");

	db.stop().unwrap();
}

#[test]
fn test_cleanup_removes_expired_tokens() {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let ttl = Duration::from_secs(60);
	let mut db = create_db_with_mock_clock(&mock, ttl);

	let token = setup_user_and_login(&mut db);

	// Token valid before expiry
	assert!(db.auth_service().validate_token(&token).is_some());

	// Advance past TTL
	mock.advance_secs(61);

	// Cleanup expired tokens
	db.auth_service().cleanup_expired();

	// Token should be gone from the database entirely
	let result = db.auth_service().validate_token(&token);
	assert!(result.is_none(), "Expired token should be cleaned up");

	db.stop().unwrap();
}
