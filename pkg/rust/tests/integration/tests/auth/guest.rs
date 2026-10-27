// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Clock, IdentityId, MockClock, RuntimeConfig, catalog::find_token_by_value, embedded, testing::db::TestDb,
	transaction::transaction::Transaction,
};
use reifydb_test_harness::auth::AuthResponseAssert;
use reifydb_value::value::{datetime::DateTime, identity::IdentityKind};

const HOUR_NANOS: i64 = 3_600_000_000_000;

fn guest_db(mock: &MockClock) -> TestDb {
	let config = RuntimeConfig {
		clock: Clock::Mock(mock.clone()),
		..Default::default()
	};
	let db = TestDb::from(embedded::memory().with_runtime_config(config).build().unwrap());
	db.admin("CREATE AUTHENTICATION METHOD guest WITH { ttl: 1h }");
	db
}

fn guest_login(db: &TestDb) -> (IdentityId, String) {
	db.auth_service().authenticate("guest", HashMap::new()).unwrap().expect_authenticated()
}

#[test]
fn a_guest_login_creates_a_guest_identity_and_its_session() {
	// Kind, name shape and session lifetime are what apps and later promotion rely on.
	let mock = MockClock::from_millis(1_700_000_000_000);
	let db = guest_db(&mock);
	let issued = mock.now();

	let (identity, token) = guest_login(&db);

	let mut txn = db.engine().begin_query(IdentityId::system()).unwrap();
	let guest = db.engine().catalog().find_identity(&mut Transaction::Query(&mut txn), identity).unwrap().unwrap();
	assert_eq!(guest.resolved_kind(), IdentityKind::Guest);
	let suffix = guest.name.strip_prefix("guest:").expect("a guest is named guest:<uuid7>");
	assert_eq!(suffix.len(), 36, "{}", guest.name);
	assert_eq!(&suffix[14..15], "7", "the suffix must be a version 7 uuid: {}", guest.name);

	let row = find_token_by_value(&mut Transaction::Query(&mut txn), &token).unwrap().expect("the session row");
	assert_eq!(row.identity, identity);
	assert_eq!(row.expires_at, Some(DateTime::from_nanos(issued.to_nanos() + HOUR_NANOS)));
}

#[test]
fn every_guest_login_is_a_new_identity() {
	let mock = MockClock::from_millis(1_700_000_000_000);
	let db = guest_db(&mock);

	let (first, _) = guest_login(&db);
	let (second, _) = guest_login(&db);
	assert_ne!(first, second);
}

#[test]
fn an_undeclared_guest_method_is_not_enabled() {
	let db = TestDb::memory();
	db.auth_service().authenticate("guest", HashMap::new()).unwrap().expect_method_not_enabled("guest");
}

#[test]
fn a_guest_login_with_credentials_is_refused() {
	// Credentials on a guest login are a client bug and must not be silently ignored.
	let mock = MockClock::from_millis(1_700_000_000_000);
	let db = guest_db(&mock);

	let err = db
		.auth_service()
		.authenticate("guest", HashMap::from([("identifier".to_string(), "alice".to_string())]))
		.unwrap_err();
	assert_eq!(err.diagnostic().code, "AU_018");
}

#[test]
fn a_guest_credential_cannot_be_created() {
	// Guests hold no credential, which is what lets the guest method be dropped while guests exist.
	let mock = MockClock::from_millis(1_700_000_000_000);
	let db = guest_db(&mock);
	db.admin("CREATE USER alice");

	let err = db.try_admin("CREATE AUTHENTICATION FOR alice { method: guest }").unwrap_err();
	assert_eq!(err.diagnostic().code, "AU_019");
	assert_eq!(db.row_count("FROM system::authentication::credentials"), 0);
}

#[test]
fn the_guest_method_cannot_promote_guests() {
	let db = TestDb::memory();
	let err = db.try_admin("CREATE AUTHENTICATION METHOD guest WITH { ttl: 1h, promote_guest: true }").unwrap_err();
	assert_eq!(err.diagnostic().code, "CA_121");
}
