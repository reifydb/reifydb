// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{
	Database, IdentityId, Value, WithSubsystem,
	auth::service::AuthResponse,
	core::durability::Durability,
	server,
	value::{
		params::Params,
		value::{duration::Duration, frame::frame::Frame, into::IntoValue, uuid::Uuid7},
	},
};
use reifydb_uptime::migration_path;

const PASSWORD: &str = "correct horse battery";
const THIRTY_DAYS_NANOS: i128 = 30 * 24 * 60 * 60 * 1_000_000_000;

fn build() -> Database {
	server::memory().with_flow(|f| f).with_migrations(migration_path()).build().expect("build memory db")
}

fn params(entries: &[(&str, Value)]) -> Params {
	let map: HashMap<String, Value> = entries.iter().map(|(k, v)| ((*k).to_string(), v.clone())).collect();
	Params::from(map)
}

fn query_as(db: &Database, id: IdentityId, rql: &str, params: Params) -> Result<Vec<Frame>, String> {
	let r = db.engine().query_as(id, rql, params);
	match r.error {
		Some(e) => Err(format!("{e:?}")),
		None => Ok(r.frames),
	}
}

fn root_cmd(db: &Database, rql: &str, params: Params) {
	let r = db.engine().command_as(IdentityId::root(), rql, params);
	if let Some(e) = r.error {
		panic!("root command failed for [{rql}]: {e:?}");
	}
}

fn rows(frames: &[Frame]) -> usize {
	frames.first().map(Frame::row_count).unwrap_or(0)
}

fn guest_login(db: &Database) -> (IdentityId, String) {
	match db.auth_service().authenticate("guest", HashMap::new()).expect("guest login") {
		AuthResponse::Authenticated {
			identity,
			token,
		} => (identity, token),
		other => panic!("the guest method must be enabled by the uptime migrations, got {other:?}"),
	}
}

fn new_guest(db: &Database) -> IdentityId {
	guest_login(db).0
}

fn password_login(db: &Database, caller: IdentityId, email: &str, password: &str) -> AuthResponse {
	db.auth_service()
		.authenticate_as(
			caller,
			"password",
			HashMap::from([
				("identifier".to_string(), email.to_string()),
				("password".to_string(), password.to_string()),
			]),
		)
		.expect("authenticate")
}

fn promote(db: &Database, guest: IdentityId, email: &str) {
	match password_login(db, guest, email, PASSWORD) {
		AuthResponse::Authenticated {
			identity,
			..
		} => assert_eq!(identity, guest, "signup from a guest session must promote that guest in place"),
		other => panic!("expected the guest to be promoted, got {other:?}"),
	}
}

fn sign_in(db: &Database, email: &str, password: &str) -> AuthResponse {
	password_login(db, IdentityId::anonymous(), email, password)
}

fn identity_column(db: &Database, identity: IdentityId, column: &str) -> Option<String> {
	let frames = query_as(
		db,
		IdentityId::root(),
		&format!("from system::identities filter {{ id == $id }} map {{ {column} }}"),
		params(&[("id", identity.into_value())]),
	)
	.expect("identity lookup");
	let frame = frames.first()?;
	if frame.row_count() == 0 {
		return None;
	}
	match frame.column(column).expect("the column reads")?.get_value(0) {
		Value::Utf8(v) => Some(v.to_string()),
		other => panic!("unexpected {column} value: {other:?}"),
	}
}

fn identity_exists_with_name(db: &Database, name: &str) -> bool {
	let frames = query_as(
		db,
		IdentityId::root(),
		"from system::identities filter { name == $name } map { id }",
		params(&[("name", Value::Utf8(name.to_string()))]),
	)
	.expect("identity lookup");
	rows(&frames) > 0
}

fn add_monitor(db: &Database, owner: IdentityId) -> Uuid7 {
	let monitor_id = Uuid7::generate(db.clock(), db.engine().rng());
	let now = db.clock().now();
	root_cmd(
		db,
		"INSERT uptime::monitors [{ id: $id, owner: $owner, name: \"m\", kind: \"http\", target: \"http://x\", \
		 interval: $iv, timeout: $iv, http_method: none, expected_status: none, keyword: none, expected_ip: none, \
		 failure_threshold: 2, enabled: true, created_at: $now, last_checked_at: none, consecutive_failures: 0, \
		 status: \"unknown\" }]",
		params(&[
			("id", monitor_id.into_value()),
			("owner", owner.into_value()),
			("iv", Duration::from_seconds(30).unwrap().into_value()),
			("now", now.into_value()),
		]),
	);
	monitor_id
}

fn owned_monitors(db: &Database, identity: IdentityId) -> usize {
	rows(&query_as(db, identity, "from uptime::monitors", Params::None).expect("monitor read"))
}

fn assert_still_a_guest(db: &Database, guest: IdentityId, name: &str, monitors: usize) {
	assert_eq!(identity_column(db, guest, "name").as_deref(), Some(name));
	assert_eq!(identity_column(db, guest, "kind").as_deref(), Some("guest"));
	assert_eq!(owned_monitors(db, guest), monitors, "the guest must keep everything it owns");
}

#[test]
fn guest_is_a_usable_identity_before_registering() {
	// The premise of guest mode: no credential, yet a principal that owns rows and reads them back under the owner
	// policies.
	let db = build();
	let identity = new_guest(&db);

	assert_eq!(identity_column(&db, identity, "kind").as_deref(), Some("guest"));
	assert!(identity_column(&db, identity, "name").unwrap().starts_with("guest:"));

	add_monitor(&db, identity);
	assert_eq!(owned_monitors(&db, identity), 1, "a guest must read back the monitors it owns");
}

#[test]
fn guests_are_isolated_from_each_other() {
	// If guests collapsed into one shared principal, every visitor would see every other visitor's monitors.
	let db = build();
	let first = new_guest(&db);
	let second = new_guest(&db);
	assert_ne!(first, second);

	add_monitor(&db, first);

	assert_eq!(owned_monitors(&db, first), 1);
	assert_eq!(owned_monitors(&db, second), 0, "a guest must not see another guest's monitors");
}

#[test]
fn guest_session_token_resolves_to_the_guest_for_thirty_days() {
	// The browser holds only this token and keeps it for 30 days, so the server session must last exactly as long.
	let db = build();
	let (identity, token) = guest_login(&db);

	let validated = db.auth_service().validate_token(&token).unwrap().expect("token must validate");
	assert_eq!(validated.identity, identity);

	let remaining = validated.expires_at.expect("a guest session must expire").to_nanos() as i128
		- db.clock().now().to_nanos() as i128;
	assert!(
		remaining > THIRTY_DAYS_NANOS - 60_000_000_000 && remaining <= THIRTY_DAYS_NANOS,
		"the guest session must last the 30 days the web client assumes, got {remaining}ns"
	);
}

#[test]
fn promotion_keeps_the_identity_and_everything_it_owns() {
	// If promotion created a new identity, the monitor would be stranded on the old one and this count would drop
	// to zero.
	let db = build();
	let identity = new_guest(&db);
	let monitor_id = add_monitor(&db, identity);

	promote(&db, identity, "guest@example.com");

	assert_eq!(owned_monitors(&db, identity), 1, "monitors must survive promotion untouched");
	let frames = query_as(
		&db,
		identity,
		"from uptime::monitors filter { id == $id } map { id, owner }",
		params(&[("id", monitor_id.into_value())]),
	)
	.expect("monitor read");
	assert_eq!(rows(&frames), 1, "the promoted identity must still own the same monitor row");
}

#[test]
fn promotion_turns_the_guest_into_a_full_user() {
	let db = build();
	let identity = new_guest(&db);
	let guest_name = identity_column(&db, identity, "name").expect("guest name");

	promote(&db, identity, "promoted@example.com");

	assert_eq!(identity_column(&db, identity, "name").as_deref(), Some("promoted@example.com"));
	assert_eq!(
		identity_column(&db, identity, "kind").as_deref(),
		Some("user"),
		"a promoted guest must stop matching guest-scoped policies"
	);
	assert!(!identity_exists_with_name(&db, &guest_name), "the guest name must not keep resolving after promotion");
}

#[test]
fn promoted_guest_can_sign_in_with_the_new_password() {
	// Password login resolves by identity name, so this proves the rename reconnects the user to their data.
	let db = build();
	let identity = new_guest(&db);
	add_monitor(&db, identity);

	promote(&db, identity, "signin@example.com");

	match sign_in(&db, "signin@example.com", PASSWORD) {
		AuthResponse::Authenticated {
			identity: signed_in,
			..
		} => {
			assert_eq!(signed_in, identity, "signing in must land on the very identity that was promoted");
			assert_eq!(
				owned_monitors(&db, signed_in),
				1,
				"the signed-in user must see the guest's monitors"
			);
		}
		other => panic!("expected authentication to succeed, got {other:?}"),
	}
}

#[test]
fn promoted_guest_rejects_a_wrong_password() {
	let db = build();
	let identity = new_guest(&db);
	promote(&db, identity, "wrongpw@example.com");

	match sign_in(&db, "wrongpw@example.com", "not the password") {
		AuthResponse::Failed {
			..
		} => {}
		other => panic!("a wrong password must fail, got {other:?}"),
	}
}

#[test]
fn a_guest_signing_up_with_a_taken_email_logs_in_with_the_right_password() {
	// The register page relies on this: a correct password for an existing account is a plain login, and the guest
	// is left alone.
	let db = build();
	let first = new_guest(&db);
	let second = new_guest(&db);
	let second_name = identity_column(&db, second, "name").expect("guest name");
	add_monitor(&db, second);
	promote(&db, first, "taken@example.com");

	match password_login(&db, second, "taken@example.com", PASSWORD) {
		AuthResponse::Authenticated {
			identity,
			..
		} => assert_eq!(identity, first, "the email must still belong to the account that claimed it first"),
		other => panic!("expected a plain login as the existing account, got {other:?}"),
	}

	assert_still_a_guest(&db, second, &second_name, 1);
}

#[test]
fn a_guest_signing_up_with_a_taken_email_and_a_wrong_password_is_refused() {
	// The register page maps exactly this reason to "this email already has an account"; the guest must not be
	// half-promoted.
	let db = build();
	let first = new_guest(&db);
	let second = new_guest(&db);
	let second_name = identity_column(&db, second, "name").expect("guest name");
	add_monitor(&db, second);
	promote(&db, first, "taken@example.com");

	match password_login(&db, second, "taken@example.com", "someone else's guess") {
		AuthResponse::Failed {
			reason,
		} => assert_eq!(reason, "invalid credentials"),
		other => panic!("a taken email with a wrong password must fail, got {other:?}"),
	}

	assert_still_a_guest(&db, second, &second_name, 1);
	assert_eq!(identity_column(&db, first, "name").as_deref(), Some("taken@example.com"));
}

#[test]
fn a_signup_from_a_promoted_account_creates_a_separate_user() {
	// Only a guest caller is promoted; a replayed session of a settled account must never rename it or reset its
	// password.
	let db = build();
	let identity = new_guest(&db);
	add_monitor(&db, identity);
	promote(&db, identity, "once@example.com");

	let other = match password_login(&db, identity, "again@example.com", "another password") {
		AuthResponse::Authenticated {
			identity: other,
			..
		} => other,
		other => panic!("a new email must sign up a new user, got {other:?}"),
	};

	assert_ne!(other, identity, "the settled account must not be reused for the new email");
	assert_eq!(identity_column(&db, other, "kind").as_deref(), Some("user"));
	assert_eq!(owned_monitors(&db, other), 0, "the new user must not see the account's monitors");
	assert_eq!(identity_column(&db, identity, "name").as_deref(), Some("once@example.com"));
	assert_eq!(owned_monitors(&db, identity), 1);
	match sign_in(&db, "once@example.com", PASSWORD) {
		AuthResponse::Authenticated {
			identity: signed_in,
			..
		} => assert_eq!(signed_in, identity, "the original account must keep its credential"),
		other => panic!("the original account must keep working, got {other:?}"),
	}
}

#[test]
fn a_promoted_account_cannot_be_turned_back_into_a_guest() {
	// Demotion would make a settled account claimable again, so the catalog must refuse to promote an identity that
	// is already a user.
	let db = build();
	let identity = new_guest(&db);
	add_monitor(&db, identity);
	promote(&db, identity, "settled@example.com");

	let mut txn = db.engine().begin_admin(IdentityId::root(), Durability::Async).expect("admin txn");
	let err = db.catalog().promote_guest_to_user(&mut txn, identity).expect_err("must be refused");
	assert_eq!(err.diagnostic().code, "CA_095");
	txn.rollback().expect("rollback");

	assert_eq!(identity_column(&db, identity, "kind").as_deref(), Some("user"));
	assert_eq!(identity_column(&db, identity, "name").as_deref(), Some("settled@example.com"));
	assert_eq!(owned_monitors(&db, identity), 1, "the account must keep everything it owns");
	match sign_in(&db, "settled@example.com", PASSWORD) {
		AuthResponse::Authenticated {
			identity: signed_in,
			..
		} => assert_eq!(signed_in, identity, "the original credential must still resolve to the account"),
		other => panic!("the account must keep working, got {other:?}"),
	}
}
