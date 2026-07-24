// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// End-to-end guard for guest mode: a visitor gets a real identity and a session
// token without registering, owns monitors under it, and - when they finally
// register - that SAME identity is promoted in place. The whole point is that
// nothing is copied or re-owned, so these tests assert on the IdentityId itself
// and on the rows it owns surviving the promotion untouched.
//
// The real shipped code is exercised: src/guest.rs via #[path] and the real
// migrations via src/schema.rs, so an owner policy or a promotion step that
// regresses fails here rather than in production.

#[path = "../src/guest.rs"]
mod guest;
#[path = "../src/schema.rs"]
mod schema;

use std::collections::HashMap;

use reifydb::{
	Database, IdentityId, Value, WithSubsystem,
	auth::service::AuthResponse,
	server,
	value::{
		params::Params,
		value::{duration::Duration, frame::frame::Frame, into::IntoValue, uuid::Uuid7},
	},
};

use crate::guest::{PromotionError, create_guest, guest_session_ttl, promote_guest};

const PASSWORD: &str = "correct horse battery";

fn build() -> Database {
	server::memory().with_flow(|f| f).with_migrations(schema::migrations()).build().expect("build memory db")
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

fn new_guest(db: &Database) -> IdentityId {
	create_guest(&db.catalog(), db.engine(), db.clock(), db.engine().rng()).expect("create guest")
}

fn promote(db: &Database, identity: IdentityId, email: &str) -> Result<(), PromotionError> {
	promote_guest(&db.catalog(), db.engine(), db.engine().rng(), identity, email, PASSWORD.to_string())
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
	match frame.columns.iter().find(|c| c.name == column)?.data.get_value(0) {
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

fn sign_in(db: &Database, email: &str, password: &str) -> AuthResponse {
	db.auth_service()
		.authenticate(
			"password",
			HashMap::from([
				("identifier".to_string(), email.to_string()),
				("password".to_string(), password.to_string()),
			]),
		)
		.expect("authenticate")
}

#[test]
fn guest_is_a_usable_identity_before_registering() {
	// The whole premise of guest mode: no credential, yet a principal that owns
	// rows and can read them back under the owner policies.
	let db = build();
	let identity = new_guest(&db);

	let kind = identity_column(&db, identity, "kind").expect("guest kind");
	assert_eq!(kind, "guest");
	assert!(guest::is_guest_kind(&kind), "the API's guest check must agree with the stored kind");
	assert!(identity_column(&db, identity, "name").unwrap().starts_with(guest::GUEST_NAME_PREFIX));

	add_monitor(&db, identity);
	assert_eq!(owned_monitors(&db, identity), 1, "a guest must read back the monitors it owns");
}

#[test]
fn guests_are_isolated_from_each_other() {
	// Guests are per-visitor principals, not one shared anonymous id. If they
	// collapsed into one, every visitor would see every other visitor's monitors.
	let db = build();
	let first = new_guest(&db);
	let second = new_guest(&db);
	assert_ne!(first, second);

	add_monitor(&db, first);

	assert_eq!(owned_monitors(&db, first), 1);
	assert_eq!(owned_monitors(&db, second), 0, "a guest must not see another guest's monitors");
}

#[test]
fn guest_session_token_resolves_to_the_guest() {
	// The browser holds only this token; it has to identify the guest for the
	// whole session, without any credential ever being created.
	let db = build();
	let identity = new_guest(&db);

	let token = db.auth_service().create_session(identity, Some(guest_session_ttl())).expect("create session");
	let validated = db.auth_service().validate_token(&token.token).expect("token must validate");

	assert_eq!(validated.identity, identity);
	assert!(token.expires_at.is_some(), "a guest session must expire eventually");
	assert!(
		token.expires_at.unwrap().to_nanos() > db.clock().now().to_nanos(),
		"a freshly minted guest session must not already be expired"
	);
}

#[test]
fn promotion_keeps_the_identity_and_everything_it_owns() {
	// This is the contract the user asked for: registering promotes the guest in
	// place. If promotion ever created a new identity instead, the monitor would
	// be stranded on the old one and this count would drop to zero.
	let db = build();
	let identity = new_guest(&db);
	let monitor_id = add_monitor(&db, identity);

	promote(&db, identity, "guest@example.com").expect("promotion");

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

	promote(&db, identity, "promoted@example.com").expect("promotion");

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
	// Password login resolves strictly by identity name, so this is what proves
	// the rename half of the promotion actually reconnects the user to their data.
	let db = build();
	let identity = new_guest(&db);
	add_monitor(&db, identity);

	promote(&db, identity, "signin@example.com").expect("promotion");

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
	promote(&db, identity, "wrongpw@example.com").expect("promotion");

	match sign_in(&db, "wrongpw@example.com", "not the password") {
		AuthResponse::Authenticated {
			..
		} => panic!("a wrong password must not authenticate"),
		AuthResponse::Failed {
			..
		} => {}
		other => panic!("expected a failure, got {other:?}"),
	}
}

#[test]
fn promotion_onto_a_taken_email_is_rejected_and_leaves_the_guest_intact() {
	// Two guests must never be able to claim the same email; the loser has to
	// stay a usable guest rather than end up half-promoted.
	let db = build();
	let first = new_guest(&db);
	let second = new_guest(&db);
	let second_name = identity_column(&db, second, "name").expect("guest name");
	add_monitor(&db, second);

	promote(&db, first, "taken@example.com").expect("first promotion");
	match promote(&db, second, "taken@example.com") {
		Err(PromotionError::Database(err)) => {
			let message = format!("{err:?}");
			assert!(message.contains("CA_040"), "expected a name-collision error, got {message}");
		}
		other => panic!("a taken email must be rejected by the unique name index, got {other:?}"),
	}

	assert_eq!(identity_column(&db, second, "name").as_deref(), Some(second_name.as_str()));
	assert_eq!(identity_column(&db, second, "kind").as_deref(), Some("guest"));
	assert_eq!(owned_monitors(&db, second), 1, "a rejected promotion must leave the guest usable");

	match sign_in(&db, "taken@example.com", PASSWORD) {
		AuthResponse::Authenticated {
			identity,
			..
		} => assert_eq!(identity, first, "the email must still belong to the guest that claimed it first"),
		other => panic!("expected the first claimant to authenticate, got {other:?}"),
	}
}

#[test]
fn a_guest_cannot_be_promoted_twice() {
	// The second attempt carries a different password; letting it through would
	// let anyone who replays a stale guest token reset a real account's password.
	let db = build();
	let identity = new_guest(&db);
	promote(&db, identity, "once@example.com").expect("promotion");

	match promote(&db, identity, "again@example.com") {
		Err(PromotionError::NotAGuest) => {}
		other => panic!("a promoted identity must not be promotable again, got {other:?}"),
	}

	assert_eq!(identity_column(&db, identity, "name").as_deref(), Some("once@example.com"));
	match sign_in(&db, "once@example.com", PASSWORD) {
		AuthResponse::Authenticated {
			identity: signed_in,
			..
		} => assert_eq!(signed_in, identity),
		other => panic!("the original account must keep working, got {other:?}"),
	}
}

#[test]
fn a_promoted_account_cannot_be_turned_back_into_a_guest() {
	// The catalog exposes exactly one kind transition - Guest -> User - so no call
	// exists that demotes an account. This pins the half that IS callable: the
	// CATALOG refuses to promote an identity that is already a user, independently
	// of promote_guest's own pre-check in src/guest.rs, which short-circuits before
	// the catalog is ever reached. Demotion is what would make a settled account
	// eligible for the guest claim flow again - rename to an attacker-chosen email
	// plus a fresh password credential - so the account must come through this
	// attempt with its kind, its name, its credential and its rows all untouched.
	let db = build();
	let identity = new_guest(&db);
	add_monitor(&db, identity);
	promote(&db, identity, "settled@example.com").expect("promotion");

	let mut txn = db.engine().begin_admin(IdentityId::root()).expect("admin txn");
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
