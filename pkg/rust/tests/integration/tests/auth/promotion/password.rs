// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb::IdentityId;
use reifydb_test_harness::auth::AuthResponseAssert;
use reifydb_value::value::identity::IdentityKind;

use super::{guest_login, name_and_kind, password_login, promotion_db, session_of};

#[test]
fn a_guest_signing_up_keeps_its_identity() {
	// Keeping the id is what carries everything the guest created over to the new account.
	let db = promotion_db(true);
	let (guest, _) = guest_login(&db);

	let (identity, token) = password_login(&db, guest, "alice@x", "pw").expect_authenticated();

	assert_eq!(identity, guest);
	assert_eq!(name_and_kind(&db, guest), ("alice@x".to_string(), IdentityKind::User));
	assert_eq!(session_of(&db, &token), Some(guest));
}

#[test]
fn a_promoted_account_is_never_promoted_again() {
	let db = promotion_db(true);
	let (guest, _) = guest_login(&db);
	let (promoted, _) = password_login(&db, guest, "alice@x", "pw").expect_authenticated();

	let (other, _) = password_login(&db, promoted, "carol@x", "pw").expect_authenticated();

	assert_ne!(other, promoted, "a user caller signing up must get a new account, not rename itself");
	assert_eq!(name_and_kind(&db, promoted).0, "alice@x");
}

#[test]
fn the_guest_session_dies_with_the_guest() {
	// A surviving guest token would let a second client keep acting as the now named account.
	let db = promotion_db(true);
	let (guest, guest_token) = guest_login(&db);

	let (_, token) = password_login(&db, guest, "alice@x", "pw").expect_authenticated();

	assert_eq!(session_of(&db, &guest_token), None);
	assert_eq!(session_of(&db, &token), Some(guest));
}

#[test]
fn an_anonymous_signup_creates_a_new_user() {
	let db = promotion_db(true);

	let (identity, _) = password_login(&db, IdentityId::anonymous(), "dave@x", "pw").expect_authenticated();

	assert_eq!(name_and_kind(&db, identity), ("dave@x".to_string(), IdentityKind::User));
}

#[test]
fn without_promote_guest_an_unknown_identifier_is_invalid_and_the_guest_untouched() {
	let db = promotion_db(false);
	let (guest, guest_token) = guest_login(&db);
	let guest_name = name_and_kind(&db, guest).0;

	password_login(&db, guest, "alice@x", "pw").expect_failed("invalid credentials");

	assert_eq!(name_and_kind(&db, guest), (guest_name, IdentityKind::Guest));
	assert_eq!(session_of(&db, &guest_token), Some(guest));
}

#[test]
fn a_guest_logging_into_an_existing_account_just_logs_in() {
	// The existing account must never absorb or rename the guest.
	let db = promotion_db(true);
	let (alice, _) = password_login(&db, IdentityId::anonymous(), "alice@x", "pw").expect_authenticated();
	let (guest, guest_token) = guest_login(&db);
	let guest_name = name_and_kind(&db, guest).0;

	let (identity, _) = password_login(&db, guest, "alice@x", "pw").expect_authenticated();

	assert_eq!(identity, alice);
	assert_eq!(name_and_kind(&db, guest), (guest_name, IdentityKind::Guest));
	assert_eq!(session_of(&db, &guest_token), Some(guest));
}
