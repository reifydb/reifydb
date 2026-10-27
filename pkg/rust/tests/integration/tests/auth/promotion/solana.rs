// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use ed25519_dalek::SigningKey;
use reifydb::{IdentityId, auth::service::AuthResponse, testing::db::TestDb};
use reifydb_test_harness::auth::AuthResponseAssert;
use reifydb_value::value::identity::IdentityKind;

use super::{guest_login, name_and_kind, promotion_db};
use crate::auth::solana::{keypair, provision_credentials, sign};

fn begin(db: &TestDb, caller: IdentityId, pubkey: &str) -> (String, String) {
	match db.auth_service().authenticate_as(caller, "solana", provision_credentials(pubkey)).unwrap() {
		AuthResponse::Challenge {
			challenge_id,
			payload,
		} => (challenge_id, payload.get("message").unwrap().clone()),
		other => panic!("expected a signing challenge, got {other:?}"),
	}
}

fn complete(db: &TestDb, caller: IdentityId, key: &SigningKey, challenge_id: String, message: &str) -> AuthResponse {
	db.auth_service()
		.authenticate_as(
			caller,
			"solana",
			HashMap::from([
				("challenge_id".to_string(), challenge_id),
				("signature".to_string(), sign(key, message)),
				("signed_message".to_string(), message.to_string()),
			]),
		)
		.unwrap()
}

#[test]
fn a_challenge_is_completed_only_by_its_own_caller_or_without_one() {
	// The oauth style redirect drops the bearer, so step two may be anonymous but never another identity.
	let db = promotion_db(true);
	let (key, pubkey) = keypair(42);
	let (guest_a, _) = guest_login(&db);
	let (guest_b, _) = guest_login(&db);

	let (challenge_id, message) = begin(&db, guest_a, &pubkey);
	complete(&db, guest_b, &key, challenge_id, &message)
		.expect_failed("challenge was issued to a different caller");
	assert_eq!(name_and_kind(&db, guest_a).1, IdentityKind::Guest);

	let (challenge_id, message) = begin(&db, guest_a, &pubkey);
	let (identity, _) = complete(&db, IdentityId::anonymous(), &key, challenge_id, &message).expect_authenticated();

	assert_eq!(identity, guest_a);
	assert_eq!(name_and_kind(&db, guest_a), (pubkey, IdentityKind::User));
}
