// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{auth::service::AuthResponse, testing::db::TestDb, value::value::Value};
use reifydb_test_harness::{
	auth::{AuthResponseAssert, auth_service},
	fixture::identity::identity,
	lookup::find_identity_by_attribute,
};

use crate::auth::solana::{begin_challenge, complete_challenge, keypair};

#[test]
fn test_login_resolves_identity_by_public_key_attribute() {
	let db = TestDb::memory();
	let (signing_key, pubkey) = keypair(7);

	// The identity is named "alice", NOT the wallet address, so name lookup can never
	// find it; resolution must go through the solana_public_key attribute that the
	// harness writes exactly as the auth service would.
	let alice = identity("alice").solana_key(&pubkey).create(&db);

	let service = auth_service(&db).build();
	let (challenge_id, message) =
		begin_challenge(&service, HashMap::from([("identifier".to_string(), pubkey.clone())]));

	let (identity, _token) =
		complete_challenge(&service, &signing_key, challenge_id, &message).expect_authenticated();
	assert_eq!(identity, alice.id, "wallet login must authenticate as alice via the attribute lookup");
}

#[test]
fn test_auto_provision_writes_public_key_attribute() {
	let db = TestDb::memory();
	let (signing_key, pubkey) = keypair(9);

	let service = auth_service(&db).build();
	let (challenge_id, message) = begin_challenge(
		&service,
		HashMap::from([("identifier".to_string(), pubkey.clone()), ("public_key".to_string(), pubkey.clone())]),
	);

	let (identity, _token) =
		complete_challenge(&service, &signing_key, challenge_id, &message).expect_authenticated();

	// Auto-provisioning must record the lookup attribute, otherwise identities whose
	// name diverges from the wallet address become unreachable on the next login.
	let found = find_identity_by_attribute(&db, "solana_public_key", &Value::Utf8(pubkey));
	assert_eq!(found.map(|ident| ident.id), Some(identity));
}

#[test]
fn test_unknown_public_key_without_provisioning_credentials_fails() {
	let db = TestDb::memory();
	let (_, pubkey) = keypair(11);

	let service = auth_service(&db).build();

	// Without a public_key credential there is nothing to auto-provision from, and no
	// identity carries this attribute value: the login must fail closed instead of
	// issuing a challenge for a nonexistent identity.
	let response = service.authenticate("solana", HashMap::from([("identifier".to_string(), pubkey)])).unwrap();
	assert!(matches!(response, AuthResponse::Failed { .. }), "expected failure, got {:?}", response);
}
