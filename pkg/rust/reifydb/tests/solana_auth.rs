// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use bs58::encode as bs58_encode;
use ed25519_dalek::{Signer, SigningKey};
use reifydb::{
	auth::service::{AuthResponse, AuthService},
	value::value::Value,
};
use reifydb_test_harness::{
	auth::{AuthResponseAssert, auth_service},
	db::TestDb,
	fixture::identity::identity,
	lookup::find_identity_by_attribute,
};

fn keypair(seed: u8) -> (SigningKey, String) {
	let signing_key = SigningKey::from_bytes(&[seed; 32]);
	let pubkey = bs58_encode(signing_key.verifying_key().as_bytes()).into_string();
	(signing_key, pubkey)
}

fn begin_challenge(service: &AuthService, credentials: HashMap<String, String>) -> (String, String) {
	match service.authenticate("solana", credentials).unwrap() {
		AuthResponse::Challenge {
			challenge_id,
			payload,
		} => (challenge_id, payload.get("message").unwrap().clone()),
		other => panic!("expected a signing challenge, got {:?}", other),
	}
}

fn complete_challenge(
	service: &AuthService,
	signing_key: &SigningKey,
	challenge_id: String,
	message: &str,
) -> AuthResponse {
	let signature = signing_key.sign(message.as_bytes());
	service.authenticate(
		"solana",
		HashMap::from([
			("challenge_id".to_string(), challenge_id),
			("signature".to_string(), bs58_encode(signature.to_bytes()).into_string()),
			("signed_message".to_string(), message.to_string()),
		]),
	)
	.unwrap()
}

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

	let (identity, _token) = complete_challenge(&service, &signing_key, challenge_id, &message).expect_authenticated();
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

	let (identity, _token) = complete_challenge(&service, &signing_key, challenge_id, &message).expect_authenticated();

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
