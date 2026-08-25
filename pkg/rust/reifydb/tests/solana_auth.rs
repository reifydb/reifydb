// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use bs58::encode as bs58_encode;
use ed25519_dalek::{Signer, SigningKey};
use reifydb::{
	auth::service::{AuthResponse, AuthService},
	testing::db::TestDb,
	value::value::Value,
};
use reifydb_test_harness::{
	auth::{AuthResponseAssert, auth_service},
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

fn sign(signing_key: &SigningKey, text: &str) -> String {
	bs58_encode(signing_key.sign(text.as_bytes()).to_bytes()).into_string()
}

fn submit(service: &AuthService, credentials: HashMap<String, String>) -> AuthResponse {
	// Deliberately unwrapped: a wallet login must answer with a typed AuthResponse, never an
	// Err, or a caller cannot tell "rejected" from "server broke".
	service.authenticate("solana", credentials).unwrap()
}

#[test]
fn test_signature_over_attacker_chosen_message_is_rejected() {
	// Pins the core bypass: the bytes that get verified must be the server's challenge text,
	// not text the client picked. If the client chooses them, any signature that wallet ever
	// produced anywhere becomes a permanent password for this identity.
	let db = TestDb::memory();
	let (signing_key, pubkey) = keypair(21);
	identity("mallory").solana_key(&pubkey).create(&db);

	let service = auth_service(&db).build();
	let (challenge_id, _server_message) =
		begin_challenge(&service, HashMap::from([("identifier".to_string(), pubkey.clone())]));

	let attacker_text = "gm";
	let response = submit(
		&service,
		HashMap::from([
			("challenge_id".to_string(), challenge_id),
			("signature".to_string(), sign(&signing_key, attacker_text)),
			("signed_message".to_string(), attacker_text.to_string()),
		]),
	);

	assert!(
		matches!(response, AuthResponse::Failed { .. }),
		"a signature over client-chosen text must never authenticate, got {:?}",
		response
	);
}

#[test]
fn test_signature_from_another_challenge_is_rejected() {
	// Each challenge must only accept its own signature. Without that binding a single captured
	// signature is replayable against every later challenge the same wallet is issued.
	let db = TestDb::memory();
	let (signing_key, pubkey) = keypair(22);
	identity("carol").solana_key(&pubkey).create(&db);

	let service = auth_service(&db).build();
	let (_challenge_a, message_a) =
		begin_challenge(&service, HashMap::from([("identifier".to_string(), pubkey.clone())]));
	let (challenge_b, message_b) =
		begin_challenge(&service, HashMap::from([("identifier".to_string(), pubkey.clone())]));
	assert_ne!(message_a, message_b, "two challenges must carry distinct nonces or the test proves nothing");

	let response = submit(
		&service,
		HashMap::from([
			("challenge_id".to_string(), challenge_b),
			("signature".to_string(), sign(&signing_key, &message_a)),
			("signed_message".to_string(), message_a),
		]),
	);

	assert!(
		matches!(response, AuthResponse::Failed { .. }),
		"a signature issued for a different challenge must not authenticate, got {:?}",
		response
	);
}

#[test]
fn test_signature_without_challenge_id_is_rejected() {
	// The challenge step must not be skippable. A caller that jumps straight to verification has
	// no server nonce bound to it, so verification there can only ever be replay.
	let db = TestDb::memory();
	let (signing_key, pubkey) = keypair(23);
	identity("dave").solana_key(&pubkey).create(&db);

	let service = auth_service(&db).build();
	let response = submit(
		&service,
		HashMap::from([
			("identifier".to_string(), pubkey.clone()),
			("signature".to_string(), sign(&signing_key, "anything")),
			("signed_message".to_string(), "anything".to_string()),
		]),
	);

	assert!(
		matches!(response, AuthResponse::Challenge { .. }),
		"a signature with no challenge_id must fall through to a fresh challenge, got {:?}",
		response
	);
}

#[test]
fn test_consumed_challenge_cannot_be_replayed() {
	// A challenge is single use. If a completed one stayed live, a captured completion could be
	// replayed for as long as the entry survived.
	let db = TestDb::memory();
	let (signing_key, pubkey) = keypair(24);
	identity("erin").solana_key(&pubkey).create(&db);

	let service = auth_service(&db).build();
	let (challenge_id, message) =
		begin_challenge(&service, HashMap::from([("identifier".to_string(), pubkey.clone())]));

	let completion = HashMap::from([
		("challenge_id".to_string(), challenge_id),
		("signature".to_string(), sign(&signing_key, &message)),
		("signed_message".to_string(), message.clone()),
	]);

	submit(&service, completion.clone()).expect_authenticated();
	submit(&service, completion).expect_failed("invalid or expired challenge");
}

#[test]
fn test_absent_signed_message_still_verifies() {
	// signed_message stays optional on the wire: the server already knows the text it issued, so
	// a client that omits it must still authenticate rather than be forced to echo it back.
	let db = TestDb::memory();
	let (signing_key, pubkey) = keypair(25);
	let frank = identity("frank").solana_key(&pubkey).create(&db);

	let service = auth_service(&db).build();
	let (challenge_id, message) =
		begin_challenge(&service, HashMap::from([("identifier".to_string(), pubkey.clone())]));

	let (authenticated, _token) = submit(
		&service,
		HashMap::from([
			("challenge_id".to_string(), challenge_id),
			("signature".to_string(), sign(&signing_key, &message)),
		]),
	)
	.expect_authenticated();

	assert_eq!(authenticated, frank.id, "omitting signed_message must not change which identity is resolved");
}

#[test]
fn test_mismatched_signed_message_is_rejected_distinctly() {
	// When the client does echo signed_message it must equal the issued text. The rejection needs
	// its own reason: collapsing it into "invalid credentials" hides a client that is signing the
	// wrong bytes behind what looks like a plain bad-password failure.
	let db = TestDb::memory();
	let (signing_key, pubkey) = keypair(26);
	identity("grace").solana_key(&pubkey).create(&db);

	let service = auth_service(&db).build();
	let (challenge_id, message) =
		begin_challenge(&service, HashMap::from([("identifier".to_string(), pubkey.clone())]));

	let response = submit(
		&service,
		HashMap::from([
			("challenge_id".to_string(), challenge_id),
			("signature".to_string(), sign(&signing_key, &message)),
			("signed_message".to_string(), "not the challenge".to_string()),
		]),
	);

	match response {
		AuthResponse::Failed {
			reason,
		} => assert_ne!(
			reason, "invalid credentials",
			"a signed_message mismatch must report its own reason, not the generic credential failure"
		),
		other => panic!("expected a failure for a mismatched signed_message, got {:?}", other),
	}
}

#[test]
fn test_client_supplied_message_cannot_override_challenge_payload() {
	// Stored challenge keys must win over anything the client sends under the same name. If client
	// credentials could shadow the payload, the attacker supplies both the message and the
	// signature over it and the nonce binding is gone.
	let db = TestDb::memory();
	let (signing_key, pubkey) = keypair(27);
	identity("heidi").solana_key(&pubkey).create(&db);

	let service = auth_service(&db).build();
	let (challenge_id, _message) =
		begin_challenge(&service, HashMap::from([("identifier".to_string(), pubkey.clone())]));

	let attacker_text = "attacker chosen message";
	let response = submit(
		&service,
		HashMap::from([
			("challenge_id".to_string(), challenge_id),
			("signature".to_string(), sign(&signing_key, attacker_text)),
			("signed_message".to_string(), attacker_text.to_string()),
			("message".to_string(), attacker_text.to_string()),
		]),
	);

	assert!(
		matches!(response, AuthResponse::Failed { .. }),
		"a client-supplied message key must not displace the stored challenge text, got {:?}",
		response
	);
}
