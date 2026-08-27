// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

mod challenge;
mod login;
mod provisioning;

use std::collections::HashMap;

use bs58::encode as bs58_encode;
use ed25519_dalek::{Signer, SigningKey};
use reifydb::auth::service::{AuthResponse, AuthService};

pub fn keypair(seed: u8) -> (SigningKey, String) {
	let signing_key = SigningKey::from_bytes(&[seed; 32]);
	let pubkey = bs58_encode(signing_key.verifying_key().as_bytes()).into_string();
	(signing_key, pubkey)
}

pub fn begin_challenge(service: &AuthService, credentials: HashMap<String, String>) -> (String, String) {
	match service.authenticate("solana", credentials).unwrap() {
		AuthResponse::Challenge {
			challenge_id,
			payload,
		} => (challenge_id, payload.get("message").unwrap().clone()),
		other => panic!("expected a signing challenge, got {:?}", other),
	}
}

pub fn complete_challenge(
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

pub fn sign(signing_key: &SigningKey, text: &str) -> String {
	bs58_encode(signing_key.sign(text.as_bytes()).to_bytes()).into_string()
}

pub fn submit(service: &AuthService, credentials: HashMap<String, String>) -> AuthResponse {
	// Deliberately unwrapped: a wallet login must answer with a typed AuthResponse, never an
	// Err, or a caller cannot tell "rejected" from "server broke".
	service.authenticate("solana", credentials).unwrap()
}

pub fn provision_credentials(public_key: &str) -> HashMap<String, String> {
	// Both keys are required to reach auto-provisioning; identifier names the not-yet-existing identity.
	HashMap::from([
		("identifier".to_string(), public_key.to_string()),
		("public_key".to_string(), public_key.to_string()),
	])
}
