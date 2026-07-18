// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use bs58::encode as bs58_encode;
use ed25519_dalek::{Signer, SigningKey};
use reifydb::{
	Clock, Database, IdentityId,
	auth::{
		method::solana::SolanaProvider,
		registry::AuthenticationRegistry,
		service::{AuthConfigurator, AuthResponse, AuthService},
	},
	core::interface::auth::AuthenticationProvider,
	embedded,
	runtime::context::rng::Rng,
	transaction::transaction::Transaction,
	value::value::{Value, value_type::ValueType},
};

fn keypair(seed: u8) -> (SigningKey, String) {
	let signing_key = SigningKey::from_bytes(&[seed; 32]);
	let pubkey = bs58_encode(signing_key.verifying_key().as_bytes()).into_string();
	(signing_key, pubkey)
}

fn auth_service(db: &Database) -> AuthService {
	AuthService::new(
		Arc::new(db.engine().clone()),
		Arc::new(AuthenticationRegistry::default()),
		Rng::seeded(42),
		Clock::Real,
		AuthConfigurator::new().configure(),
	)
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
	let db = embedded::memory().build().unwrap();
	let (signing_key, pubkey) = keypair(7);
	let clock = Clock::Real;
	let rng = Rng::seeded(1);

	// The identity is named "alice", NOT the wallet address, so name lookup can
	// never find it; resolution must go through the solana_public_key attribute.
	let engine = db.engine();
	let mut admin = engine.begin_admin(IdentityId::root()).unwrap();
	let catalog = engine.catalog();
	let alice = catalog.create_identity(&mut admin, "alice", &clock, &rng).unwrap();
	let properties = SolanaProvider::new(clock.clone())
		.create(&rng, &HashMap::from([("public_key".to_string(), pubkey.clone())]))
		.unwrap();
	catalog.create_authentication(&mut admin, alice.id, "solana", properties).unwrap();
	let attribute = catalog.create_identity_attribute(&mut admin, "solana_public_key", ValueType::Utf8).unwrap();
	catalog.set_identity_attribute_value(&mut admin, alice.id, &attribute, Value::Utf8(pubkey.clone())).unwrap();
	admin.commit().unwrap();

	let service = auth_service(&db);
	let (challenge_id, message) =
		begin_challenge(&service, HashMap::from([("identifier".to_string(), pubkey.clone())]));

	let response = complete_challenge(&service, &signing_key, challenge_id, &message);
	let AuthResponse::Authenticated {
		identity,
		..
	} = response
	else {
		panic!("expected wallet login to authenticate via the attribute lookup, got {:?}", response);
	};
	assert_eq!(identity, alice.id);
}

#[test]
fn test_auto_provision_writes_public_key_attribute() {
	let db = embedded::memory().build().unwrap();
	let (signing_key, pubkey) = keypair(9);

	let service = auth_service(&db);
	let (challenge_id, message) = begin_challenge(
		&service,
		HashMap::from([("identifier".to_string(), pubkey.clone()), ("public_key".to_string(), pubkey.clone())]),
	);

	let response = complete_challenge(&service, &signing_key, challenge_id, &message);
	let AuthResponse::Authenticated {
		identity,
		..
	} = response
	else {
		panic!("expected first wallet login to auto-provision and authenticate, got {:?}", response);
	};

	// Auto-provisioning must record the lookup attribute, otherwise identities
	// whose name diverges from the wallet address become unreachable.
	let engine = db.engine();
	let mut txn = engine.begin_query(IdentityId::root()).unwrap();
	let found = engine
		.catalog()
		.find_identity_by_attribute_value(
			&mut Transaction::Query(&mut txn),
			"solana_public_key",
			&Value::Utf8(pubkey),
		)
		.unwrap();
	assert_eq!(found.map(|ident| ident.id), Some(identity));
}

#[test]
fn test_unknown_public_key_without_provisioning_credentials_fails() {
	let db = embedded::memory().build().unwrap();
	let (_, pubkey) = keypair(11);

	let service = auth_service(&db);

	// Without a public_key credential there is nothing to auto-provision from, and
	// no identity carries this attribute value: the login must fail closed instead
	// of issuing a challenge for a nonexistent identity.
	let response = service.authenticate("solana", HashMap::from([("identifier".to_string(), pubkey)])).unwrap();
	assert!(matches!(response, AuthResponse::Failed { .. }), "expected failure, got {:?}", response);
}
