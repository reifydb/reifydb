// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::HashMap;

use reifydb::{auth::service::AuthResponse, testing::db::TestDb, value::value::Value};
use reifydb_test_harness::{
	auth::{AuthResponseAssert, auth_service},
	lookup::{find_identity_by_attribute, find_identity_by_name},
};

use crate::auth::solana::{begin_challenge, complete_challenge, keypair, provision_credentials};

#[test]
fn test_failed_verification_creates_no_identity() {
	// No identity may exist until a signature verifies, otherwise anyone can squat any wallet.
	let db = TestDb::memory();
	let (_, pubkey) = keypair(41);
	let (attacker_key, _) = keypair(42);

	let service = auth_service(&db).build();
	let (challenge_id, message) = begin_challenge(&service, provision_credentials(&pubkey));

	let response = complete_challenge(&service, &attacker_key, challenge_id, &message);

	assert!(
		matches!(response, AuthResponse::Failed { .. }),
		"a signature from a key other than the claimed wallet must not authenticate, got {:?}",
		response
	);
	assert!(
		find_identity_by_name(&db, &pubkey).is_none(),
		"a wallet whose signature never verified must leave no identity behind"
	);
	assert!(
		find_identity_by_attribute(&db, "solana_public_key", &Value::Utf8(pubkey)).is_none(),
		"a wallet whose signature never verified must leave no lookup attribute behind"
	);
}

#[test]
fn test_abandoned_challenge_creates_no_identity() {
	// Issuing a challenge is unauthenticated, so it must cost the database nothing at all.
	let db = TestDb::memory();
	let (_, pubkey) = keypair(43);

	let service = auth_service(&db).build();
	let (_challenge_id, _message) = begin_challenge(&service, provision_credentials(&pubkey));

	assert!(
		find_identity_by_name(&db, &pubkey).is_none(),
		"issuing a challenge must write no identity, the signature has not been seen yet"
	);
	assert!(
		find_identity_by_attribute(&db, "solana_public_key", &Value::Utf8(pubkey)).is_none(),
		"issuing a challenge must write no lookup attribute, the signature has not been seen yet"
	);
}

#[test]
fn test_successful_provision_creates_identity_once() {
	// A repeat login must reuse the provisioned identity, never split a wallet across two.
	let db = TestDb::memory();
	let (signing_key, pubkey) = keypair(44);

	let service = auth_service(&db).build();
	let (challenge_id, message) = begin_challenge(&service, provision_credentials(&pubkey));
	let (identity, _token) =
		complete_challenge(&service, &signing_key, challenge_id, &message).expect_authenticated();

	assert_eq!(
		find_identity_by_attribute(&db, "solana_public_key", &Value::Utf8(pubkey.clone())).map(|i| i.id),
		Some(identity),
		"a verified signature must provision the identity together with its lookup attribute"
	);

	let (second_id, second_message) = begin_challenge(&service, provision_credentials(&pubkey));
	let (second_identity, _second_token) =
		complete_challenge(&service, &signing_key, second_id, &second_message).expect_authenticated();

	assert_eq!(
		second_identity, identity,
		"a repeat wallet login must resolve the provisioned identity, never provision a second one"
	);
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

	// Auto-provisioning must record the lookup attribute or a renamed identity is unreachable.
	let found = find_identity_by_attribute(&db, "solana_public_key", &Value::Utf8(pubkey));
	assert_eq!(found.map(|ident| ident.id), Some(identity));
}
