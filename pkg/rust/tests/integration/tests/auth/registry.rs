// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, sync::Arc};

use reifydb::{
	IdentityId,
	core::interface::auth::{AuthStep, AuthenticationProvider},
	embedded,
	runtime::context::rng::Rng,
	testing::db::TestDb,
	value::error::{Diagnostic, Error},
};
use reifydb_test_harness::{auth::AuthResponseAssert, engine::AsEngine};

struct PinProvider;

impl AuthenticationProvider for PinProvider {
	fn method(&self) -> &str {
		"pin"
	}

	fn create(&self, _rng: &Rng, config: &HashMap<String, String>) -> Result<HashMap<String, String>, Error> {
		let pin = config.get("pin").ok_or_else(pin_required)?;
		Ok(HashMap::from([("pin".to_string(), pin.clone())]))
	}

	fn authenticate(
		&self,
		stored: &HashMap<String, String>,
		credentials: &HashMap<String, String>,
	) -> Result<AuthStep, Error> {
		let expected = stored.get("pin").ok_or_else(pin_required)?;
		let supplied = credentials.get("pin").ok_or_else(pin_required)?;
		if expected == supplied {
			Ok(AuthStep::Authenticated)
		} else {
			Ok(AuthStep::Failed)
		}
	}
}

fn pin_required() -> Error {
	Error(Box::new(Diagnostic {
		code: "TEST_PIN_REQUIRED".to_string(),
		message: "pin is required".to_string(),
		..Default::default()
	}))
}

fn pin_credentials(identifier: &str, pin: &str) -> HashMap<String, String> {
	HashMap::from([("identifier".to_string(), identifier.to_string()), ("pin".to_string(), pin.to_string())])
}

#[test]
fn engine_and_auth_service_share_one_provider_registry() {
	// Credential creation reads the engine registry, login reads the auth service one.
	// A provider present in only one of them is half-installed, so both halves must
	// resolve providers from the very same registry instance.
	let db = TestDb::memory();

	let engine_registry = db.engine().services().auth_registry.clone();
	let service_registry = db.auth_service().auth_registry().clone();

	assert!(
		Arc::ptr_eq(&engine_registry, &service_registry),
		"credential creation and login must resolve providers from one registry"
	);
}

#[test]
fn a_registered_provider_serves_both_credential_creation_and_login() {
	// One registration must reach both halves of the credential lifecycle.
	// Creating the credential proves the engine half, logging in proves the service half.
	let db = TestDb::from(embedded::memory().with_auth_provider(PinProvider).build().expect("build"));

	db.admin("CREATE USER bob");
	db.admin("CREATE AUTHENTICATION FOR bob { method: pin; pin: '4711' }");

	let (identity, token) = db
		.auth_service()
		.authenticate("pin", pin_credentials("bob", "4711"))
		.expect("login must reach the registered provider")
		.expect_authenticated();

	assert_ne!(identity, IdentityId::default(), "login must resolve the created identity");
	assert!(!token.is_empty(), "a successful login must mint a session token");

	// A provider that always authenticates would pass the assertions above, so reject a wrong pin.
	db.auth_service()
		.authenticate("pin", pin_credentials("bob", "0000"))
		.expect("a wrong pin is a failed login, not an error")
		.expect_failed("invalid credentials");
}
