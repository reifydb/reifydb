// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::collections::BTreeMap;

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::duration::Duration;

use super::{GITHUB, committed, github_engine};

#[test]
fn a_declared_method_is_stored_with_its_typed_settings() {
	let t = github_engine();
	let frames = t.admin(GITHUB);
	let row = frames[0].to_string();
	assert!(row.contains("github") && row.contains("created") && row.contains("true"), "{row}");

	let method = committed(&t, "github").expect("github must be declared");
	assert_eq!(method.ttl, Some(Duration::from_days(30).unwrap()));
	assert!(method.promote_guest);
	assert_eq!(
		method.properties,
		BTreeMap::from([
			("client_id".to_string(), "abc".to_string()),
			("redirect_uri".to_string(), "https://app/cb".to_string()),
		])
	);
}

#[test]
fn forever_is_stored_as_no_ttl() {
	let t = TestEngine::new();
	t.admin("CREATE AUTHENTICATION METHOD token WITH { ttl: forever }");
	assert_eq!(committed(&t, "token").unwrap().ttl, None);
}

#[test]
fn an_unregistered_provider_is_refused_and_the_registered_ones_are_listed() {
	// The name is the provider, so a typo must fail instead of declaring a method nothing can serve.
	let t = TestEngine::new();
	let err = t.admin_err("CREATE AUTHENTICATION METHOD githb WITH { ttl: 1h }");
	assert!(err.contains("CA_125"), "{err}");
	assert!(err.contains("github, guest, password, solana, token"), "{err}");
	assert!(committed(&t, "githb").is_none());
}

#[test]
fn provider_settings_are_checked_by_the_provider() {
	// A github method without redirect_uri would fail on the first login instead of at declaration.
	let t = TestEngine::new();
	let missing = t.admin_err("CREATE AUTHENTICATION METHOD github WITH { ttl: 1h, client_id: 'abc' }");
	assert!(missing.contains("CA_126") && missing.contains("redirect_uri"), "{missing}");

	let unknown = t.admin_err(
		"CREATE AUTHENTICATION METHOD github WITH { ttl: 1h, client_id: 'a', redirect_uri: 'b', scope: 'x' }",
	);
	assert!(unknown.contains("CA_122") && unknown.contains("scope"), "{unknown}");

	let password = t.admin_err("CREATE AUTHENTICATION METHOD password WITH { ttl: 1h, rounds: '10' }");
	assert!(password.contains("CA_122") && password.contains("rounds"), "{password}");
}

#[test]
fn only_identity_creating_providers_may_promote_guests() {
	let t = TestEngine::new();
	let err = t.admin_err("CREATE AUTHENTICATION METHOD token WITH { ttl: 1h, promote_guest: true }");
	assert!(err.contains("CA_121"), "{err}");
	assert!(committed(&t, "token").is_none());

	t.admin("CREATE AUTHENTICATION METHOD password WITH { ttl: 1h, promote_guest: true }");
	assert!(committed(&t, "password").unwrap().promote_guest);
}

#[test]
fn a_second_declaration_of_the_same_method_is_refused() {
	let t = TestEngine::new();
	t.admin("CREATE AUTHENTICATION METHOD token WITH { ttl: forever }");
	let err = t.admin_err("CREATE AUTHENTICATION METHOD token WITH { ttl: 1h }");
	assert!(err.contains("CA_117"), "{err}");
	assert_eq!(committed(&t, "token").unwrap().ttl, None);
}
