// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

use super::committed;

#[test]
fn a_method_in_use_cannot_be_dropped_until_its_credentials_are_gone() {
	// Dropping it would leave a credential naming a method that no longer exists.
	let t = TestEngine::new();
	t.admin("CREATE AUTHENTICATION METHOD password WITH { ttl: 1h }");
	t.admin("CREATE USER alice");
	t.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'secret' }");

	let err = t.admin_err("DROP AUTHENTICATION METHOD password");
	assert!(err.contains("CA_120") && err.contains("1 credentials"), "{err}");
	assert!(committed(&t, "password").is_some());

	t.admin("DROP AUTHENTICATION FOR alice { method: password }");
	let frames = t.admin("DROP AUTHENTICATION METHOD password");
	assert!(frames[0].to_string().contains("dropped"), "{}", frames[0]);
	assert!(committed(&t, "password").is_none());
}

#[test]
fn drop_if_exists_on_a_missing_method_reports_not_dropped() {
	let t = TestEngine::new();
	let frames = t.admin("DROP AUTHENTICATION METHOD IF EXISTS github");
	let row = frames[0].to_string();
	assert!(row.contains("github") && row.contains("false"), "{row}");
}

#[test]
fn dropping_a_missing_method_without_if_exists_is_not_found() {
	let t = TestEngine::new();
	let err = t.admin_err("DROP AUTHENTICATION METHOD github");
	assert!(err.contains("CA_118"), "{err}");
}
