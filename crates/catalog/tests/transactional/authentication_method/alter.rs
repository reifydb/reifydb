// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::duration::Duration;

use super::{GITHUB, committed, github_engine};

#[test]
fn alter_merges_the_given_settings_over_the_stored_ones() {
	// Replacing instead of merging would drop client_id and leave a github method that cannot log in.
	let t = github_engine();
	t.admin(GITHUB);
	t.admin("ALTER AUTHENTICATION METHOD github WITH { redirect_uri: 'https://app/new' }");

	let method = committed(&t, "github").unwrap();
	assert_eq!(method.properties.get("client_id").map(String::as_str), Some("abc"));
	assert_eq!(method.properties.get("redirect_uri").map(String::as_str), Some("https://app/new"));
	assert_eq!(method.ttl, Some(Duration::from_days(30).unwrap()), "an alter without ttl must keep the old ttl");
	assert!(method.promote_guest, "an alter without promote_guest must keep the old value");
}

#[test]
fn alter_can_set_forever_and_a_new_duration() {
	let t = TestEngine::new();
	t.admin("CREATE AUTHENTICATION METHOD token WITH { ttl: 1h }");

	t.admin("ALTER AUTHENTICATION METHOD token WITH { ttl: forever }");
	assert_eq!(committed(&t, "token").unwrap().ttl, None);

	t.admin("ALTER AUTHENTICATION METHOD token WITH { ttl: 2h }");
	assert_eq!(committed(&t, "token").unwrap().ttl, Some(Duration::from_hours_const(2)));
}

#[test]
fn a_rejected_alter_leaves_the_row_unchanged() {
	let t = github_engine();
	t.admin(GITHUB);
	let before = committed(&t, "github").unwrap();

	let err = t.admin_err("ALTER AUTHENTICATION METHOD github WITH { ttl: 1h, scope: 'x' }");
	assert!(err.contains("CA_122"), "{err}");
	assert_eq!(committed(&t, "github").unwrap(), before);
}

#[test]
fn altering_an_undeclared_method_is_not_found() {
	let t = TestEngine::new();
	let err = t.admin_err("ALTER AUTHENTICATION METHOD token WITH { ttl: 1h }");
	assert!(err.contains("CA_118"), "{err}");
	assert!(committed(&t, "token").is_none());
}
