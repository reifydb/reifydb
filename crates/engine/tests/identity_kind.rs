// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Identity kinds: CREATE USER / CREATE SERVICE, the reserved `kind` attribute
//! name, and the `$identity.kind` field.
//!
//! The reserved-name checks matter because `$identity` is a security-sensitive
//! symbol: field access resolves by first match over the column names, so a user
//! attribute sharing a name with a built-in field would be silently shadowed by
//! it rather than raising an error.

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{Value, identity::IdentityId},
};

fn lookup_identity(t: &TestEngine, name: &str) -> IdentityId {
	let frames = t.query(&format!("from system::identities filter {{ name == '{name}' }}"));
	let frame = frames.first().expect("identity frame");
	let col = frame.columns.iter().find(|c| c.name == "id").expect("id column");
	match col.data.get_value(0) {
		Value::IdentityId(id) => id,
		other => panic!("unexpected identity value: {other:?}"),
	}
}

fn kind_of(t: &TestEngine, name: &str) -> String {
	let frames = t.query(&format!("from system::identities filter {{ name == '{name}' }} map {{ kind: kind }}"));
	let frame = frames.first().expect("identity frame");
	let col = frame.columns.iter().find(|c| c.name == "kind").expect("kind column");
	match col.data.get_value(0) {
		Value::Utf8(s) => s,
		other => panic!("unexpected kind value: {other:?}"),
	}
}

#[test]
fn test_create_user_stores_user_kind() {
	let t = TestEngine::new();
	t.admin("CREATE USER alice");
	assert_eq!(kind_of(&t, "alice"), "user");
}

#[test]
fn test_create_service_stores_service_kind() {
	// The whole point of the discriminant: CREATE SERVICE must persist a kind
	// distinct from CREATE USER, not merely parse.
	let t = TestEngine::new();
	t.admin("CREATE SERVICE probe_a");
	assert_eq!(kind_of(&t, "probe_a"), "service");
}

#[test]
fn test_create_user_attribute_kind_is_rejected() {
	// `kind` is now a built-in $identity field. Without this rejection an
	// identity carrying both would produce two columns named `kind`, and field
	// access would silently return the built-in, never the attribute.
	let t = TestEngine::new();
	let err = t.admin_err("CREATE USER ATTRIBUTE kind: utf8");
	assert!(err.contains("CA_093"), "expected CA_093, got: {err}");
}

#[test]
fn test_create_user_attribute_service_is_rejected() {
	// Consequence of introducing the SERVICE keyword: the attribute-name guard
	// rejects anything that collides with an RQL keyword.
	let t = TestEngine::new();
	let err = t.admin_err("CREATE USER ATTRIBUTE service: utf8");
	assert!(err.contains("CA_093"), "expected CA_093, got: {err}");
}

#[test]
fn test_existing_reserved_attribute_names_still_rejected() {
	// Guards against the reserved list being replaced rather than extended.
	let t = TestEngine::new();
	for name in ["id", "name", "roles"] {
		let err = t.admin_err(&format!("CREATE USER ATTRIBUTE {name}: utf8"));
		assert!(err.contains("CA_093"), "expected CA_093 for `{name}`, got: {err}");
	}
}

#[test]
fn test_drop_user_refuses_a_service() {
	// Without this the two spellings would be interchangeable and the kind
	// would carry no weight at the DDL layer.
	let t = TestEngine::new();
	t.admin("CREATE SERVICE probe_a");
	let err = t.admin_err("DROP USER probe_a");
	assert!(err.contains("CA_095"), "expected CA_095, got: {err}");
}

#[test]
fn test_drop_service_refuses_a_user() {
	let t = TestEngine::new();
	t.admin("CREATE USER alice");
	let err = t.admin_err("DROP SERVICE alice");
	assert!(err.contains("CA_095"), "expected CA_095, got: {err}");
}

#[test]
fn test_matching_drop_statements_succeed() {
	// The mismatch guard must not make identities undroppable: each kind has to
	// remain reachable through its own statement.
	let t = TestEngine::new();
	t.admin("CREATE USER alice");
	t.admin("CREATE SERVICE probe_a");

	t.admin("DROP USER alice");
	t.admin("DROP SERVICE probe_a");

	let remaining = t.query("from system::identities");
	assert_eq!(remaining.first().expect("frame").row_count(), 0, "both identities should be gone");
}

#[test]
fn test_service_may_not_hold_a_password_credential() {
	// This is the invariant the whole discriminant exists to make enforceable.
	// It is only sound because kind is total and immutable: an optional or
	// revocable marker could be absent at this moment and the check would pass.
	let t = TestEngine::new();
	t.admin("CREATE SERVICE probe_a");
	let err = t.admin_err("CREATE AUTHENTICATION FOR probe_a { method: password; password: 'hunter22' }");
	assert!(err.contains("CA_095"), "expected CA_095, got: {err}");
}

#[test]
fn test_service_may_hold_a_token_credential() {
	// The rule targets password specifically; tokens are how a service is meant
	// to authenticate, so this must keep working.
	let t = TestEngine::new();
	t.admin("CREATE SERVICE probe_a");
	t.admin("CREATE AUTHENTICATION FOR probe_a { method: token; token: 'probe-secret' }");
}

#[test]
fn test_user_may_still_hold_a_password_credential() {
	// Regression guard: the rule must key on kind, not on the method alone.
	let t = TestEngine::new();
	t.admin("CREATE USER alice");
	t.admin("CREATE AUTHENTICATION FOR alice { method: password; password: 'hunter22' }");
}

#[test]
fn test_identity_kind_is_visible_to_a_non_privileged_identity() {
	// $identity is only populated for non-privileged identities, so this must
	// run as the identity itself rather than as root. This is the capability
	// the discriminant exists to enable: discriminating on kind in a predicate.
	let t = TestEngine::new();
	t.admin("CREATE SERVICE probe_a");
	t.admin("CREATE SESSION POLICY allow_query { query: { filter { true } } }");
	let probe = lookup_identity(&t, "probe_a");

	let frames = t
		.inner()
		.query_as(probe, "map { k: $identity.kind }", Params::None)
		.check()
		.unwrap_or_else(|e| panic!("query_as failed: {e:?}"));
	let frame = frames.first().expect("frame");
	let col = frame.columns.iter().find(|c| c.name == "k").expect("k column");
	assert_eq!(col.data.get_value(0), Value::Utf8("service".to_string()));
}
