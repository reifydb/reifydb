// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Identity kinds against a fully bootstrapped database.
//!
//! These live here rather than beside the engine tests because `TestEngine` does
//! not run the root bootstrap, so `system::identities` is empty there. Root's
//! kind is only observable once `bootstrap_root_identity` has run, and that path
//! writes the row through `create_identity_with_id`, which is separate from the
//! DDL path every other identity takes.

use std::collections::HashMap;

use reifydb::testing::db::TestDb;
use reifydb_test_harness::auth::AuthResponseAssert;
use reifydb_value::value::{Value, identity::IdentityId};

fn id_of(db: &TestDb, name: &str) -> IdentityId {
	let frames = db.query(&format!("from system::identities filter {{ name == '{name}' }} map {{ id: id }}"));
	let frame = frames.first().expect("identity frame");
	let col = frame.columns.iter().find(|c| c.name == "id").expect("id column");
	match col.data.get_value(0) {
		Value::IdentityId(id) => id,
		other => panic!("unexpected id value: {other:?}"),
	}
}

fn kind_of(db: &TestDb, name: &str) -> String {
	let frames = db.query(&format!("from system::identities filter {{ name == '{name}' }} map {{ kind: kind }}"));
	let frame = frames.first().expect("identity frame");
	let col = frame.columns.iter().find(|c| c.name == "kind").expect("kind column");
	assert_eq!(frame.row_count(), 1, "expected exactly one identity named `{name}`");
	match col.data.get_value(0) {
		Value::Utf8(s) => s,
		other => panic!("unexpected kind value: {other:?}"),
	}
}

#[test]
fn root_is_bootstrapped_with_the_root_kind() {
	let db = TestDb::memory();
	assert_eq!(kind_of(&db, "root"), "root");
}

#[test]
fn create_user_and_create_service_persist_distinct_kinds() {
	let db = TestDb::memory();
	db.admin("create user alice");
	db.admin("create service probe_a");

	assert_eq!(kind_of(&db, "alice"), "user");
	assert_eq!(kind_of(&db, "probe_a"), "service");
}

#[test]
fn a_service_authenticates_by_token_and_is_refused_a_password() {
	// The end-to-end shape uptime's probes rely on: a service principal with a
	// fixed token, which the engine will not let hold a password credential.
	let db = TestDb::memory();
	db.admin("create service probe_a");
	db.admin("create authentication for probe_a { method: token; token: 'probe-secret' }");

	let (identity, _session_token) = db
		.auth_service()
		.authenticate("token", HashMap::from([("token".to_string(), "probe-secret".to_string())]))
		.expect("token authentication failed")
		.expect_authenticated();
	assert_eq!(identity, id_of(&db, "probe_a"), "token must resolve to the probe's own identity");

	let err = admin_err(&db, "create authentication for probe_a { method: password; password: 'hunter22' }");
	assert!(err.contains("CA_095"), "a service must not be able to hold a password, got: {err}");
}

#[test]
fn a_hyphenated_service_name_round_trips() {
	// uptime names its probes `probe-a` / `probe-b`. A hyphen is a minus token in
	// RQL, so the name only parses when backtick-quoted; this is exactly the form
	// uptime's provisioning emits.
	let db = TestDb::memory();
	db.admin("create service `probe-a`");
	db.admin("create authentication for `probe-a` { method: token; token: 'probe-a-dev-token' }");

	assert_eq!(kind_of(&db, "probe-a"), "service");

	let (identity, _) = db
		.auth_service()
		.authenticate("token", HashMap::from([("token".to_string(), "probe-a-dev-token".to_string())]))
		.expect("token authentication failed")
		.expect_authenticated();
	assert_eq!(identity, id_of(&db, "probe-a"));
}

fn admin_err(db: &TestDb, rql: &str) -> String {
	format!("{:?}", db.try_admin(rql).expect_err("expected the statement to be rejected"))
}

#[test]
fn root_cannot_be_dropped() {
	// Before identity kinds there was no guard of any sort on drop_identity, so
	// `DROP USER root` succeeded and removed the bootstrap identity. Root only
	// exists in a bootstrapped database, which is why this lives here.
	let db = TestDb::memory();
	let err = admin_err(&db, "drop user root");
	assert!(err.contains("CA_095"), "expected CA_095, got: {err}");
	assert_eq!(kind_of(&db, "root"), "root", "root must survive the rejected drop");
}

#[test]
fn root_cannot_be_altered() {
	// Attributes are only ever read through $identity, which is not populated
	// for privileged identities, so setting one on root is a silent no-op.
	let db = TestDb::memory();
	db.admin("create user attribute org_id: utf8");

	let alter_err = admin_err(&db, "alter user root { org_id: 'acme' }");
	assert!(alter_err.contains("CA_095"), "expected CA_095, got: {alter_err}");
}

#[test]
fn root_can_still_be_given_credentials() {
	// Root IS a loginable principal: every client auth path authenticates by
	// attaching a token to root first. An earlier version of the builtin guard
	// covered CREATE AUTHENTICATION too and broke every grpc/http/ws auth test.
	// The builtin guard must never extend to credentials.
	let db = TestDb::memory();
	let frames = db.admin("create authentication for root { method: token; token: 'roottok' }");

	let frame = frames.first().expect("authentication frame");
	let created = frame.columns.iter().find(|c| c.name == "created").expect("created column");
	assert_eq!(created.data.get_value(0), Value::Boolean(true));
}
