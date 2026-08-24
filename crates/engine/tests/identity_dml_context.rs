// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::{engine::TestEngine, fixture::identity::identity};
use reifydb_value::{params::Params, value::identity::IdentityId};

fn alice_engine() -> (TestEngine, IdentityId) {
	// is_privileged() short-circuits policy enforcement, so only a non-privileged caller exercises this.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4, who: identity_id, was_root: boolean, was_anon: boolean }");
	t.admin(
		"CREATE TABLE POLICY p ON test::t { from: { filter { true } }, insert: { filter { true } }, update: { filter { true } }, delete: { filter { true } } }",
	);
	let alice = identity("alice").create(&t);
	(t, alice.id)
}

#[test]
fn insert_evaluates_identity_functions_as_the_caller() {
	// INSERT builds its QueryContext with a hard-coded root identity, letting a non-privileged writer stamp rows as
	// root.
	let (t, alice) = alice_engine();

	let r = t.inner().command_as(
		alice,
		"INSERT test::t [{ id: 1, who: identity::id(), was_root: is::root(), was_anon: is::anonymous() }]",
		Params::None,
	);
	assert!(r.error.is_none(), "alice's insert must pass the permissive policy, got: {:?}", r.error);

	let frames = t.query("FROM test::t map { who, was_root, was_anon }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows.len(), 1);
	assert_eq!(rows[0].get::<IdentityId>("who").unwrap().unwrap(), alice, "row must be stamped with alice");
	assert!(!rows[0].get::<bool>("was_root").unwrap().unwrap(), "is::root() must be false for alice");
	assert!(!rows[0].get::<bool>("was_anon").unwrap().unwrap(), "is::anonymous() must be false for alice");
}

#[test]
fn insert_returning_evaluates_identity_functions_as_the_caller() {
	// evaluate_returning builds its own EvalContext with root, so RETURNING leaks root independently of the row
	// list.
	let (t, alice) = alice_engine();

	let r = t.inner().command_as(
		alice,
		"INSERT test::t [{ id: 1, who: identity::id(), was_root: false, was_anon: false }] RETURNING { id, caller: identity::id(), root: is::root() }",
		Params::None,
	);
	assert!(r.error.is_none(), "alice's insert must pass the permissive policy, got: {:?}", r.error);

	let rows: Vec<_> = r.frames[0].rows().collect();
	assert_eq!(rows[0].get::<IdentityId>("caller").unwrap().unwrap(), alice, "RETURNING must see alice");
	assert!(!rows[0].get::<bool>("root").unwrap().unwrap(), "RETURNING is::root() must be false for alice");
}

#[test]
fn update_evaluates_identity_functions_as_the_caller() {
	// UPDATE has the same hard-coded root EvalContext as INSERT.
	let (t, alice) = alice_engine();
	t.command("INSERT test::t [{ id: 1, who: identity::id(), was_root: false, was_anon: false }]");

	let r = t.inner().command_as(
		alice,
		"UPDATE test::t { who: identity::id(), was_root: is::root() } FILTER { id == 1 }",
		Params::None,
	);
	assert!(r.error.is_none(), "alice's update must pass the permissive policy, got: {:?}", r.error);

	let frames = t.query("FROM test::t map { who, was_root }");
	let rows: Vec<_> = frames[0].rows().collect();
	assert_eq!(rows[0].get::<IdentityId>("who").unwrap().unwrap(), alice, "update must stamp alice");
	assert!(!rows[0].get::<bool>("was_root").unwrap().unwrap(), "is::root() must be false for alice");
}

#[test]
fn delete_returning_evaluates_identity_functions_as_the_caller() {
	// DELETE ... RETURNING routes through the same root-pinned returning context.
	let (t, alice) = alice_engine();
	t.command("INSERT test::t [{ id: 1, who: identity::id(), was_root: false, was_anon: false }]");

	let r = t.inner().command_as(
		alice,
		"DELETE test::t FILTER { id == 1 } RETURNING { id, caller: identity::id(), root: is::root() }",
		Params::None,
	);
	assert!(r.error.is_none(), "alice's delete must pass the permissive policy, got: {:?}", r.error);

	let rows: Vec<_> = r.frames[0].rows().collect();
	assert_eq!(rows[0].get::<IdentityId>("caller").unwrap().unwrap(), alice, "RETURNING must see alice");
	assert!(!rows[0].get::<bool>("root").unwrap().unwrap(), "RETURNING is::root() must be false for alice");
}
