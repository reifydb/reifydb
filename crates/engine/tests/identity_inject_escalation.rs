// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::{engine::TestEngine, fixture::identity::identity};
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{params::Params, value::identity::IdentityId};

fn root_literal() -> String {
	format!("cast('{}', identity_id)", IdentityId::root())
}

fn guarded_engine() -> (TestEngine, IdentityId) {
	// Every policy here denies alice, so any row she reads or writes proves the identity check was skipped.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::secret { id: int4, data: utf8 }");
	t.admin(
		"CREATE TABLE POLICY p ON test::secret { from: { filter { false } }, insert: { filter { false } }, update: { filter { false } }, delete: { filter { false } } }",
	);
	t.command("INSERT test::secret [{ id: 1, data: 'classified' }]");
	let alice = identity("alice").create(&t);
	(t, alice.id)
}

#[test]
fn a_caller_must_not_choose_the_identity_of_its_own_transaction() {
	// identity::inject is a builtin routine, so it never reaches the CallableOp::Call policy gate that guards
	// catalog procedures; the transaction identity must survive the call untouched.
	let (t, alice) = guarded_engine();

	let mut txn = t.inner().begin_command(alice).unwrap();
	let r = Transaction::Command(&mut txn).rql(&format!("CALL identity::inject({})", root_literal()), Params::None);
	assert!(r.error.is_some(), "identity::inject must be rejected outside a test transaction");

	assert_eq!(txn.identity, alice, "a call must never rewrite the transaction identity");
	assert!(!txn.identity.is_privileged(), "alice escalated herself to a privileged identity");
}

#[test]
fn an_injected_root_must_not_bypass_a_denying_write_policy() {
	// Write policies are enforced at execution time against the live transaction identity, so an inject earlier in
	// the same batch turns a denied insert into an accepted one.
	let (t, alice) = guarded_engine();

	let denied = t.inner().command_as(alice, "INSERT test::secret [{ id: 2, data: 'alice' }]", Params::None);
	assert!(denied.error.is_some(), "the deny-all insert policy must reject alice on its own");

	let escalated = t.inner().command_as(
		alice,
		&format!(
			"CALL identity::inject({}); INSERT test::secret [{{ id: 3, data: 'escalated' }}]",
			root_literal()
		),
		Params::None,
	);
	assert!(escalated.error.is_some(), "the same insert must stay denied after a call to identity::inject");

	let rows = TestEngine::row_count(&t.query("FROM test::secret FILTER { id == 3 }"));
	assert_eq!(rows, 0, "alice wrote a row the insert policy forbids");
}

#[test]
fn an_anonymous_caller_must_not_inject_root() {
	// The root identity is a fixed public constant, so this needs no credentials at all.
	let (t, _alice) = guarded_engine();

	let mut txn = t.inner().begin_command(IdentityId::anonymous()).unwrap();
	let _ = Transaction::Command(&mut txn).rql(&format!("CALL identity::inject({})", root_literal()), Params::None);

	assert!(!txn.identity.is_privileged(), "an unauthenticated caller escalated to a privileged identity");
}
