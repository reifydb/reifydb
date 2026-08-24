// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::{engine::TestEngine, fixture::identity::identity};
use reifydb_value::{params::Params, value::identity::IdentityId};

fn guarded_engine() -> (TestEngine, IdentityId) {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::secret { id: int4, data: utf8 }");
	t.admin("CREATE TABLE POLICY p ON test::secret { from: { filter { false } } }");
	t.command("INSERT test::secret [{ id: 1, data: 'classified' }, { id: 2, data: 'more' }]");
	let alice = identity("alice").create(&t).id;
	(t, alice)
}

fn columns_seen(t: &TestEngine, who: IdentityId, rql: &str) -> Vec<String> {
	let r = t.inner().query_as(who, rql, Params::None);
	assert!(r.error.is_none(), "{rql} errored: {:?}", r.error);
	r.frames.iter()
		.filter(|f| f.rows().count() > 0)
		.flat_map(|f| f.columns.iter().map(|c| c.name.clone()))
		.collect()
}

#[test]
fn a_procedure_body_must_not_read_past_a_denying_from_policy() {
	// read policy is injected at compile time, so a body compiled with the raw compiler reads with no
	// policy at all; a caller must never see through a procedure what it cannot see directly.
	// asserting on the columns that carry data, not on a row count: the procedure still returns a
	// single none-valued `value` row when its body yields nothing, which a count cannot tell apart
	// from a leaked row.
	let (t, alice) = guarded_engine();
	t.admin("CREATE PROCEDURE test::leak AS { FROM test::secret }");
	t.admin("CREATE PROCEDURE POLICY cp ON test::leak { call: { filter { true } } }");

	assert!(columns_seen(&t, alice, "FROM test::secret").is_empty(), "the from policy must deny the direct read");

	let leaked = columns_seen(&t, alice, "CALL test::leak()");
	assert!(!leaked.iter().any(|c| c == "data"), "a procedure body leaked the secret column: {leaked:?}");

	// the control: injection must not over-apply and blind a privileged reader.
	let privileged = columns_seen(&t, IdentityId::root(), "CALL test::leak()");
	assert!(privileged.iter().any(|c| c == "data"), "root lost access through the same procedure: {privileged:?}");
}

#[test]
fn a_procedure_body_must_not_leak_a_filtered_secret() {
	// a body that filters on the secret turns the procedure into an oracle: even one surviving row
	// confirms the content of a row the caller may not read.
	let (t, alice) = guarded_engine();
	t.admin("CREATE PROCEDURE test::probe AS { FROM test::secret filter { data == 'classified' } }");
	t.admin("CREATE PROCEDURE POLICY cp ON test::probe { call: { filter { true } } }");

	let leaked = columns_seen(&t, alice, "CALL test::probe()");
	assert!(
		!leaked.iter().any(|c| c == "data"),
		"a procedure body confirmed the content of a denied row: {leaked:?}"
	);

	let privileged = columns_seen(&t, IdentityId::root(), "CALL test::probe()");
	assert!(privileged.iter().any(|c| c == "data"), "root lost access through the same procedure: {privileged:?}");
}
