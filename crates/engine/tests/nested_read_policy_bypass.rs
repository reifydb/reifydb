// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::{engine::TestEngine, fixture::identity::identity};
use reifydb_value::{params::Params, value::identity::IdentityId};

fn owned_engine() -> (TestEngine, IdentityId) {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::secret { k: int4, owner: utf8, data: utf8 }");
	t.admin("CREATE TABLE POLICY own ON test::secret { from: { filter { owner == $identity.name } } }");
	t.admin("CREATE TABLE test::open { k: int4 }");
	t.admin("CREATE TABLE POLICY open ON test::open { from: { filter { true } } }");
	t.command(
		"INSERT test::secret [{ k: 1, owner: 'alice', data: 'alice-note' }, \
		 { k: 1, owner: 'bob', data: 'bob-secret' }]",
	);
	t.command("INSERT test::open [{ k: 1 }]");
	let alice = identity("alice").create(&t).id;
	(t, alice)
}

fn data_seen(t: &TestEngine, who: IdentityId, rql: &str) -> Vec<String> {
	let r = t.inner().command_as(who, rql, Params::None);
	assert!(r.error.is_none(), "{rql} errored: {:?}", r.error);
	let mut seen: Vec<String> =
		r.frames.iter()
			.flat_map(|f| f.columns.iter().filter(|c| c.name.ends_with("data")))
			.flat_map(|c| (0..c.data.len()).map(move |i| c.data.as_string(i)))
			.collect();
	seen.sort();
	seen
}

fn assert_only_own_rows(rql: &str) {
	let (t, alice) = owned_engine();
	assert_eq!(data_seen(&t, alice, "FROM test::secret"), vec!["alice-note"], "the direct read must be scoped");
	assert_eq!(data_seen(&t, alice, rql), vec!["alice-note"], "the nested read is not scoped like the direct one");
}

#[test]
fn a_let_binding_is_scoped() {
	// a let-bound scan that skips injection hands every owner's rows to `from $r`.
	assert_only_own_rows("let $r = from test::secret; from $r");
}

#[test]
fn an_assignment_is_scoped() {
	// reassigning a variable from a scan must not bypass the policy a let binding obeys.
	assert_only_own_rows("let $r = from test::secret filter { false }; $r = from test::secret; from $r");
}

#[test]
fn an_if_branch_is_scoped() {
	// a branch body is a separate plan; an unwalked branch reads unfiltered.
	assert_only_own_rows("if true { from test::secret }");
}

#[test]
fn an_else_branch_is_scoped() {
	// an unwalked else branch hands every owner's rows to the caller.
	assert_only_own_rows(
		"if false { from test::open } else if false { from test::open } else { from test::secret }",
	);
}

#[test]
fn an_else_if_branch_is_scoped() {
	// an unwalked else-if branch hands every owner's rows to the caller.
	assert_only_own_rows("if false { from test::open } else if true { from test::secret }");
}

#[test]
fn a_match_arm_is_scoped() {
	// an unwalked match arm hands every owner's rows to the caller.
	assert_only_own_rows("MATCH { false => { from test::open }, ELSE => { from test::secret } }");
}

#[test]
fn a_loop_body_is_scoped() {
	// an unwalked loop body appends every owner's rows to the variable.
	assert_only_own_rows("loop { append $out from { from test::secret }; break }; from $out");
}

fn rows_counted(rql: &str) -> String {
	let (t, alice) = owned_engine();
	let r = t.inner().command_as(alice, rql, Params::None);
	assert!(r.error.is_none(), "{rql} errored: {:?}", r.error);
	r.frames[0].columns[0].data.as_string(0)
}

#[test]
fn a_while_body_is_scoped() {
	// an unwalked while body counts the rows of every owner, not only the caller's.
	let rql = "let $n = 0; let $c = 0; \
		   while $n < 1 { $n = $n + 1; for $row in { from test::secret } { $c = $c + 1 } }; map { c: $c }";
	assert_eq!(rows_counted(rql), "1", "the while body saw rows of another owner");
}

#[test]
fn a_for_body_is_scoped() {
	// a for body must be scoped independently of its iterable.
	let rql = "let $c = 0; \
		   for $i in { from test::open } { for $row in { from test::secret } { $c = $c + 1 } }; map { c: $c }";
	assert_eq!(rows_counted(rql), "1", "the for body saw rows of another owner");
}

#[test]
fn a_for_iterable_is_scoped() {
	// iterating an unscoped scan reveals how many rows other owners hold.
	let rql = "let $n = 0; for $row in { from test::secret } { $n = $n + 1 }; map { n: $n }";
	assert_eq!(rows_counted(rql), "1", "the for iterable saw rows of another owner");
}

#[test]
fn an_append_into_a_variable_is_scoped() {
	// the append source is a nested statement; the variable must receive only permitted rows.
	assert_only_own_rows("append $out from { from test::secret }; from $out");
}

#[test]
fn an_inner_join_right_side_is_scoped() {
	// the right side of a join is a nested scan; unscoped, it joins in every owner's rows.
	assert_only_own_rows("from test::open inner join { from test::secret } as s using (k, s.k)");
}

#[test]
fn a_left_join_right_side_is_scoped() {
	// a left join keeps the right side's rows as columns, so an unscoped right side leaks them.
	assert_only_own_rows("from test::open left join { from test::secret } as s using (k, s.k)");
}

#[test]
fn a_natural_join_right_side_is_scoped() {
	// a natural join resolves its right side like the other joins and must be scoped the same.
	assert_only_own_rows("from test::open natural join { from test::secret } as s");
}

#[test]
fn a_udf_body_is_scoped() {
	// an unwalked function body hands every owner's rows to its caller.
	assert_only_own_rows("UDF leak () { from test::secret }; leak()");
}

#[test]
fn a_returned_query_is_scoped() {
	// an unwalked return statement hands every owner's rows to the caller.
	assert_only_own_rows("UDF leak () { RETURN { from test::secret } }; leak()");
}

#[test]
fn a_closure_body_is_scoped() {
	// an unwalked closure body hands every owner's rows to its caller.
	assert_only_own_rows("let $leak = () { from test::secret }; $leak()");
}

#[test]
fn an_insert_source_is_scoped() {
	// an insert fed by a scan copies rows; unscoped, it exfiltrates them into a readable table.
	let (t, alice) = owned_engine();
	t.admin("CREATE TABLE test::copied { k: int4, owner: utf8, data: utf8 }");
	t.admin(
		"CREATE TABLE POLICY copy_target ON test::copied { from: { filter { true } }, insert: { filter { true } } }",
	);
	let r = t.inner().command_as(alice, "INSERT test::copied FROM test::secret", Params::None);
	assert!(r.error.is_none(), "insert errored: {:?}", r.error);
	assert_eq!(data_seen(&t, IdentityId::root(), "FROM test::copied"), vec!["alice-note"], "insert copied rows");
}

fn copied_by(t: &TestEngine, who: IdentityId, insert: &str, read: &str) -> Vec<String> {
	let r = t.inner().command_as(who, insert, Params::None);
	assert!(r.error.is_none(), "{insert} errored: {:?}", r.error);
	data_seen(t, IdentityId::root(), read)
}

#[test]
fn a_ringbuffer_insert_source_is_scoped() {
	// an unwalked ringbuffer insert source exfiltrates every owner's rows into the ringbuffer.
	let (t, alice) = owned_engine();
	t.admin("CREATE RINGBUFFER test::copied { k: int4, owner: utf8, data: utf8 } WITH { capacity: 10 }");
	t.admin("CREATE RINGBUFFER POLICY copy_target ON test::copied { insert: { filter { true } } }");
	let copied = copied_by(&t, alice, "INSERT test::copied FROM test::secret", "FROM test::copied");
	assert_eq!(copied, vec!["alice-note"], "the ringbuffer insert copied rows of another owner");
}

#[test]
fn a_series_insert_source_is_scoped() {
	// an unwalked series insert source exfiltrates every owner's rows into the series.
	let (t, alice) = owned_engine();
	t.admin("CREATE SERIES test::copied { k: int4, owner: utf8, data: utf8 } WITH { key: k }");
	t.admin("CREATE SERIES POLICY copy_target ON test::copied { insert: { filter { true } } }");
	let copied = copied_by(&t, alice, "INSERT test::copied FROM test::secret", "FROM test::copied");
	assert_eq!(copied, vec!["alice-note"], "the series insert copied rows of another owner");
}

#[test]
fn a_dictionary_insert_source_is_scoped() {
	// an unwalked dictionary insert source exfiltrates every owner's values into the dictionary.
	let (t, alice) = owned_engine();
	t.admin("CREATE TABLE test::words { value: utf8, owner: utf8 }");
	t.admin("CREATE TABLE POLICY own_words ON test::words { from: { filter { owner == $identity.name } } }");
	t.command(
		"INSERT test::words [{ value: 'alice-note', owner: 'alice' }, { value: 'bob-secret', owner: 'bob' }]",
	);
	t.admin("CREATE DICTIONARY test::copied FOR utf8 AS uint4");
	t.admin("CREATE DICTIONARY POLICY copy_target ON test::copied { insert: { filter { true } } }");
	let copied =
		copied_by(&t, alice, "INSERT test::copied FROM test::words", "FROM test::copied MAP { data: value }");
	assert_eq!(copied, vec!["alice-note"], "the dictionary insert copied values of another owner");
}

#[test]
fn a_let_binding_in_a_procedure_body_is_scoped() {
	// an unwalked let binding in a procedure body hands every owner's rows to the caller.
	let (t, alice) = owned_engine();
	t.admin("CREATE PROCEDURE test::leak AS { let $r = from test::secret; from $r }");
	t.admin("CREATE PROCEDURE POLICY cp ON test::leak { call: { filter { true } } }");
	assert_eq!(data_seen(&t, alice, "CALL test::leak()"), vec!["alice-note"], "the procedure leaked rows");
}

#[test]
fn a_nested_scan_of_an_unguarded_table_is_denied() {
	// default deny: a nested scan of a table with no from policy must see nothing, like a direct one.
	let (t, alice) = owned_engine();
	t.admin("CREATE TABLE test::unguarded { k: int4, data: utf8 }");
	t.command("INSERT test::unguarded [{ k: 1, data: 'hidden' }]");
	let nested = "let $r = from test::unguarded; from $r";
	assert_eq!(data_seen(&t, IdentityId::root(), nested), vec!["hidden"], "the form must surface the row to root");
	assert!(data_seen(&t, alice, "FROM test::unguarded").is_empty(), "the direct read must be denied");
	let seen = data_seen(&t, alice, nested);
	assert!(seen.is_empty(), "a nested read of an unguarded table was allowed: {seen:?}");
}

#[test]
fn a_privileged_reader_is_not_scoped_through_a_let_binding() {
	// the walk must not over-apply: root bypasses from policies at every depth.
	let (t, _) = owned_engine();
	let seen = data_seen(&t, IdentityId::root(), "let $r = from test::secret; from $r");
	assert_eq!(seen, vec!["alice-note", "bob-secret"], "root lost rows through a let binding");
}
