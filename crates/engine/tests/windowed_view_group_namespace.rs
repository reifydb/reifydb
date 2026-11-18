// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use arrow_array::RecordBatch;
use reifydb_catalog::vtable::user::{UserVTable, UserVTableColumn};
use reifydb_core::value::batch::empty_batch;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{params::Params, value::value_type::ValueType};

const GROUP: &str = "create deferred windowed view group test::g { c: int8 } with { members: [ { view: test::g::m1, duration: 5s, pane: 1s }, { view: test::g::m2, duration: 10s, pane: 1s } ] } as { from test::events window rolling { c: math::count(id) } }";

const OWN_GROUP: &str = "create deferred windowed view group test::g { c: int8 } with { members: [ { view: test::g_1, duration: 5s, pane: 1s }, { view: test::g_2, duration: 10s, pane: 1s } ] } as { from test::events window rolling { c: math::count(id) } }";

#[derive(Clone)]
struct PlainTable;

impl UserVTable for PlainTable {
	fn vtable(&self) -> Vec<UserVTableColumn> {
		vec![UserVTableColumn::new("id", ValueType::Int4)]
	}

	fn get(&self) -> RecordBatch {
		empty_batch()
	}
}

fn prepared() -> TestEngine {
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin("create table test::events { id: int4, ts: datetime, value: int4 } with { time: event(ts) }");
	t.admin("create table test::src { id: int4 } with { time: processing }");
	t
}

fn with_group() -> TestEngine {
	let t = prepared();
	t.admin(GROUP);
	t
}

fn refused(t: &TestEngine, rql: &str) -> (String, String) {
	let result = t.inner().admin_as(TestEngine::identity(), rql, Params::None);
	let error = result.error.unwrap_or_else(|| panic!("must be refused: {rql}"));
	(error.code.clone(), error.message.clone())
}

fn count(t: &TestEngine, rql: &str) -> usize {
	TestEngine::row_count(&t.query(rql))
}

fn namespaces(t: &TestEngine, name: &str) -> usize {
	count(t, &format!("from system::namespaces filter {{name == '{name}'}}"))
}

fn views(t: &TestEngine, name: &str) -> usize {
	count(t, &format!("from system::views filter {{name == '{name}'}}"))
}

fn groups(t: &TestEngine, name: &str) -> usize {
	count(t, &format!("from system::windowed_view_groups filter {{name == '{name}'}}"))
}

fn assert_group_intact(t: &TestEngine, case: &str) {
	assert_eq!(groups(t, "g"), 1, "{case}: the group stays");
	assert_eq!(views(t, "m1"), 1, "{case}: member m1 stays");
	assert_eq!(views(t, "m2"), 1, "{case}: member m2 stays");
	assert_eq!(namespaces(t, "test::g"), 1, "{case}: namespace test::g stays");
}

#[test]
fn a_group_creates_the_namespace_of_its_members() {
	// Without the group making its namespace, a member named test::g::m1 has nowhere to live.
	let t = with_group();
	assert_eq!(namespaces(&t, "test::g"), 1, "the group made namespace test::g");
	assert_eq!(views(&t, "m1"), 1, "member m1");
	assert_eq!(views(&t, "m2"), 1, "member m2");
	t.query("from test::g::m1");
}

#[test]
fn a_group_over_an_existing_namespace_is_refused() {
	// A group that adopts an existing namespace would later drop objects it never made.
	let t = prepared();
	t.admin("create namespace test::g");
	let (code, message) = refused(&t, GROUP);
	assert_eq!(code, "CA_137", "{message}");
	assert!(message.ends_with("is already taken by a namespace"), "{message}");
	assert_eq!(groups(&t, "g"), 0, "no group");
	assert_eq!(views(&t, "m1"), 0, "no member m1");
	assert_eq!(views(&t, "m2"), 0, "no member m2");
}

#[test]
fn a_namespace_under_a_group_name_is_refused() {
	// Without the check in create namespace a namespace takes the name the group and its flow hold.
	let t = prepared();
	t.admin(OWN_GROUP);
	let (code, message) = refused(&t, "create namespace test::g");
	assert_eq!(code, "CA_137", "{message}");
	assert!(message.ends_with("is already taken by a flow"), "{message}");
}

#[test]
fn a_group_with_members_in_its_own_namespace_creates_no_namespace() {
	// A group that always makes its namespace leaves an empty test::g nobody asked for.
	let t = prepared();
	t.admin(OWN_GROUP);
	assert_eq!(namespaces(&t, "test::g"), 0, "no namespace test::g");
}

#[test]
fn dropping_a_group_drops_its_namespace_and_a_non_member_object_inside() {
	// A group drop that skips its namespace leaves test::g and what is inside it behind.
	let t = with_group();
	t.admin("create table test::g::extra { id: int4 }");
	t.admin("drop windowed view group test::g");
	assert_eq!(namespaces(&t, "test::g"), 0, "namespace test::g is gone");
	assert_eq!(views(&t, "m1"), 0, "member m1 is gone");
	assert_eq!(views(&t, "m2"), 0, "member m2 is gone");
	assert_eq!(count(&t, "from system::tables filter {name == 'extra'}"), 0, "table extra is gone");
	t.admin("create namespace test::g");
}

#[test]
fn dropping_a_group_is_refused_while_its_namespace_holds_what_the_drop_cannot_remove() {
	// Without the refusal the drop goes through and leaves an object the namespace drop cannot remove.
	let cases: [(&str, &[&str], &str, &str); 8] = [
		("series", &["create series test::g::s { k: int8, v: int4 } with { key: k }"], "series", "s"),
		("procedure", &["create procedure test::g::p as { \"A\" }"], "procedure", "p"),
		(
			"handler",
			&[
				"create event test::evt { Fired { n: int4 } }",
				"create handler test::g::h on test::evt::Fired { from test::src }",
			],
			"procedure",
			"h",
		),
		(
			"sink",
			&[
				"create sink test::g::k as { from test::src to postgres { uri: \"postgres://h/db\", table: \"t\" } }",
			],
			"sink",
			"k",
		),
		(
			"source",
			&[
				"create source test::g::o as { from postgres { uri: \"postgres://h/db\", query: \"q\" } to test::src }",
			],
			"source",
			"o",
		),
		("test", &["create test test::g::t { from test::src | assert { id > 0 } }"], "test", "t"),
		(
			"binding",
			&[
				"create procedure test::dep_proc as { \"A\" }",
				"create grpc binding test::g::b for test::dep_proc with { name: \"b_g\" }",
			],
			"binding",
			"b",
		),
		("child namespace", &["create namespace test::g::deep"], "namespace", "deep"),
	];
	for (case, setup, kind, name) in cases {
		let t = with_group();
		for statement in setup {
			t.admin(statement);
		}
		assert_refused_while_holding(&t, case, kind, name);
	}

	let t = with_group();
	let namespace = {
		let catalog = t.inner().catalog();
		let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
		let mut txn = Transaction::Query(&mut query_txn);
		catalog.find_namespace_by_name(&mut txn, "test::g").unwrap().unwrap().id()
	};
	t.inner().register_virtual_table(namespace, "vt", PlainTable).unwrap();
	assert_refused_while_holding(&t, "virtual table", "virtual table", "vt");
}

fn assert_refused_while_holding(t: &TestEngine, case: &str, kind: &str, name: &str) {
	let holds = format!("still holds {kind} `{name}`");
	for _ in 0..2 {
		let (code, message) = refused(t, "drop windowed view group test::g");
		assert_eq!(code, "CA_138", "{case}: {message}");
		assert!(message.ends_with(&holds), "{case}: {message}");
	}
	assert_group_intact(t, case);
}

#[test]
fn dropping_a_group_is_refused_while_another_flow_reads_a_table_in_its_namespace() {
	// A group drop through the catalog namespace drop skips the in-use check and strands the reader.
	let t = with_group();
	t.admin("create table test::g::extra { id: int4 } with { time: processing }");
	t.admin("create deferred view test::reader { id: int4 } as { from test::g::extra }");
	let (code, message) = refused(&t, "drop windowed view group test::g");
	assert_eq!(code, "CA_034", "{message}");
	assert_group_intact(&t, "reader");
	assert_eq!(count(&t, "from system::tables filter {name == 'extra'}"), 1, "table extra stays");
	assert_eq!(views(&t, "reader"), 1, "view reader stays");
	t.query("from test::g::m1");
}

#[test]
fn dropping_a_group_that_created_no_namespace_drops_no_namespace() {
	// A drop that removes the group's parent namespace, or fails on a missing test::g, breaks here.
	let t = prepared();
	t.admin(OWN_GROUP);
	t.admin("create table test::keep { id: int4 }");
	t.admin("drop windowed view group test::g");
	assert_eq!(namespaces(&t, "test::g"), 0, "no namespace test::g");
	assert_eq!(namespaces(&t, "test"), 1, "namespace test stays");
	assert_eq!(count(&t, "from system::tables filter {name == 'keep'}"), 1, "table keep stays");
	assert_eq!(views(&t, "g_1"), 0, "member g_1 is gone");
	assert_eq!(views(&t, "g_2"), 0, "member g_2 is gone");
}

#[test]
fn dropping_a_group_drops_a_non_member_deferred_view_inside_its_namespace_together_with_its_flow() {
	// A namespace drop that skips flows leaves the flow of other running over a view that is gone.
	let t = with_group();
	t.admin("create deferred view test::g::other { id: int4 } as { from test::src }");
	t.admin("drop windowed view group test::g");
	assert_eq!(views(&t, "other"), 0, "view other is gone");
	assert_eq!(count(&t, "from system::flows filter {name == 'other'}"), 0, "flow other is gone");
	assert_eq!(count(&t, "from system::flows filter {name == 'g'}"), 0, "flow g is gone");
	assert_eq!(namespaces(&t, "test::g"), 0, "namespace test::g is gone");
}
