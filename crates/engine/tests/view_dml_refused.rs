// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

const VIEWS: [(&str, &str); 5] = [
	("deferred table view", "create deferred view test::v { id: int4 } as { from test::src }"),
	(
		"deferred ring buffer view",
		"create deferred ringbuffer view test::v { id: int4 } with { capacity: 100 } as { from test::src }",
	),
	(
		"deferred series view",
		"create deferred series view test::v { id: int4 } with { precision: millisecond } as { from test::src }",
	),
	(
		"deferred queue view",
		"create deferred queue view test::v { id: int4 } with { fifo: {} } as { from test::src }",
	),
	("transactional view", "create transactional view test::v { id: int4 } as { from test::src }"),
];

fn prepared(view: &str) -> TestEngine {
	let t = TestEngine::new();
	t.admin("create namespace test");
	t.admin("create table test::src { id: int4 } with { time: processing }");
	t.admin(view);
	t
}

fn assert_view_write_refused(t: &TestEngine, case: &str, rql: &str, verb: &str) {
	let result = t.inner().command_as(TestEngine::identity(), rql, Params::None);
	let error = result.error.unwrap_or_else(|| panic!("{case}: must be refused: {rql}"));
	assert_eq!(error.code, "VIEW_001", "{case}: {}", error.message);
	assert!(error.message.contains("written only by its flow"), "{case}: {}", error.message);
	assert!(error.message.ends_with(&format!("{verb} is not allowed")), "{case}: {}", error.message);
}

#[test]
fn insert_into_any_view_is_refused_as_a_view_write() {
	// Without the view check each storage kind answers with its own unrelated code.
	for (case, view) in VIEWS {
		assert_view_write_refused(&prepared(view), case, "INSERT test::v [{ id: 1 }]", "INSERT");
	}
}

#[test]
fn update_of_any_view_is_refused_as_a_view_write() {
	// Without the view check each storage kind answers with its own unrelated code.
	for (case, view) in VIEWS {
		assert_view_write_refused(&prepared(view), case, "UPDATE test::v { id: 2 } FILTER {id == 1}", "UPDATE");
	}
}

#[test]
fn delete_from_any_view_is_refused_as_a_view_write() {
	// Without the view check each storage kind answers with its own unrelated code.
	for (case, view) in VIEWS {
		assert_view_write_refused(&prepared(view), case, "DELETE test::v FILTER {id == 1}", "DELETE");
	}
}

#[test]
fn an_ordinal_insert_into_a_view_is_refused_as_a_view_write() {
	// A view check placed after the source compile answers with the ordinal arity error.
	let t = prepared(VIEWS[0].1);
	assert_view_write_refused(&t, "ordinal", "INSERT test::v [(1)]", "INSERT");
}

#[test]
fn an_insert_with_options_into_a_view_is_refused_as_a_view_write() {
	// A view check placed after the options check answers with the queue-only options error.
	let t = prepared(VIEWS[0].1);
	assert_view_write_refused(
		&t,
		"options",
		"INSERT test::v [{ id: 1 }] WITH { deduplication_key: \"k\" }",
		"INSERT",
	);
}

#[test]
fn dml_on_plain_objects_still_works_next_to_a_view() {
	// A view rule that matches too much refuses writes to plain objects.
	let t = prepared(VIEWS[0].1);
	t.admin("create table test::t { id: int4 }");
	t.admin("create ringbuffer test::r { id: int4 } with { capacity: 4 }");
	t.admin("create series test::s { k: int8, v: int4 } with { key: k }");
	t.admin("create queue test::q { id: int4 } with { fifo: {} }");

	t.command("INSERT test::t [{ id: 1 }]");
	t.command("UPDATE test::t { id: 2 } FILTER {id == 1}");
	t.command("DELETE test::t FILTER {id == 2}");
	t.command("INSERT test::r [{ id: 1 }]");
	t.command("UPDATE test::r { id: 2 } FILTER {id == 1}");
	t.command("DELETE test::r FILTER {id == 2}");
	t.command("INSERT test::s [{ k: 1, v: 1 }]");
	t.command("INSERT test::q [{ id: 1 }]");

	let result =
		t.inner().command_as(TestEngine::identity(), "UPDATE test::q { id: 2 } FILTER {id == 1}", Params::None);
	let error = result.error.expect("UPDATE on a queue must stay refused");
	assert_eq!(error.code, "QUEUE_001", "{}", error.message);
}
