// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::{queue::Queue, view::View};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::params::Params;

const SETUP: [&str; 6] = [
	"CREATE NAMESPACE test",
	"CREATE NAMESPACE members",
	"CREATE TABLE test::dep_src { id: int4 }",
	"CREATE TABLE test::dep_events { id: int4, ts: datetime, value: int4 } WITH { time: event(ts) }",
	"CREATE EVENT test::dep_event { Fired { n: int4 } }",
	"CREATE PROCEDURE test::dep_proc AS { \"A\" }",
];

const RECIPES: [(&str, &str, &str); 13] = [
	("table", "table", "CREATE TABLE test::x { id: int4 }"),
	("ring buffer", "ring buffer", "CREATE RINGBUFFER test::x { id: int4 } WITH { capacity: 4 }"),
	("deferred view", "view", "CREATE DEFERRED VIEW test::x { id: int4 } AS { FROM test::dep_src }"),
	("dictionary", "dictionary", "CREATE DICTIONARY test::x FOR utf8 AS uint4"),
	("series", "series", "CREATE SERIES test::x { k: int8, v: int4 } WITH { key: k }"),
	("queue", "queue", "CREATE QUEUE test::x { id: int4 } WITH { fifo: {} }"),
	("enum", "enum or event", "CREATE ENUM test::x { Active, Gone }"),
	("event", "enum or event", "CREATE EVENT test::x { Fired { n: int4 } }"),
	("procedure", "procedure", "CREATE PROCEDURE test::x AS { \"A\" }"),
	(
		"sink",
		"sink",
		"CREATE SINK test::x AS { FROM test::dep_src TO postgres { uri: \"postgres://h/db\", table: \"t\" } }",
	),
	(
		"source",
		"source",
		"CREATE SOURCE test::x AS { FROM postgres { uri: \"postgres://h/db\", query: \"q\" } TO test::dep_src }",
	),
	("test", "test", "CREATE TEST test::x { CALL test::dep_proc() }"),
	("binding", "binding", "CREATE GRPC BINDING test::x FOR test::dep_proc WITH { name: \"x_g\" }"),
];

const QUEUE_VIEW: &str = r#"CREATE DEFERRED QUEUE VIEW test::q { id: int4, kind: utf8 } WITH {
		partition: { by: { kind } },
		fifo: { ordered_by: id },
		retention: { done: 7d },
		retry: { attempts: 9, backoff: 30s }
	} AS { from test::src }"#;

const NAME_BINDING: &str =
	"CREATE HTTP BINDING test::b FOR test::greet WITH { method: \"GET\", path: \"/a\", format: \"json\" }";

const BINDING_PAIRS: [(&str, &str, &str, &[&str]); 4] = [
	("name", NAME_BINDING, "CREATE GRPC BINDING test::b FOR test::greet WITH { name: \"b_g\" }", &["b"]),
	(
		"http",
		"CREATE HTTP BINDING test::b1 FOR test::greet WITH { method: \"GET\", path: \"/same\", format: \"json\" }",
		"CREATE HTTP BINDING test::b2 FOR test::greet WITH { method: \"GET\", path: \"/same\", format: \"json\" }",
		&["b1", "b2"],
	),
	(
		"grpc",
		"CREATE GRPC BINDING test::b1 FOR test::greet WITH { name: \"same\" }",
		"CREATE GRPC BINDING test::b2 FOR test::greet WITH { name: \"same\" }",
		&["b1", "b2"],
	),
	(
		"ws",
		"CREATE WS BINDING test::b1 FOR test::greet WITH { name: \"same\" }",
		"CREATE WS BINDING test::b2 FOR test::greet WITH { name: \"same\" }",
		&["b1", "b2"],
	),
];

fn binding_key_text(case: &str) -> &'static str {
	match case {
		"name" => "::b` already exists",
		"http" => "::GET /same` already exists",
		_ => "::same` already exists",
	}
}

fn prepared() -> TestEngine {
	let t = TestEngine::new();
	for statement in SETUP {
		t.admin(statement);
	}
	t
}

fn with_greet() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE PROCEDURE test::greet AS { \"hi\" }");
	t
}

fn refused(t: &TestEngine, rql: &str) -> (String, String) {
	let result = t.inner().admin_as(TestEngine::identity(), rql, Params::None);
	let error = result.error.unwrap_or_else(|| panic!("must be refused: {rql}"));
	(error.code.clone(), error.message.clone())
}

fn assert_taken_by(t: &TestEngine, rql: &str, kind: &str) {
	let (code, message) = refused(t, rql);
	assert_eq!(code, "CA_137", "{rql}: {message}");
	assert!(message.ends_with(&format!("is already taken by a {kind}")), "{rql}: {message}");
}

fn count(t: &TestEngine, rql: &str) -> usize {
	TestEngine::row_count(&t.query(rql))
}

fn find(t: &TestEngine, name: &str) -> (Option<View>, Option<Queue>) {
	let catalog = t.inner().catalog();
	let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
	let mut txn = Transaction::Query(&mut query_txn);
	let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
	let view = catalog.find_view_by_name(&mut txn, namespace.id(), name).unwrap();
	let queue = catalog.find_queue_by_name(&mut txn, namespace.id(), name).unwrap();
	(view, queue)
}

fn bindings_named(t: &TestEngine, names: &[&str]) -> usize {
	names.iter().map(|name| count(t, &format!("from system::bindings filter {{name == '{name}'}}"))).sum()
}

#[test]
fn a_name_held_by_one_kind_is_refused_for_every_other_kind() {
	// Without the check in the second kind's create fn the second object is made under a taken name.
	for (first, first_text, first_rql) in RECIPES {
		for (second, second_text, second_rql) in RECIPES {
			if first == second || (first_text == "enum or event" && second_text == "enum or event") {
				continue;
			}
			let t = prepared();
			t.admin(first_rql);
			let (code, message) = refused(&t, second_rql);
			assert_eq!(code, "CA_137", "{first} then {second}: {message}");
			assert!(
				message.ends_with(&format!("is already taken by a {first_text}")),
				"{first} then {second}: {message}"
			);
		}
	}
}

#[test]
fn a_queue_view_still_creates_its_queue_under_its_own_name() {
	// Without the queue partner the view's own queue is refused under the view's name.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::src { id: int4, kind: utf8 } WITH { time: processing }");
	t.admin(QUEUE_VIEW);
	let (view, queue) = find(&t, "q");
	assert!(view.is_some(), "view q must exist");
	assert!(queue.is_some(), "queue q must exist");
}

#[test]
fn a_queue_view_name_is_refused_for_other_kinds_and_takes_none_of_theirs() {
	// A wrong probe order names the queue; a partner rule without the id compare lets a queue view over a table.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::src { id: int4, kind: utf8 } WITH { time: processing }");
	t.admin(QUEUE_VIEW);
	for rql in [
		"CREATE TABLE test::q { id: int4 }",
		"CREATE RINGBUFFER test::q { id: int4 } WITH { capacity: 4 }",
		"CREATE SERIES test::q { k: int8, v: int4 } WITH { key: k }",
		"CREATE DICTIONARY test::q FOR utf8 AS uint4",
	] {
		assert_taken_by(&t, rql, "view");
	}

	t.admin("CREATE TABLE test::t { id: int4 }");
	assert_taken_by(&t, &QUEUE_VIEW.replace("test::q", "test::t"), "table");
	let (_, queue) = find(&t, "t");
	assert!(queue.is_none(), "a refused queue view must leave no queue behind");
}

#[test]
fn a_same_kind_duplicate_keeps_its_own_error() {
	// A cross-kind check that does not skip its own kind replaces the same-kind code.
	let t = prepared();
	let group = "create deferred windowed view group test::win { c: int8 } with { members: [ { view: test::w5, duration: 5s, pane: 1s } ] } as { from test::dep_events window rolling { c: math::count(id) } }";
	for (first, second, code) in [
		("CREATE TABLE test::x { id: int4 }", "CREATE TABLE test::x { id: int4 }", "CA_003"),
		(
			"CREATE DEFERRED VIEW test::v { id: int4 } AS { FROM test::dep_src }",
			"CREATE DEFERRED VIEW test::v { id: int4 } AS { FROM test::dep_src }",
			"CA_003",
		),
		("CREATE NAMESPACE test::n", "CREATE NAMESPACE test::n", "CA_001"),
		(group, &group.replace("test::w5", "test::w6"), "CA_134"),
	] {
		t.admin(first);
		let (got, message) = refused(&t, second);
		assert_eq!(got, code, "{second}: {message}");
	}
}

#[test]
fn a_dropped_name_is_free_for_another_kind() {
	// A probe that reads a stale row refuses a name its holder gave up.
	let t = prepared();
	t.admin("CREATE TABLE test::x { id: int4 }");
	t.admin("DROP TABLE test::x");
	t.admin("CREATE RINGBUFFER test::x { id: int4 } WITH { capacity: 4 }");
	t.admin("CREATE NAMESPACE test::n");
	t.admin("DROP NAMESPACE test::n");
	t.admin("CREATE TABLE test::n { id: int4 }");
}

#[test]
fn a_handler_still_creates_its_procedure_and_a_view_its_flow() {
	// Handler and procedure must skip each other, and every flow caller must pass its partner, or these creates
	// fail.
	let t = prepared();
	t.admin("CREATE TABLE test::src { id: int4, kind: utf8 } WITH { time: processing }");
	t.admin("CREATE HANDLER test::h ON test::dep_event::Fired { FROM test::dep_src }");
	assert_eq!(count(&t, "from system::procedures filter {name == 'h'}"), 1, "the handler's procedure h");
	assert_eq!(count(&t, "from system::handlers filter {name == 'h'}"), 1, "the handler h");

	t.admin("CREATE DEFERRED VIEW test::d { id: int4 } AS { FROM test::dep_src }");
	t.admin("CREATE TRANSACTIONAL VIEW test::tv { id: int4 } AS { FROM test::dep_src }");
	t.admin(QUEUE_VIEW);
	t.admin("create deferred windowed view group test::g { c: int8 } with { members: [ { view: test::g::m1, duration: 5s, pane: 1s }, { view: test::g::m2, duration: 10s, pane: 1s } ] } as { from test::dep_events window rolling { c: math::count(id) } }");
	for name in ["d", "tv", "q", "g"] {
		assert_eq!(count(&t, &format!("from system::flows filter {{name == '{name}'}}")), 1, "flow {name}");
	}
	assert_eq!(count(&t, "from system::namespaces filter {name == 'test::g'}"), 1, "the group namespace");
}

#[test]
fn an_object_and_a_namespace_under_one_name_are_refused_in_both_orders() {
	// Without the namespace check or the namespace probe one order lets an object and a namespace share a name.
	let t = prepared();
	t.admin("CREATE TABLE test::x { id: int4 }");
	assert_taken_by(&t, "CREATE NAMESPACE test::x", "table");
	t.admin("CREATE NAMESPACE test::y");
	assert_taken_by(&t, "CREATE TABLE test::y { id: int4 }", "namespace");
	t.admin("CREATE TABLE test::z { id: int4 }");
	assert_taken_by(&t, "CREATE NAMESPACE test::z::deep", "table");
}

#[test]
fn if_not_exists_forgives_only_the_same_kind() {
	// An engine site that reads IF NOT EXISTS as a pass for any kind swallows the refusal.
	let t = prepared();
	t.admin("CREATE TABLE test::x { id: int4 }");
	t.admin("CREATE TABLE IF NOT EXISTS test::x { id: int4 }");
	assert_eq!(count(&t, "from system::tables filter {name == 'x'}"), 1, "the table stays one");
	assert_taken_by(&t, "CREATE DICTIONARY IF NOT EXISTS test::x FOR utf8 AS uint4", "table");
	assert_taken_by(&t, "CREATE NAMESPACE IF NOT EXISTS test::x", "table");
}

#[test]
fn a_group_member_named_like_its_group_is_refused() {
	// A group flow that passes a view as partner lets a member take the group's name.
	let t = prepared();
	assert_taken_by(
		&t,
		"create deferred windowed view group test::g { c: int8 } with { members: [ { view: test::g, duration: 5s, pane: 1s } ] } as { from test::dep_events window rolling { c: math::count(id) } }",
		"view",
	);
	assert_eq!(count(&t, "from system::windowed_view_groups filter {name == 'g'}"), 0, "no group g");
	assert_eq!(count(&t, "from system::views filter {name == 'g'}"), 0, "no view g");
	assert_eq!(count(&t, "from system::flows filter {name == 'g'}"), 0, "no flow g");
}

#[test]
fn a_second_test_of_one_name_is_refused() {
	// A test check that skips its own kind lets two tests share one name.
	let statement = "CREATE TEST test::t { FROM test::items | ASSERT { id > 0 } }";
	let setup = |t: &TestEngine| {
		t.admin("CREATE NAMESPACE test");
		t.admin("CREATE TABLE test::items { id: int4 }");
	};
	let tests_named_t = |t: &TestEngine| {
		let catalog = t.inner().catalog();
		let mut query_txn = t.inner().begin_query(TestEngine::identity()).unwrap();
		let mut txn = Transaction::Query(&mut query_txn);
		let namespace = catalog.find_namespace_by_name(&mut txn, "test").unwrap().unwrap();
		catalog.list_tests_in_namespace(&mut txn, namespace.id())
			.unwrap()
			.iter()
			.filter(|x| x.name == "t")
			.count()
	};

	let t = TestEngine::new();
	setup(&t);
	t.admin(statement);
	assert_taken_by(&t, statement, "test");
	assert_eq!(tests_named_t(&t), 1, "exactly one test t after a refused second one");

	let t = TestEngine::new();
	setup(&t);
	assert_taken_by(&t, &format!("{statement}; {statement}"), "test");
	assert_eq!(tests_named_t(&t), 0, "a refused command leaves no test t");
}

#[test]
fn two_bindings_of_one_name_or_one_protocol_key_in_one_admin_command_are_refused() {
	// A lookup that reads only the committed cache misses the first binding of the same command.
	for (case, first, second, names) in BINDING_PAIRS {
		let t = with_greet();
		let (code, message) = refused(&t, &format!("{first}; {second}"));
		assert_eq!(code, "CA_087", "{case}: {message}");
		assert!(message.ends_with(binding_key_text(case)), "{case}: {message}");
		assert_eq!(bindings_named(&t, names), 0, "{case}: a refused command leaves no binding");
	}
}

#[test]
fn a_second_binding_of_one_name_or_key_in_a_later_command_is_still_refused() {
	// A lookup that reads only uncommitted rows misses a binding an earlier command committed.
	for (case, first, second, names) in BINDING_PAIRS {
		let t = with_greet();
		t.admin(first);
		let (code, message) = refused(&t, second);
		assert_eq!(code, "CA_087", "{case}: {message}");
		assert!(message.ends_with(binding_key_text(case)), "{case}: {message}");
		assert_eq!(bindings_named(&t, names), 1, "{case}: one binding is left");
	}
}

#[test]
fn a_binding_dropped_and_made_again_in_one_command_is_allowed_and_two_protocols_share_a_name() {
	// A lookup that reads the cache refuses the re-create; one that ignores the protocol variant refuses ws plus
	// grpc.
	let t = with_greet();
	t.admin(NAME_BINDING);
	t.admin(&format!("DROP BINDING test::b; {NAME_BINDING}"));
	assert_eq!(bindings_named(&t, &["b"]), 1, "one binding b");

	let t = with_greet();
	t.admin(
		"CREATE WS BINDING test::w FOR test::greet WITH { name: \"same\" }; CREATE GRPC BINDING test::g FOR test::greet WITH { name: \"same\" }",
	);
	assert_eq!(bindings_named(&t, &["w", "g"]), 2, "a ws and a grpc binding share one key name");
}
