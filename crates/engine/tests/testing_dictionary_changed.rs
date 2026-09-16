// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::params::Params;

fn setup() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE s");
	t.admin("CREATE DICTIONARY s::colors FOR utf8 AS uint2");
	t
}

fn run_only_test(t: &TestEngine, body: &str) -> (String, String) {
	t.admin(&format!("CREATE TEST s::only {{ {body} }}"));
	let result = t.inner().admin_as(TestEngine::identity(), "RUN TESTS s", Params::None);
	assert!(result.error.is_none(), "RUN TESTS itself must not fail, got: {:?}", result.error);
	let rows: Vec<_> = result.frames[0].rows().collect();
	assert_eq!(rows.len(), 1, "exactly the one test must run, got: {}", result.frames[0]);
	(rows[0].get::<String>("outcome").unwrap().unwrap(), rows[0].get::<String>("message").unwrap().unwrap())
}

#[test]
fn a_dictionary_insert_in_a_test_body_is_reported_by_the_changed_procedure() {
	// Without a recorded change the procedure returns no rows and the assertions cannot see the insert.
	let t = setup();
	let (outcome, message) = run_only_test(
		&t,
		"INSERT s::colors [{ value: 'red' }]; testing::dictionaries::changed(\"s::colors\") | ASSERT { op == \"insert\" } | ASSERT { new_value == 'red' }",
	);
	assert_eq!(outcome, "pass", "the insert of red must be reported, got message: {message}");
}

#[test]
fn a_wrong_value_assertion_on_a_dictionary_change_fails() {
	// A pass on the right value must come from a real row, so asserting a different value must fail.
	let t = setup();
	let (outcome, message) = run_only_test(
		&t,
		"INSERT s::colors [{ value: 'red' }]; testing::dictionaries::changed(\"s::colors\") | ASSERT { new_value == 'blue' }",
	);
	assert_eq!(outcome, "fail", "asserting blue on the red insert must fail the assertion, got message: {message}");
}

#[test]
fn inserting_a_value_the_dictionary_already_holds_records_no_change() {
	// Only a newly created entry is a change, so re-inserting red must not show up next to green.
	let t = setup();
	t.command("INSERT s::colors [{ value: 'red' }]");
	let (outcome, message) = run_only_test(
		&t,
		"INSERT s::colors [{ value: 'red' }, { value: 'green' }]; testing::dictionaries::changed(\"s::colors\") | ASSERT { new_value == 'green' }",
	);
	assert_eq!(outcome, "pass", "only the new green entry may be reported, got message: {message}");
}
