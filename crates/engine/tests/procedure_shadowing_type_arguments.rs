// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::frame::frame::Frame;

const SCRIPT: &str = "let $duration = 'slow'; let $r = is::type('a', duration); map { r: $r }";

fn r_text(frames: &[Frame]) -> Vec<String> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0].columns.iter().find(|c| c.name == "r").expect("r column");
	(0..column.data.len()).map(|i| column.data.as_string(i)).collect()
}

#[test]
fn a_type_argument_binds_by_the_routine_that_exists_when_the_call_runs() {
	// The same script text must follow the procedure as it appears and disappears, never keep an earlier binding.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE is");

	assert_eq!(r_text(&t.command(SCRIPT)), vec!["false"], "the builtin must read duration as the type");

	t.admin("CREATE PROCEDURE is::type { value: utf8, kind: utf8 } AS { map { r: $kind } }");
	assert_eq!(r_text(&t.command(SCRIPT)), vec!["slow"], "the procedure must read duration as the variable");

	t.admin("DROP PROCEDURE is::type");
	assert_eq!(r_text(&t.command(SCRIPT)), vec!["false"], "the builtin must take the type again once dropped");
}

#[test]
fn a_procedure_named_like_a_builtin_rejects_an_undefined_name_at_the_builtin_type_position() {
	// A shadowing procedure must fail on an undefined variable, never receive the builtin's type.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE is");
	t.admin("CREATE PROCEDURE is::type { value: utf8, kind: utf8 } AS { map { r: $kind } }");

	let err = t.command_err("let $r = is::type('a', utf8); map { r: $r }");

	assert!(err.contains("RUNTIME_001"), "utf8 is not a variable, so the call must fail to load it, got: {err}");
	assert!(err.contains("Variable 'utf8' is not defined"), "got: {err}");
}
