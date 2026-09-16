// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	env::{current_exe, var_os},
	process::Command,
};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::value::frame::frame::Frame;

const CHILD: &str = "REIFYDB_PROCEDURE_NAMED_LIKE_A_BUILTIN_CHILD";

fn r_text(frames: &[Frame]) -> Vec<String> {
	assert_eq!(frames.len(), 1, "expected exactly one frame, got {}", frames.len());
	let column = frames[0].columns.iter().find(|c| c.name == "r").expect("r column");
	(0..column.data.len()).map(|i| column.data.as_string(i)).collect()
}

#[test]
fn a_procedure_named_like_a_builtin_reads_a_variable_where_the_builtin_takes_a_type() {
	// The procedure takes plain utf8, so it must never borrow the builtin's type position.
	if var_os(CHILD).is_some() {
		let t = TestEngine::new();
		t.admin("CREATE NAMESPACE ns");
		t.admin("CREATE NAMESPACE is");
		t.admin("CREATE PROCEDURE ns::echo { value: utf8, kind: utf8 } AS { map { r: $kind } }");
		t.admin("CREATE PROCEDURE is::type { value: utf8, kind: utf8 } AS { map { r: $kind } }");

		let plain =
			r_text(&t.command("let $duration = 'slow'; let $r = ns::echo('a', duration); map { r: $r }"));
		assert_eq!(plain, vec!["slow"], "a bare name at a value position must load the variable");
		let shadowing =
			r_text(&t.command("let $duration = 'slow'; let $r = is::type('a', duration); map { r: $r }"));
		assert_eq!(
			shadowing, plain,
			"a procedure named is::type must bind its arguments like any other procedure"
		);
		return;
	}

	let module = module_path!().split_once("::").map(|(_, rest)| rest).unwrap();
	let name =
		format!("{module}::a_procedure_named_like_a_builtin_reads_a_variable_where_the_builtin_takes_a_type");
	let output = Command::new(current_exe().unwrap())
		.args([name.as_str(), "--exact", "--nocapture", "--test-threads=1"])
		.env(CHILD, "1")
		.output()
		.unwrap();

	let stdout = String::from_utf8_lossy(&output.stdout);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(output.status.success(), "the child ended with {}:\n{stdout}\n{stderr}", output.status);
	assert!(stdout.contains("1 passed"), "the child must have run this test:\n{stdout}");
}
