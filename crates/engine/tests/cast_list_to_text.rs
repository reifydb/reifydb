// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	env::{current_exe, var_os},
	process::Command,
};

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{params::Params, value::value_type::ValueType};

const CHILD: &str = "REIFYDB_CAST_LIST_TO_TEXT_CHILD";

#[test]
fn casting_a_list_to_utf8_returns_instead_of_overflowing_the_stack() {
	// A stack overflow aborts the whole process, so the query must run in a child process.
	if var_os(CHILD).is_some() {
		let t = TestEngine::new();
		let result = t.inner().query_as(TestEngine::identity(), "map { x: cast([1, 2], utf8) }", Params::None);
		if result.error.is_none() {
			let column = result.frames[0].columns.iter().find(|c| c.name == "x").expect("x column");
			assert_eq!(column.data.get_type(), ValueType::Utf8);
		}
		return;
	}

	let module = module_path!().split_once("::").map(|(_, rest)| rest).unwrap();
	let name = format!("{module}::casting_a_list_to_utf8_returns_instead_of_overflowing_the_stack");
	let output = Command::new(current_exe().unwrap())
		.args([name.as_str(), "--exact", "--nocapture", "--test-threads=1"])
		.env(CHILD, "1")
		.output()
		.unwrap();

	let stdout = String::from_utf8_lossy(&output.stdout);
	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(output.status.success(), "the cast must return, the child ended with {}:\n{stderr}", output.status);
	assert!(stdout.contains("1 passed"), "the child must have run this test:\n{stdout}");
}
