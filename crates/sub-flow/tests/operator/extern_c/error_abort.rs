// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{env, process::Command};

use reifydb_sdk::flow::operator::NostateMount;
use reifydb_test_harness::operator::change::trigger;

use super::Harness;
use crate::common::ErroringOperator;

const CHILD_ENV: &str = "REIFYDB_OPERATOR_ERROR_ABORT_EXTERN_C_CHILD";
const CHILD_TEST: &str = "extern_c::error_abort::apply_error_aborts";

#[test]
fn apply_error_aborts() {
	// The abort lives in the extern_c_apply export, so this must drive the production handle, never a stand-in.
	if env::var(CHILD_ENV).is_ok() {
		let mut harness = Harness::<NostateMount<ErroringOperator>>::builder().build().expect("harness build");
		let _ = harness.apply(trigger());
		eprintln!("extern_c_apply returned instead of aborting");
		return;
	}

	let exe = env::current_exe().expect("current_exe");
	let output = Command::new(&exe)
		.args(["--exact", CHILD_TEST, "--nocapture"])
		.env(CHILD_ENV, "1")
		.output()
		.expect("spawn child");

	assert!(
		!output.status.success(),
		"child should have aborted; stdout={:?} stderr={:?}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr),
	);
}
