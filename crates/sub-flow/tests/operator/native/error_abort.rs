// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Regression #9: operators are not meant to fail. An `Err` from a native
// operator's apply must ABORT the process (via `run_or_abort`), not propagate as
// a Result. Driven through the native harness; forked because abort kills the
// process. The FFI counterpart aborts in the `ffi_apply` export instead - see
// ffi/error_abort.rs.

use std::{env, process::Command};

use super::Harness;
use crate::common::{ErroringOperator, trigger};

const CHILD_ENV: &str = "REIFYDB_OPERATOR_ERROR_ABORT_NATIVE_CHILD";
const CHILD_TEST: &str = "native::error_abort::apply_error_aborts";

#[test]
fn apply_error_aborts() {
	if env::var(CHILD_ENV).is_ok() {
		let mut harness = Harness::<ErroringOperator>::builder().build().expect("harness build");
		let _ = harness.apply(trigger());
		eprintln!("native apply returned instead of aborting");
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
