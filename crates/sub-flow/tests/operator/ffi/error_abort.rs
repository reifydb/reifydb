// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

// Regression #9: operators are not meant to fail. An `Err` from an operator's
// apply must ABORT the process. On the FFI backend the abort lives in the
// `ffi_apply` EXPORT (the `.so` boundary), not in `FFIOperatorAdapter::apply`,
// so this drives the operator through `drive_ffi_apply` rather than the
// in-process harness. Forked because abort kills the process. The native
// counterpart aborts in `run_or_abort` - see native/error_abort.rs.

use std::{env, process::Command};

use reifydb_sdk::{operator::FFIOperatorAdapter, testing::harness::drive_ffi_apply};

use crate::common::{ErroringOperator, trigger};

const CHILD_ENV: &str = "REIFYDB_OPERATOR_ERROR_ABORT_FFI_CHILD";
const CHILD_TEST: &str = "ffi::error_abort::apply_error_aborts";

#[test]
fn apply_error_aborts() {
	if env::var(CHILD_ENV).is_ok() {
		let _ = drive_ffi_apply::<FFIOperatorAdapter<ErroringOperator>>(&trigger());
		eprintln!("ffi_apply returned instead of aborting");
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
