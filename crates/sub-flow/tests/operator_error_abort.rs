// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Regression #9: operators are not meant to fail. An `Err` returned from a
// dual-build operator's `apply` must ABORT the process on BOTH backends (the FFI
// guest already aborts on a non-zero code; the native backend used to propagate
// the Err as a Result). This pins the native side: an erroring operator aborts
// rather than returning cleanly. Tested via subprocess fork because abort kills
// the process (a swap-runner would take down the whole flow test binary).

use std::{collections::HashMap, env, process::Command};

use fixtures::{NODE_ID, deferred_txn, engine};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diffs},
	},
};
use reifydb_sdk::{
	error::{Result as SdkResult, SdkError},
	operator::{OperatorLogic, context::OperatorContext, view::ChangeView},
};
use reifydb_sub_flow::operator::{Operator, native::NativeOperator};
use reifydb_type::value::{Value, datetime::DateTime};

#[path = "state/fixtures.rs"]
mod fixtures;

const CHILD_ENV: &str = "REIFYDB_OPERATOR_ERROR_ABORT_CHILD";
const CHILD_TEST_NAME: &str = "native_operator_apply_error_aborts";

// Always fails in apply, so the backend's error handling is the only thing under test.
struct ErroringOperator;

impl OperatorLogic for ErroringOperator {
	fn create(_id: FlowNodeId, _config: &HashMap<String, Value>) -> SdkResult<Self> {
		Ok(ErroringOperator)
	}

	fn apply(&mut self, _ctx: &mut impl OperatorContext, _change: impl ChangeView) -> SdkResult<()> {
		Err(SdkError::Other("operator apply must abort, not return Err".to_string()))
	}
}

#[test]
fn native_operator_apply_error_aborts() {
	if env::var(CHILD_ENV).is_ok() {
		run_child();
		return;
	}

	let exe = env::current_exe().expect("current_exe");
	let output = Command::new(&exe)
		.args(["--exact", CHILD_TEST_NAME, "--nocapture"])
		.env(CHILD_ENV, "1")
		.output()
		.expect("spawn child");

	assert!(
		!output.status.success(),
		"native operator that returned Err must abort, but the child exited cleanly; stdout={:?} stderr={:?}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr),
	);

	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(stderr.contains("native operator"), "missing native-operator abort message; stderr={}", stderr);
	assert!(stderr.contains("aborting"), "missing abort marker in message; stderr={}", stderr);
}

fn run_child() {
	tracing_subscriber::fmt().with_writer(std::io::stderr).with_ansi(false).without_time().init();

	let e = engine();
	let mut txn = deferred_txn(&e);
	let op = NativeOperator::new(ErroringOperator, NODE_ID, 0);
	let change = Change::from_flow(NODE_ID, CommitVersion(1), Diffs::new(), DateTime::from_nanos(0));

	let _ = op.apply(&mut txn, change);

	// Reached only if apply returned instead of aborting (the pre-fix behavior).
	// The child then exits 0, and the parent assertion above fails (RED).
}
