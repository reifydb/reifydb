// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

// Backend-agnostic: `enforce_apply_capabilities` runs in `Operators::apply` (the
// dispatch wrapper), the same code regardless of FFI vs native adapter, so there
// is nothing backend-specific to twin. An operator that receives a diff kind it
// did not declare must abort. Forked because abort kills the process.

use std::{env, process::Command};

use reifydb_abi::operator::capabilities::OperatorCapability;
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff, Diffs},
	},
	value::column::columns::Columns,
};
use reifydb_sub_flow::operator::guard::enforce_apply_capabilities;
use reifydb_value::value::datetime::DateTime;

const CHILD_ENV: &str = "REIFYDB_CAPABILITY_ABORT_CHILD";
const CHILD_TEST_NAME: &str = "capability_abort::aborts_when_operator_receives_undeclared_diff_kind";

#[test]
fn aborts_when_operator_receives_undeclared_diff_kind() {
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
		"child should have aborted; stdout={:?} stderr={:?}",
		String::from_utf8_lossy(&output.stdout),
		String::from_utf8_lossy(&output.stderr),
	);

	let stderr = String::from_utf8_lossy(&output.stderr);
	assert!(
		stderr.contains("does not declare the corresponding capability"),
		"missing abort message; stderr={}",
		stderr
	);
	assert!(stderr.contains("Update"), "missing Update capability in message; stderr={}", stderr);
	assert!(stderr.contains("update"), "missing diff kind in message; stderr={}", stderr);
}

fn run_child() {
	tracing_subscriber::fmt().with_writer(std::io::stderr).with_ansi(false).without_time().init();

	let mut diffs = Diffs::new();
	diffs.push(Diff::update(Columns::empty(), Columns::empty()));
	let change = Change::from_flow(FlowNodeId(42), CommitVersion(0), diffs, DateTime::default());

	let caps = &[OperatorCapability::Insert];
	assert!(!caps.contains(&OperatorCapability::Update));

	enforce_apply_capabilities(FlowNodeId(42), caps, &change);

	unreachable!("enforce_apply_capabilities should have aborted on update diff");
}
