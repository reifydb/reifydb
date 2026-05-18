// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{env, process::Command};

use reifydb_abi::operator::capabilities::{CAPABILITY_INSERT, CAPABILITY_UPDATE};
use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::flow::FlowNodeId,
		change::{Change, Diff, Diffs},
	},
	value::column::columns::Columns,
};
use reifydb_sub_flow::operator::capability_guard::enforce_apply_capabilities;
use reifydb_type::value::datetime::DateTime;

const CHILD_ENV: &str = "REIFYDB_CAPABILITY_ABORT_CHILD";
const CHILD_TEST_NAME: &str = "aborts_when_operator_receives_undeclared_diff_kind";

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
		stderr.contains("does not declare the corresponding capability bit"),
		"missing abort message; stderr={}",
		stderr
	);
	assert!(stderr.contains("0x00000002"), "missing CAPABILITY_UPDATE bit (0x02) in message; stderr={}", stderr);
	assert!(stderr.contains("update"), "missing diff kind in message; stderr={}", stderr);
}

fn run_child() {
	tracing_subscriber::fmt().with_writer(std::io::stderr).with_ansi(false).without_time().init();

	let mut diffs = Diffs::new();
	diffs.push(Diff::update(Columns::empty(), Columns::empty()));
	let change = Change::from_flow(FlowNodeId(42), CommitVersion(0), diffs, DateTime::default());

	let caps = CAPABILITY_INSERT;
	assert_eq!(caps & CAPABILITY_UPDATE, 0);

	enforce_apply_capabilities(FlowNodeId(42), caps, &change);

	unreachable!("enforce_apply_capabilities should have aborted on update diff");
}
