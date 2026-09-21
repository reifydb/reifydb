// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_value::{
	params::Params,
	value::{frame::frame::Frame, identity::IdentityId},
};

fn columns(frames: &[Frame]) -> Vec<Vec<String>> {
	frames.iter().map(|f| f.columns.iter().map(|c| c.name.clone()).collect()).collect()
}

fn in_transaction(t: &TestEngine, rql: &str) -> Vec<Frame> {
	let mut txn = t.inner().begin_command(IdentityId::system()).unwrap();
	let r = txn.rql(rql, Params::None);
	if let Some(e) = r.error {
		panic!("rql failed: {e:?}\nrql: {rql}")
	}
	r.frames
}

fn every_path(t: &TestEngine, rql: &str) -> Vec<(&'static str, Vec<Vec<String>>)> {
	vec![
		("query", columns(&t.query(rql))),
		("command", columns(&t.command(rql))),
		("admin", columns(&t.admin(rql))),
		("rql", columns(&in_transaction(t, rql))),
	]
}

fn names(frames: &[&[&str]]) -> Vec<Vec<String>> {
	frames.iter().map(|f| f.iter().map(|c| c.to_string()).collect()).collect()
}

#[test]
fn any_output_statement_returns_only_output_frames_in_order() {
	// A caller that marks statements output must not also get the last frame, or its shape tuple stops lining up.
	let t = TestEngine::new();
	for (path, frames) in every_path(&t, "OUTPUT MAP {one: 1}; MAP {two: 2}; OUTPUT MAP {three: 3}; MAP {four: 4}")
	{
		assert_eq!(frames, names(&[&["one"], &["three"]]), "path: {path}");
	}
}

#[test]
fn no_output_statement_returns_only_the_last_frame() {
	// Without an output marker the result must stay the last frame, otherwise plain multi-statement writes change
	// shape.
	let t = TestEngine::new();
	for (path, frames) in every_path(&t, "MAP {one: 1}; MAP {two: 2}") {
		assert_eq!(frames, names(&[&["two"]]), "path: {path}");
	}
}

#[test]
fn a_trailing_output_statement_is_returned_once() {
	// The last statement is both output and last, so counting it twice would hand the caller a duplicate frame.
	let t = TestEngine::new();
	for (path, frames) in every_path(&t, "MAP {one: 1}; OUTPUT MAP {two: 2}") {
		assert_eq!(frames, names(&[&["two"]]), "path: {path}");
	}
}

#[test]
fn a_failing_later_statement_rolls_back_and_returns_no_frames() {
	// An error after an output insert must not leak its frame or its row, or the caller sees a write that never
	// committed.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { id: int4 }");
	let rql = "OUTPUT INSERT test::t [{ id: 1 }]; ASSERT { 1 == 2 } \"later statement fails\"";
	let command = t.inner().command_as(IdentityId::system(), rql, Params::None);
	let admin = t.inner().admin_as(IdentityId::system(), rql, Params::None);
	for (path, r) in [("command", command), ("admin", admin)] {
		assert!(r.error.is_some(), "path {path}: the failing assert must fail the batch");
		assert!(r.frames.is_empty(), "path {path}: a failed batch must return no frames");
	}
	assert_eq!(TestEngine::row_count(&t.query("FROM test::t")), 0, "the insert before the failure must roll back");
}
