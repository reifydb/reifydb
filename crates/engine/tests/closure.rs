// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2025 ReifyDB

use reifydb_core::interface::auth::Identity;
use reifydb_engine::test_utils::create_test_engine;
use reifydb_type::value::frame::frame::Frame;

fn test_identity() -> Identity {
	Identity::root()
}

/// Run an RQL script and return the result frames.
fn run_script(rql: &str) -> Vec<Frame> {
	let engine = create_test_engine();
	let identity = test_identity();
	engine.query_as(&identity, rql, Default::default()).unwrap()
}

/// Extract a single i64 scalar from the first frame's "value" column.
fn scalar_i64(frames: &[Frame]) -> i64 {
	let frame = &frames[0];
	// Try i8 first (small literals), then widen
	if let Ok(Some(v)) = frame.get::<i8>("value", 0) {
		return v as i64;
	}
	if let Ok(Some(v)) = frame.get::<i16>("value", 0) {
		return v as i64;
	}
	if let Ok(Some(v)) = frame.get::<i32>("value", 0) {
		return v as i64;
	}
	frame.get::<i64>("value", 0).unwrap().unwrap()
}

#[test]
fn test_closure_basic_call() {
	let frames = run_script("let $double = ($x) { $x * 2 }; $double(21)");
	assert_eq!(scalar_i64(&frames), 42);
}

#[test]
fn test_closure_capture_from_enclosing_scope() {
	let frames = run_script("let $base = 10; let $adder = ($x) { $x + $base }; $adder(5)");
	assert_eq!(scalar_i64(&frames), 15);
}

#[test]
fn test_closure_no_captures() {
	let frames = run_script("let $id = ($x) { $x }; $id(42)");
	assert_eq!(scalar_i64(&frames), 42);
}

#[test]
fn test_closure_parameter_shadows_capture() {
	let frames = run_script("let $x = 100; let $f = ($x) { $x + 1 }; $f(5)");
	assert_eq!(scalar_i64(&frames), 6);
}

#[test]
fn test_closure_from_function() {
	let frames = run_script("let $n = 5; let $add5 = ($x) { $x + $n }; $add5(10)");
	assert_eq!(scalar_i64(&frames), 15);
}

#[test]
fn test_closure_nested_propagation() {
	let frames = run_script("let $x = 10; let $outer = () { let $inner = () { $x + 1 }; $inner() }; $outer()");
	assert_eq!(scalar_i64(&frames), 11);
}

#[test]
fn test_closure_deep_nesting() {
	let frames = run_script(
		"let $x = 5; let $l1 = () { let $l2 = () { let $l3 = () { $x * 2 }; $l3() }; $l2() }; $l1()",
	);
	assert_eq!(scalar_i64(&frames), 10);
}

#[test]
fn test_closure_nested_with_local_variable() {
	let frames = run_script(
		"let $x = 100; let $outer = () { let $local = 42; let $inner = () { $local }; $inner() }; $outer()",
	);
	assert_eq!(scalar_i64(&frames), 42);
}
