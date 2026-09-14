// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

fn engine() -> TestEngine {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::t { a: int4, b: int4 }");
	t.command("INSERT test::t [{ a: 2, b: 1 }, { a: 1, b: 2 }]");
	t
}

#[test]
fn sorting_by_a_list_column_reports_an_error_instead_of_panicking() {
	// Value ordering treats a list compare as unreachable, so sort must reject the column before comparing.
	let t = engine();

	let err = t.query_err("FROM test::t | extend { l: [a, b] } | sort { l }");

	assert!(err.contains("SORT_002"), "sorting by a list column must report SORT_002, got: {err}");
}

#[test]
fn top_k_by_a_list_column_reports_an_error_instead_of_panicking() {
	// Sort followed by take runs the top-k node, which compares values on its own path.
	let t = engine();

	let err = t.query_err("FROM test::t | extend { l: [a, b] } | sort { l } | take 1");

	assert!(err.contains("SORT_002"), "top-k by a list column must report SORT_002, got: {err}");
}

#[test]
fn sorting_by_an_optional_list_column_reports_an_error_instead_of_panicking() {
	// Wrapping a list in Option does not make it orderable, so the check must look inside the Option.
	let t = engine();
	t.command("INSERT test::t [{ a: 3, b: 4 }]");

	let err = t.query_err("FROM test::t | extend { l: if a > 1 { [a, b] } else { none } } | sort { l }");

	assert!(err.contains("SORT_002"), "sorting by an optional list column must report SORT_002, got: {err}");
}
