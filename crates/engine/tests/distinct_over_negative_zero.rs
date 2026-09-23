// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

#[test]
fn distinct_merges_zero_and_negative_zero() {
	// 0.0 and -0.0 compare equal and render identically, so distinct must never hand back both.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::f { id: int4, z: float8, sign: float8 }");
	t.command("INSERT test::f [{ id: 1, z: 0.0, sign: 1.0 }, { id: 2, z: 0.0, sign: -1.0 }]");

	let frames = t.query("FROM test::f map { v: z * sign } | distinct { v }");

	assert_eq!(
		TestEngine::row_count(&frames),
		1,
		"distinct over 0.0 and -0.0 must yield one row, got:\n{}",
		frames[0]
	);
}
