// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[test]
fn batch_methods_have_no_per_key_default() {
	// without this, a provided loop comes back and a new store pays one call per key while every other test
	// still passes
	trybuild::TestCases::new().compile_fail("tests/batch_compile_fail/*.rs");
}
