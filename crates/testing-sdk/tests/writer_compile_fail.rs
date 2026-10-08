// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[test]
fn a_column_writer_takes_only_its_own_cell_type() {
	// a text and a blob writer share one buffer layout, so if either takes the other's cells the mistake
	// compiles and the host decodes the column under the wrong type
	trybuild::TestCases::new().compile_fail("tests/writer_compile_fail/*.rs");
}
