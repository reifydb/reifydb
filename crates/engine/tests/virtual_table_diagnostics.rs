// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;

// The resolver hands any name under a system namespace through as a virtual table without consulting
// the catalog, so a misspelled one reaches the compiler, which has no implementation for it. That used
// to panic on the statement's worker thread and take the whole process down, so `from system::version`
// (a typo for `versions`) ended the database for every other client.
//
// These tests pin the diagnostic rather than the mere absence of a crash, so that a rewrite which
// starts resolving these names earlier, or which reports something vaguer, has to change them on
// purpose. The final test is the one that keeps the others honest: the names that do exist must still
// work, or a fix that rejected everything would look just as green.
//
// Only the `system` namespace is covered here: `system::procedures` and `system::bindings` are
// registered by the server, not by this harness, so a query against them fails at namespace
// resolution before it ever reaches the compiler. They take the same path once resolved.

#[test]
fn an_unknown_system_virtual_table_reports_an_error_naming_it() {
	let t = TestEngine::new();

	let err = t.query_err("from system::version");

	assert!(err.contains("CA_023"), "an unknown virtual table must report CA_023, got: {err}");
	assert!(err.contains("system::version"), "the diagnostic must name the table that was not found, got: {err}");
}

#[test]
fn an_unknown_virtual_table_leaves_the_engine_able_to_serve_the_next_statement() {
	// The defect was not that the statement failed, it was that the process died with it. A second
	// statement on the same engine has to still run.
	let t = TestEngine::new();

	let _ = t.query_err("from system::version");

	let frames = t.query("MAP { value: 1 }");
	assert_eq!(frames.len(), 1, "the engine must still answer after an unknown virtual table");
}

#[test]
fn the_virtual_tables_that_exist_still_resolve() {
	let t = TestEngine::new();

	for rql in ["from system::versions", "from system::tables", "from system::namespaces"] {
		let frames = t.query(rql);
		assert_eq!(frames.len(), 1, "`{rql}` must still resolve to a virtual table");
	}
}
