// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;

#[test]
fn test_read_only_rejects_admin() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::items { id: int8, name: utf8 }");

	t.inner().set_read_only();
	assert!(t.inner().is_read_only());

	let err = t.admin_err("CREATE TABLE test::other { id: int8 }");
	assert!(err.contains("ENG_007"), "expected ENG_007, got: {err}");
}

#[test]
fn test_read_only_rejects_command() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::items { id: int8, name: utf8 }");

	t.inner().set_read_only();

	let err = t.command_err("INSERT test::items [{ id: 1, name: 'hello' }]");
	assert!(err.contains("ENG_007"), "expected ENG_007, got: {err}");
}

#[test]
fn test_read_only_allows_query() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::items { id: int8, name: utf8 }");
	t.command("INSERT test::items [{ id: 1, name: 'hello' }]");

	t.inner().set_read_only();

	// Query should still work
	let frames = t.query("FROM test::items");
	assert_eq!(TestEngine::row_count(&frames), 1);
}
