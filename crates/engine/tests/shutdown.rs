// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::{params::Params, value::{identity::IdentityId}};

#[test]
fn shutdown_rejects_external_command_with_txn_014() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::items { id: int8, name: utf8 }");

	t.inner().set_shutting_down();
	assert!(t.inner().is_shutting_down());

	let r = t.inner().command_as(IdentityId::root(), "INSERT test::items [{ id: 1, name: 'x' }]", Params::None);
	let err = r.error.expect("an external command must be rejected once the engine is shutting down");
	assert!(format!("{err:?}").contains("TXN_014"), "expected TXN_014, got: {err:?}");
}

#[test]
fn shutdown_rejects_external_admin_with_txn_014() {
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");

	t.inner().set_shutting_down();

	let r = t.inner().admin_as(IdentityId::root(), "CREATE TABLE test::other { id: int8 }", Params::None);
	let err = r.error.expect("an external admin must be rejected once the engine is shutting down");
	assert!(format!("{err:?}").contains("TXN_014"), "expected TXN_014, got: {err:?}");
}

#[test]
fn shutdown_allows_internal_system_writes() {
	// The shutdown drain commits deferred-view updates as IdentityId::system(); those internal
	// writes must NOT be rejected, otherwise the CDC consumer could never catch up during shutdown.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::items { id: int8, name: utf8 }");

	t.inner().set_shutting_down();

	let r = t.inner().command_as(IdentityId::system(), "INSERT test::items [{ id: 1, name: 'x' }]", Params::None);
	assert!(r.error.is_none(), "system writes must proceed during shutdown, got: {:?}", r.error);
}

#[test]
fn shutdown_does_not_gate_reads() {
	// Reads stay available during shutdown (the gate is on command/admin/procedure only) - the drain
	// itself needs queries. An external read must never be rejected with the shutdown error.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE test");
	t.admin("CREATE TABLE test::items { id: int8, name: utf8 }");
	t.command("INSERT test::items [{ id: 1, name: 'x' }]");

	t.inner().set_shutting_down();

	let r = t.inner().query_as(IdentityId::root(), "FROM test::items", Params::None);
	if let Some(e) = &r.error {
		assert!(
			!format!("{e:?}").contains("TXN_014"),
			"reads must not be rejected by the shutdown gate, got: {e:?}"
		);
	}
}
