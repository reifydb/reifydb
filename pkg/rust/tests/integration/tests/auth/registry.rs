// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::sync::Arc;

use reifydb::testing::db::TestDb;
use reifydb_test_harness::engine::AsEngine;

#[test]
fn engine_and_auth_service_share_one_provider_registry() {
	// Credential creation reads the engine registry, login reads the auth service one.
	// A provider present in only one of them is half-installed, so both halves must
	// resolve providers from the very same registry instance.
	let db = TestDb::memory();

	let engine_registry = db.engine().services().auth_registry.clone();
	let service_registry = db.auth_service().auth_registry().clone();

	assert!(
		Arc::ptr_eq(&engine_registry, &service_registry),
		"credential creation and login must resolve providers from one registry"
	);
}
