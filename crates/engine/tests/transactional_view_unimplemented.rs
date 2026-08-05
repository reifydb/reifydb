// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::panic::{AssertUnwindSafe, catch_unwind};

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::{params::Params, value::identity::IdentityId};

#[test]
fn create_transactional_view_panics_unimplemented_at_create_time() {
	// Transactional views are parse-and-plan-only until execution is reimplemented: the CREATE
	// must die at the DDL activation seam before any storage, catalog entry, or flow exists.
	// Falsified by routing Instruction::CreateTransactionalView back to a working create: the
	// statement would then succeed and no panic message would match.
	let t = TestEngine::new();
	t.admin("CREATE NAMESPACE ns");
	t.admin("CREATE TABLE ns::src { id: int4 }");

	let panic = catch_unwind(AssertUnwindSafe(|| {
		t.inner().admin_as(
			IdentityId::system(),
			"CREATE TRANSACTIONAL VIEW ns::v { id: int4 } AS { FROM ns::src }",
			Params::None,
		)
	}))
	.expect_err("CREATE TRANSACTIONAL VIEW must panic at create time, not succeed or return an error");

	let message = panic
		.downcast_ref::<String>()
		.map(String::as_str)
		.or_else(|| panic.downcast_ref::<&str>().copied())
		.unwrap_or("");
	assert!(
		message.contains("transactional view execution"),
		"the panic must come from the transactional-view activation seam; got: {message:?}"
	);

	// The seam fires before the catalog mutates, so nothing may half-register.
	let frames = t.query("FROM system::views");
	let names: Vec<String> = frames
		.first()
		.and_then(|f| f.columns.iter().find(|c| c.name == "name"))
		.map(|c| (0..c.data.len()).map(|i| c.data.get_value(i).to_string()).collect())
		.unwrap_or_default();
	assert!(!names.iter().any(|n| n == "v"), "no view may exist after the failed create; got {names:?}");
}
