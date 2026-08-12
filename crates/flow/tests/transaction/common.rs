// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::identity::IdentityId;

pub fn create_test_transaction() -> AdminTransaction {
	let t = TestEngine::new();
	t.begin_admin(IdentityId::system()).unwrap()
}
