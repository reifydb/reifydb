// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_store_operator::store::OperatorStore;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::admin::AdminTransaction;
use reifydb_value::value::identity::IdentityId;

pub fn create_test_transaction() -> (AdminTransaction, OperatorStore) {
	let t = TestEngine::new();
	let operators = t.inner().operator_state();
	(t.begin_admin(IdentityId::system()).unwrap(), operators)
}
