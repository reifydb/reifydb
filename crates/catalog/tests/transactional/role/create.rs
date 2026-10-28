// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::durability::Durability;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{params::Params, value::identity::IdentityId};

#[test]
fn create_drop_create_same_name_in_one_txn_succeeds() {
	// A drop must hide an earlier in-txn create, otherwise the recreate resolves to the dropped role.
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql("CREATE ROLE role_create_rec", Params::None);
	assert!(r.error.is_none(), "first create failed: {:?}", r.error);
	let first = catalog
		.find_role_by_name(&mut Transaction::Admin(&mut txn), "role_create_rec")
		.unwrap()
		.expect("in-txn create must be findable by name")
		.id;
	let r = txn.rql("DROP ROLE role_create_rec", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	assert!(
		catalog.find_role_by_name(&mut Transaction::Admin(&mut txn), "role_create_rec").unwrap().is_none(),
		"in-txn drop must hide the in-txn create"
	);
	let r = txn.rql("CREATE ROLE role_create_rec", Params::None);
	assert!(r.error.is_none(), "recreate after in-txn drop failed: {:?}", r.error);
	let second = catalog
		.find_role_by_name(&mut Transaction::Admin(&mut txn), "role_create_rec")
		.unwrap()
		.expect("recreated role must be findable by name")
		.id;
	assert_ne!(first, second, "lookup by name must return the recreated object, never the dropped one");
}
