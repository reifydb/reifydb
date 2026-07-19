// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::{params::Params, value::{identity::IdentityId}};
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iad_drop_a: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP USER ATTRIBUTE iad_drop_a", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	assert!(
		catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), "iad_drop_a")
			.unwrap()
			.is_none(),
		"within-txn dropped attribute must not be findable by name"
	);
}

#[test]
fn rolled_back_drop_leaves_attribute_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iad_drop_b: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP USER ATTRIBUTE iad_drop_b", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn2), "iad_drop_b")
			.unwrap()
			.is_some(),
		"rolled-back drop must leave attribute intact"
	);
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iad_drop_c: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP USER ATTRIBUTE iad_drop_c", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn2), "iad_drop_c")
			.unwrap()
			.is_none(),
		"committed drop must not be findable in new txn"
	);
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iad_drop_d: utf8");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("DROP USER ATTRIBUTE iad_drop_d", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn2), "iad_drop_d")
			.unwrap()
			.is_some(),
		"concurrent txn must still see the attribute while drop is uncommitted"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn3), "iad_drop_d")
			.unwrap()
			.is_none(),
		"after commit, dropped attribute must not be findable in a fresh txn"
	);
}
