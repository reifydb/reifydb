// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER ATTRIBUTE iac_create_a: utf8", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	assert!(
		catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), "iac_create_a")
			.unwrap()
			.is_some(),
		"within-txn created attribute must be findable by name"
	);
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER ATTRIBUTE iac_create_b: utf8", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn2), "iac_create_b")
			.unwrap()
			.is_none(),
		"rolled-back create must not leave attribute behind"
	);
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER ATTRIBUTE iac_create_c: utf8", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn2), "iac_create_c")
			.unwrap()
			.is_some(),
		"committed attribute must be findable in new txn"
	);
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("CREATE USER ATTRIBUTE iac_create_d: utf8", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn2), "iac_create_d")
			.unwrap()
			.is_none(),
		"concurrent txn must not see uncommitted attribute"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn3), "iac_create_d")
			.unwrap()
			.is_some(),
		"after commit, attribute must be findable in a fresh txn"
	);
}
