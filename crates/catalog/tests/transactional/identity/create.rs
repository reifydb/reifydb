// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER idn_create_a", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "idn_create_a").unwrap().is_some(),
		"within-txn created identity must be findable by name"
	);
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER idn_create_b", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "idn_create_b").unwrap().is_none(),
		"rolled-back create must not leave identity behind"
	);
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE USER idn_create_c", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "idn_create_c").unwrap().is_some(),
		"committed identity must be findable in new txn"
	);
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("CREATE USER idn_create_d", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "idn_create_d").unwrap().is_none(),
		"concurrent txn must not see uncommitted identity"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn3), "idn_create_d").unwrap().is_some(),
		"after commit, identity must be findable in a fresh txn"
	);
}
