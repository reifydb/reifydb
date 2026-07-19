// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::{params::Params, value::{identity::IdentityId}};
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE ial_keep_a: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER ATTRIBUTE ial_new_a: utf8", Params::None);
	txn.rql("DROP USER ATTRIBUTE ial_keep_a", Params::None);

	let all = catalog.list_identity_attributes(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(all.iter().any(|x| x.name == "ial_new_a"));
	assert!(!all.iter().any(|x| x.name == "ial_keep_a"));
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE ial_keep_b: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER ATTRIBUTE ial_new_b: utf8", Params::None);
	txn.rql("DROP USER ATTRIBUTE ial_keep_b", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_identity_attributes(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all.iter().any(|x| x.name == "ial_new_b"));
	assert!(all.iter().any(|x| x.name == "ial_keep_b"));
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE ial_keep_c: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER ATTRIBUTE ial_new_c: utf8", Params::None);
	txn.rql("DROP USER ATTRIBUTE ial_keep_c", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_identity_attributes(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(all.iter().any(|x| x.name == "ial_new_c"));
	assert!(!all.iter().any(|x| x.name == "ial_keep_c"));
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE ial_keep_d: utf8");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE USER ATTRIBUTE ial_new_d: utf8", Params::None);
	txn1.rql("DROP USER ATTRIBUTE ial_keep_d", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_identity_attributes(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all.iter().any(|x| x.name == "ial_new_d"));
	assert!(all.iter().any(|x| x.name == "ial_keep_d"));

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_identity_attributes(&mut Transaction::Admin(&mut txn3)).unwrap();
	assert!(all.iter().any(|x| x.name == "ial_new_d"));
	assert!(!all.iter().any(|x| x.name == "ial_keep_d"));
}
