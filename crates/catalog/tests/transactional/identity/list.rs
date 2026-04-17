// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER idl_keep_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER idl_new_a", Params::None);
	txn.rql("DROP USER idl_keep_a", Params::None);

	let all = catalog.list_identities_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(all.iter().any(|x| x.name == "idl_new_a"));
	assert!(!all.iter().any(|x| x.name == "idl_keep_a"));
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER idl_keep_b");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER idl_new_b", Params::None);
	txn.rql("DROP USER idl_keep_b", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_identities_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all.iter().any(|x| x.name == "idl_new_b"));
	assert!(all.iter().any(|x| x.name == "idl_keep_b"));
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER idl_keep_c");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER idl_new_c", Params::None);
	txn.rql("DROP USER idl_keep_c", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_identities_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(all.iter().any(|x| x.name == "idl_new_c"));
	assert!(!all.iter().any(|x| x.name == "idl_keep_c"));
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER idl_keep_d");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE USER idl_new_d", Params::None);
	txn1.rql("DROP USER idl_keep_d", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all_txn2 = catalog.list_identities_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all_txn2.iter().any(|x| x.name == "idl_new_d"));
	assert!(all_txn2.iter().any(|x| x.name == "idl_keep_d"));

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let all_txn3 = catalog.list_identities_all(&mut Transaction::Admin(&mut txn3)).unwrap();
	assert!(all_txn3.iter().any(|x| x.name == "idl_new_d"));
	assert!(!all_txn3.iter().any(|x| x.name == "idl_keep_d"));
}
