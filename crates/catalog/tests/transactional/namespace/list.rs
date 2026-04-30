// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_list_keep_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE NAMESPACE ns_list_new_a", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP NAMESPACE ns_list_keep_a", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let all = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(
		all.iter().any(|n| n.name() == "ns_list_new_a"),
		"within-txn created namespace must appear in list_namespaces_all"
	);
	assert!(
		!all.iter().any(|n| n.name() == "ns_list_keep_a"),
		"within-txn dropped namespace must not appear in list_namespaces_all"
	);
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_list_keep_b");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE NAMESPACE ns_list_new_b", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP NAMESPACE ns_list_keep_b", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(
		!all.iter().any(|n| n.name() == "ns_list_new_b"),
		"rolled-back create must not appear in list_namespaces_all in a later txn"
	);
	assert!(
		all.iter().any(|n| n.name() == "ns_list_keep_b"),
		"rolled-back drop must leave the namespace in list_namespaces_all in a later txn"
	);
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_list_keep_c");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE NAMESPACE ns_list_new_c", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP NAMESPACE ns_list_keep_c", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(
		all.iter().any(|n| n.name() == "ns_list_new_c"),
		"committed create must appear in list_namespaces_all in a new txn"
	);
	assert!(
		!all.iter().any(|n| n.name() == "ns_list_keep_c"),
		"committed drop must not appear in list_namespaces_all in a new txn"
	);
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_list_keep_d");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("CREATE NAMESPACE ns_list_new_d", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn1.rql("DROP NAMESPACE ns_list_keep_d", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all_in_txn2 = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(
		!all_in_txn2.iter().any(|n| n.name() == "ns_list_new_d"),
		"txn2 must not observe txn1's uncommitted create"
	);
	assert!(
		all_in_txn2.iter().any(|n| n.name() == "ns_list_keep_d"),
		"txn2 must still observe the pre-existing namespace while txn1's drop is uncommitted"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let all_in_txn3 = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn3)).unwrap();
	assert!(
		all_in_txn3.iter().any(|n| n.name() == "ns_list_new_d"),
		"after txn1 commits, the created namespace must be visible"
	);
	assert!(
		!all_in_txn3.iter().any(|n| n.name() == "ns_list_keep_d"),
		"after txn1 commits, the dropped namespace must not be visible"
	);
}
