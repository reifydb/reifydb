// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_drop_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP NAMESPACE ns_drop_a", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let found = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "ns_drop_a")
		.unwrap();
	assert!(found.is_none(), "uncommitted DROP NAMESPACE must hide the namespace within its dropping txn");

	let all = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(
		!all.iter().any(|n| n.name() == "ns_drop_a"),
		"uncommitted DROP NAMESPACE must remove the namespace from list_namespaces_all within its dropping txn"
	);
}

#[test]
fn rolled_back_drop_leaves_namespace_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_drop_b");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP NAMESPACE ns_drop_b", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "ns_drop_b")
		.unwrap();
	assert!(found.is_some(), "rolled-back DROP must leave the namespace visible in a later txn");

	let all = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(
		all.iter().any(|n| n.name() == "ns_drop_b"),
		"rolled-back DROP must leave the namespace in list_namespaces_all in a later txn"
	);
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_drop_c");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP NAMESPACE ns_drop_c", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "ns_drop_c")
		.unwrap();
	assert!(found.is_none(), "committed DROP NAMESPACE must not be visible in a new txn");

	let all = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(
		!all.iter().any(|n| n.name() == "ns_drop_c"),
		"committed DROP NAMESPACE must not appear in list_namespaces_all in a new txn"
	);
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE ns_drop_d");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("DROP NAMESPACE ns_drop_d", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn2 = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "ns_drop_d")
		.unwrap();
	assert!(
		found_in_txn2.is_some(),
		"txn2 must still observe the namespace while txn1's DROP is uncommitted"
	);
	let all_in_txn2 = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(
		all_in_txn2.iter().any(|n| n.name() == "ns_drop_d"),
		"txn2's list must still include the namespace while txn1's DROP is uncommitted"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn3 = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn3), "ns_drop_d")
		.unwrap();
	assert!(found_in_txn3.is_none(), "after txn1 commits, the namespace must not be visible in a later txn");
}
