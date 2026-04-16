// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE NAMESPACE ns_create_a", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let found = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "ns_create_a")
		.unwrap();
	assert!(found.is_some(), "uncommitted CREATE NAMESPACE must be visible within its creating txn");

	let all = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(
		all.iter().any(|n| n.name() == "ns_create_a"),
		"uncommitted CREATE NAMESPACE must appear in list_namespaces_all within its creating txn"
	);
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE NAMESPACE ns_create_b", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "ns_create_b")
		.unwrap();
	assert!(found.is_none(), "rolled-back namespace must not be visible in a later txn");

	let all = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(
		!all.iter().any(|n| n.name() == "ns_create_b"),
		"rolled-back namespace must not appear in list_namespaces_all in a later txn"
	);
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE NAMESPACE ns_create_c", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "ns_create_c")
		.unwrap();
	assert!(found.is_some(), "committed namespace must be visible in a new txn");

	let all = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(
		all.iter().any(|n| n.name() == "ns_create_c"),
		"committed namespace must appear in list_namespaces_all in a new txn"
	);
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("CREATE NAMESPACE ns_create_d", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn2 = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "ns_create_d")
		.unwrap();
	assert!(
		found_in_txn2.is_none(),
		"txn2 must not observe txn1's uncommitted CREATE NAMESPACE"
	);
	let all_in_txn2 = catalog.list_namespaces_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(
		!all_in_txn2.iter().any(|n| n.name() == "ns_create_d"),
		"txn2's list must not include txn1's uncommitted CREATE NAMESPACE"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn3 = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn3), "ns_create_d")
		.unwrap();
	assert!(found_in_txn3.is_some(), "after txn1 commits, namespace must be visible in a later txn");
}
