// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_create_a");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_create_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE TEST tns_create_a::foo { ASSERT { true } }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let found = catalog.find_test_by_name(&mut Transaction::Admin(&mut txn), ns_id, "foo").unwrap();
	assert!(found.is_some(), "uncommitted CREATE TEST must be visible within its creating txn");

	let all = catalog.list_all_tests(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(
		all.iter().any(|x| x.namespace == ns_id && x.name == "foo"),
		"uncommitted CREATE TEST must appear in list_all_tests within its creating txn"
	);

	let in_ns = catalog.list_tests_in_namespace(&mut Transaction::Admin(&mut txn), ns_id).unwrap();
	assert!(
		in_ns.iter().any(|x| x.name == "foo"),
		"uncommitted CREATE TEST must appear in list_tests_in_namespace within its creating txn"
	);
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_create_b");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_create_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE TEST tns_create_b::foo { ASSERT { true } }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "foo").unwrap();
	assert!(found.is_none(), "rolled-back test must not be visible in a later txn");

	let all = catalog.list_all_tests(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name == "foo"));
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_create_c");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_create_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE TEST tns_create_c::foo { ASSERT { true } }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "foo").unwrap();
	assert!(found.is_some(), "committed test must be visible in a new txn");

	let all = catalog.list_all_tests(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name == "foo"));
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_create_d");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_create_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("CREATE TEST tns_create_d::foo { ASSERT { true } }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn2 = catalog.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "foo").unwrap();
	assert!(found_in_txn2.is_none(), "txn2 must not observe txn1's uncommitted CREATE TEST");
	let all_in_txn2 = catalog.list_all_tests(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all_in_txn2.iter().any(|x| x.namespace == ns_id && x.name == "foo"));

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn3 = catalog.find_test_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "foo").unwrap();
	assert!(found_in_txn3.is_some(), "after txn1 commits, the test must be visible in a later txn");
}
