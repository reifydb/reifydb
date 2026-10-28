// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::durability::Durability;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{params::Params, value::identity::IdentityId};

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_create_a");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_create_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
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
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_create_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql("CREATE TEST tns_create_b::foo { ASSERT { true } }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
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
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_create_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql("CREATE TEST tns_create_c::foo { ASSERT { true } }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
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
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_create_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn1.rql("CREATE TEST tns_create_d::foo { ASSERT { true } }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found_in_txn2 = catalog.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "foo").unwrap();
	assert!(found_in_txn2.is_none(), "txn2 must not observe txn1's uncommitted CREATE TEST");
	let all_in_txn2 = catalog.list_all_tests(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all_in_txn2.iter().any(|x| x.namespace == ns_id && x.name == "foo"));

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found_in_txn3 = catalog.find_test_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "foo").unwrap();
	assert!(found_in_txn3.is_some(), "after txn1 commits, the test must be visible in a later txn");
}

#[test]
fn create_drop_create_same_name_in_one_txn_succeeds() {
	// A drop must hide an earlier in-txn create, otherwise the recreate fails or resolves to the dropped test.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_create_rec");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let ns_id = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "tns_create_rec")
		.unwrap()
		.unwrap()
		.id();
	let r = txn.rql("CREATE TEST tns_create_rec::foo { ASSERT { true } }", Params::None);
	assert!(r.error.is_none(), "first create failed: {:?}", r.error);
	let first = catalog
		.find_test_by_name(&mut Transaction::Admin(&mut txn), ns_id, "foo")
		.unwrap()
		.expect("in-txn create must be findable by name")
		.id;
	let r = txn.rql("DROP TEST tns_create_rec::foo", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	assert!(
		catalog.find_test_by_name(&mut Transaction::Admin(&mut txn), ns_id, "foo").unwrap().is_none(),
		"in-txn drop must hide the in-txn create"
	);
	let r = txn.rql("CREATE TEST tns_create_rec::foo { ASSERT { true } }", Params::None);
	assert!(r.error.is_none(), "recreate after in-txn drop failed: {:?}", r.error);
	let second = catalog
		.find_test_by_name(&mut Transaction::Admin(&mut txn), ns_id, "foo")
		.unwrap()
		.expect("recreated test must be findable by name")
		.id;
	assert_ne!(first, second, "lookup by name must return the recreated object, never the dropped one");
}
