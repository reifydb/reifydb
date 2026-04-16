// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// All tests in this module are ignored until RQL gains `DROP TEST ns::name`.
// When that lands, removing `#[ignore]` is expected to surface the missing
// `.retain()` in `list_all_tests` / `list_tests_in_namespace` at
// `crates/catalog/src/catalog/test.rs:140-214`.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
#[ignore = "awaiting RQL DROP TEST"]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_drop_a");
	t.admin("CREATE TEST tns_drop_a::foo { ASSERT { true } }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_drop_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP TEST tns_drop_a::foo", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let found = catalog
		.find_test_by_name(&mut Transaction::Admin(&mut txn), ns_id, "foo")
		.unwrap();
	assert!(found.is_none(), "uncommitted DROP TEST must hide the test within its dropping txn");

	let all = catalog.list_all_tests(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name == "foo"));

	let in_ns = catalog.list_tests_in_namespace(&mut Transaction::Admin(&mut txn), ns_id).unwrap();
	assert!(!in_ns.iter().any(|x| x.name == "foo"));
}

#[test]
#[ignore = "awaiting RQL DROP TEST"]
fn rolled_back_drop_leaves_test_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_drop_b");
	t.admin("CREATE TEST tns_drop_b::foo { ASSERT { true } }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_drop_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP TEST tns_drop_b::foo", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "foo")
		.unwrap();
	assert!(found.is_some(), "rolled-back DROP TEST must leave the test visible in a later txn");
}

#[test]
#[ignore = "awaiting RQL DROP TEST"]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_drop_c");
	t.admin("CREATE TEST tns_drop_c::foo { ASSERT { true } }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_drop_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP TEST tns_drop_c::foo", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "foo")
		.unwrap();
	assert!(found.is_none(), "committed DROP TEST must not be visible in a new txn");
}

#[test]
#[ignore = "awaiting RQL DROP TEST"]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_drop_d");
	t.admin("CREATE TEST tns_drop_d::foo { ASSERT { true } }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_drop_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("DROP TEST tns_drop_d::foo", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn2 = catalog
		.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "foo")
		.unwrap();
	assert!(found_in_txn2.is_some(), "txn2 must still observe the test while txn1's DROP is uncommitted");

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn3 = catalog
		.find_test_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "foo")
		.unwrap();
	assert!(found_in_txn3.is_none(), "after txn1 commits, the test must not be visible in a later txn");
}
