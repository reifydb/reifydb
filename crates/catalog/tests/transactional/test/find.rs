// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Combined create+drop within a single txn; asserts via all find methods:
// `find_test_by_name`, `find_test` (by id).
//
// All scenarios are ignored until `DROP TEST` lands in the RQL grammar.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
#[ignore = "awaiting RQL DROP TEST"]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tsf_ns_a");
	t.admin("CREATE TEST tsf_ns_a::keep { ASSERT { true } }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tsf_ns_a")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_test_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE TEST tsf_ns_a::new { ASSERT { true } }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP TEST tsf_ns_a::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let new_test = catalog
		.find_test_by_name(&mut Transaction::Admin(&mut txn), ns_id, "new")
		.unwrap()
		.expect("within-txn created test must be findable by name");
	let new_id = new_test.id;
	assert!(
		catalog.find_test(&mut Transaction::Admin(&mut txn), new_id).unwrap().is_some(),
		"within-txn created test must be findable by id"
	);

	assert!(
		catalog.find_test_by_name(&mut Transaction::Admin(&mut txn), ns_id, "keep").unwrap().is_none(),
		"within-txn dropped test must not be findable by name"
	);
	assert!(
		catalog.find_test(&mut Transaction::Admin(&mut txn), keep_id).unwrap().is_none(),
		"within-txn dropped test must not be findable by id"
	);
}

#[test]
#[ignore = "awaiting RQL DROP TEST"]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tsf_ns_b");
	t.admin("CREATE TEST tsf_ns_b::keep { ASSERT { true } }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tsf_ns_b")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_test_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE TEST tsf_ns_b::new { ASSERT { true } }", Params::None);
	txn.rql("DROP TEST tsf_ns_b::keep", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new").unwrap().is_none());
	assert!(catalog.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep").unwrap().is_some());
	assert!(
		catalog.find_test(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some(),
		"rolled-back drop must leave test findable by id"
	);
}

#[test]
#[ignore = "awaiting RQL DROP TEST"]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tsf_ns_c");
	t.admin("CREATE TEST tsf_ns_c::keep { ASSERT { true } }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tsf_ns_c")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_test_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE TEST tsf_ns_c::new { ASSERT { true } }", Params::None);
	txn.rql("DROP TEST tsf_ns_c::keep", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let new_test = catalog
		.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new")
		.unwrap()
		.expect("committed create must be findable by name");
	let new_id = new_test.id;
	assert!(
		catalog.find_test(&mut Transaction::Admin(&mut txn2), new_id).unwrap().is_some(),
		"committed create must be findable by id"
	);
	assert!(catalog.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep").unwrap().is_none());
	assert!(
		catalog.find_test(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_none(),
		"committed drop must not be findable by id"
	);
}

#[test]
#[ignore = "awaiting RQL DROP TEST"]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tsf_ns_d");
	t.admin("CREATE TEST tsf_ns_d::keep { ASSERT { true } }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tsf_ns_d")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_test_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE TEST tsf_ns_d::new { ASSERT { true } }", Params::None);
	txn1.rql("DROP TEST tsf_ns_d::keep", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new").unwrap().is_none());
	assert!(catalog.find_test_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep").unwrap().is_some());
	assert!(
		catalog.find_test(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some(),
		"txn2 must see keep by id while txn1 is uncommitted"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let new_test = catalog
		.find_test_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "new")
		.unwrap()
		.expect("after commit, new test must be findable by name");
	let new_id = new_test.id;
	assert!(catalog.find_test(&mut Transaction::Admin(&mut txn3), new_id).unwrap().is_some());
	assert!(catalog.find_test_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "keep").unwrap().is_none());
	assert!(catalog.find_test(&mut Transaction::Admin(&mut txn3), keep_id).unwrap().is_none());
}
