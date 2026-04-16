// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Combined create+drop within a single txn; currently ignored because
// `DROP TEST` is not in the RQL grammar. When it lands, scenario A is
// the canonical reproducer for the `.retain()` omission in
// `list_all_tests` / `list_tests_in_namespace` at
// `crates/catalog/src/catalog/test.rs:140-214`.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
#[ignore = "awaiting RQL DROP TEST"]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_list_a");
	t.admin("CREATE TEST tns_list_a::keep { ASSERT { true } }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_list_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE TEST tns_list_a::new { ASSERT { true } }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP TEST tns_list_a::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let all = catalog.list_all_tests(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(
		all.iter().any(|x| x.namespace == ns_id && x.name == "new"),
		"within-txn created test must appear in list_all_tests"
	);
	assert!(
		!all.iter().any(|x| x.namespace == ns_id && x.name == "keep"),
		"within-txn dropped test must not appear in list_all_tests"
	);

	let in_ns = catalog.list_tests_in_namespace(&mut Transaction::Admin(&mut txn), ns_id).unwrap();
	assert!(in_ns.iter().any(|x| x.name == "new"));
	assert!(
		!in_ns.iter().any(|x| x.name == "keep"),
		"within-txn dropped test must not appear in list_tests_in_namespace"
	);
}

#[test]
#[ignore = "awaiting RQL DROP TEST"]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_list_b");
	t.admin("CREATE TEST tns_list_b::keep { ASSERT { true } }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_list_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE TEST tns_list_b::new { ASSERT { true } }", Params::None);
	txn.rql("DROP TEST tns_list_b::keep", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_all_tests(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name == "new"));
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name == "keep"));

	let in_ns = catalog.list_tests_in_namespace(&mut Transaction::Admin(&mut txn2), ns_id).unwrap();
	assert!(!in_ns.iter().any(|x| x.name == "new"));
	assert!(in_ns.iter().any(|x| x.name == "keep"));
}

#[test]
#[ignore = "awaiting RQL DROP TEST"]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_list_c");
	t.admin("CREATE TEST tns_list_c::keep { ASSERT { true } }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_list_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE TEST tns_list_c::new { ASSERT { true } }", Params::None);
	txn.rql("DROP TEST tns_list_c::keep", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_all_tests(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name == "new"));
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name == "keep"));

	let in_ns = catalog.list_tests_in_namespace(&mut Transaction::Admin(&mut txn2), ns_id).unwrap();
	assert!(in_ns.iter().any(|x| x.name == "new"));
	assert!(!in_ns.iter().any(|x| x.name == "keep"));
}

#[test]
#[ignore = "awaiting RQL DROP TEST"]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tns_list_d");
	t.admin("CREATE TEST tns_list_d::keep { ASSERT { true } }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tns_list_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE TEST tns_list_d::new { ASSERT { true } }", Params::None);
	txn1.rql("DROP TEST tns_list_d::keep", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all_in_txn2 = catalog.list_all_tests(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all_in_txn2.iter().any(|x| x.namespace == ns_id && x.name == "new"));
	assert!(all_in_txn2.iter().any(|x| x.namespace == ns_id && x.name == "keep"));

	let in_ns_txn2 = catalog.list_tests_in_namespace(&mut Transaction::Admin(&mut txn2), ns_id).unwrap();
	assert!(!in_ns_txn2.iter().any(|x| x.name == "new"));
	assert!(in_ns_txn2.iter().any(|x| x.name == "keep"));

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let all_in_txn3 = catalog.list_all_tests(&mut Transaction::Admin(&mut txn3)).unwrap();
	assert!(all_in_txn3.iter().any(|x| x.namespace == ns_id && x.name == "new"));
	assert!(!all_in_txn3.iter().any(|x| x.namespace == ns_id && x.name == "keep"));

	let in_ns_txn3 = catalog.list_tests_in_namespace(&mut Transaction::Admin(&mut txn3), ns_id).unwrap();
	assert!(in_ns_txn3.iter().any(|x| x.name == "new"));
	assert!(!in_ns_txn3.iter().any(|x| x.name == "keep"));
}
