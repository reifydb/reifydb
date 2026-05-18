// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE dns_list_a");
	t.admin("CREATE DICTIONARY dns_list_a::keep FOR utf8 AS uint2");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "dns_list_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE DICTIONARY dns_list_a::new FOR utf8 AS uint2", Params::None);
	txn.rql("DROP DICTIONARY dns_list_a::keep", Params::None);

	let in_ns = catalog.list_dictionaries(&mut Transaction::Admin(&mut txn), ns_id).unwrap();
	assert!(in_ns.iter().any(|x| x.name() == "new"));
	assert!(!in_ns.iter().any(|x| x.name() == "keep"));

	let all = catalog.list_all_dictionaries(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(all.iter().any(|x| x.name() == "new"));
	assert!(!all.iter().any(|x| x.name() == "keep"));
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE dns_list_b");
	t.admin("CREATE DICTIONARY dns_list_b::keep FOR utf8 AS uint2");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "dns_list_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE DICTIONARY dns_list_b::new FOR utf8 AS uint2", Params::None);
	txn.rql("DROP DICTIONARY dns_list_b::keep", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let in_ns = catalog.list_dictionaries(&mut Transaction::Admin(&mut txn2), ns_id).unwrap();
	assert!(!in_ns.iter().any(|x| x.name() == "new"));
	assert!(in_ns.iter().any(|x| x.name() == "keep"));

	let all = catalog.list_all_dictionaries(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all.iter().any(|x| x.name() == "new"));
	assert!(all.iter().any(|x| x.name() == "keep"));
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE dns_list_c");
	t.admin("CREATE DICTIONARY dns_list_c::keep FOR utf8 AS uint2");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "dns_list_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE DICTIONARY dns_list_c::new FOR utf8 AS uint2", Params::None);
	txn.rql("DROP DICTIONARY dns_list_c::keep", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let in_ns = catalog.list_dictionaries(&mut Transaction::Admin(&mut txn2), ns_id).unwrap();
	assert!(in_ns.iter().any(|x| x.name() == "new"));
	assert!(!in_ns.iter().any(|x| x.name() == "keep"));

	let all = catalog.list_all_dictionaries(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(all.iter().any(|x| x.name() == "new"));
	assert!(!all.iter().any(|x| x.name() == "keep"));
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE dns_list_d");
	t.admin("CREATE DICTIONARY dns_list_d::keep FOR utf8 AS uint2");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "dns_list_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE DICTIONARY dns_list_d::new FOR utf8 AS uint2", Params::None);
	txn1.rql("DROP DICTIONARY dns_list_d::keep", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let in_ns_txn2 = catalog.list_dictionaries(&mut Transaction::Admin(&mut txn2), ns_id).unwrap();
	assert!(!in_ns_txn2.iter().any(|x| x.name() == "new"));
	assert!(in_ns_txn2.iter().any(|x| x.name() == "keep"));

	let all_txn2 = catalog.list_all_dictionaries(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all_txn2.iter().any(|x| x.name() == "new"));
	assert!(all_txn2.iter().any(|x| x.name() == "keep"));

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let in_ns_txn3 = catalog.list_dictionaries(&mut Transaction::Admin(&mut txn3), ns_id).unwrap();
	assert!(in_ns_txn3.iter().any(|x| x.name() == "new"));
	assert!(!in_ns_txn3.iter().any(|x| x.name() == "keep"));

	let all_txn3 = catalog.list_all_dictionaries(&mut Transaction::Admin(&mut txn3)).unwrap();
	assert!(all_txn3.iter().any(|x| x.name() == "new"));
	assert!(!all_txn3.iter().any(|x| x.name() == "keep"));
}
