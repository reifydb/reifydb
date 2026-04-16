// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE dns_drop_a");
	t.admin("CREATE DICTIONARY dns_drop_a::d FOR utf8 AS uint2");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "dns_drop_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP DICTIONARY dns_drop_a::d", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	assert!(
		catalog
			.find_dictionary_by_name(&mut Transaction::Admin(&mut txn), ns_id, "d")
			.unwrap()
			.is_none()
	);
	let all = catalog.list_dictionaries(&mut Transaction::Admin(&mut txn), ns_id).unwrap();
	assert!(!all.iter().any(|x| x.name() == "d"));
}

#[test]
fn rolled_back_drop_leaves_dictionary_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE dns_drop_b");
	t.admin("CREATE DICTIONARY dns_drop_b::d FOR utf8 AS uint2");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "dns_drop_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP DICTIONARY dns_drop_b::d", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_dictionary_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "d")
			.unwrap()
			.is_some()
	);
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE dns_drop_c");
	t.admin("CREATE DICTIONARY dns_drop_c::d FOR utf8 AS uint2");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "dns_drop_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP DICTIONARY dns_drop_c::d", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_dictionary_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "d")
			.unwrap()
			.is_none()
	);
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE dns_drop_d");
	t.admin("CREATE DICTIONARY dns_drop_d::d FOR utf8 AS uint2");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "dns_drop_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("DROP DICTIONARY dns_drop_d::d", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_dictionary_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "d")
			.unwrap()
			.is_some()
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_dictionary_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "d")
			.unwrap()
			.is_none()
	);
}
