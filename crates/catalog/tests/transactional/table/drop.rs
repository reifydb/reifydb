// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tabns_drop_a");
	t.admin("CREATE TABLE tabns_drop_a::t { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tabns_drop_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP TABLE tabns_drop_a::t", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let found = catalog.find_table_by_name(&mut Transaction::Admin(&mut txn), ns_id, "t").unwrap();
	assert!(found.is_none());

	let all = catalog.list_tables_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name() == "t"));
}

#[test]
fn rolled_back_drop_leaves_table_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tabns_drop_b");
	t.admin("CREATE TABLE tabns_drop_b::t { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tabns_drop_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP TABLE tabns_drop_b::t", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_table_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "t").unwrap();
	assert!(found.is_some());
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tabns_drop_c");
	t.admin("CREATE TABLE tabns_drop_c::t { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tabns_drop_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP TABLE tabns_drop_c::t", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_table_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "t").unwrap();
	assert!(found.is_none());
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tabns_drop_d");
	t.admin("CREATE TABLE tabns_drop_d::t { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tabns_drop_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("DROP TABLE tabns_drop_d::t", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn2 = catalog.find_table_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "t").unwrap();
	assert!(found_in_txn2.is_some());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn3 = catalog.find_table_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "t").unwrap();
	assert!(found_in_txn3.is_none());
}
