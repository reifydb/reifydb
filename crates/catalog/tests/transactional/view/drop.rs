// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

const SETUP_V_A: &str = "CREATE DEFERRED VIEW vns_drop_a::v { id: int4 } AS { FROM vns_drop_a::src MAP { id: id } }";
const SETUP_V_B: &str = "CREATE DEFERRED VIEW vns_drop_b::v { id: int4 } AS { FROM vns_drop_b::src MAP { id: id } }";
const SETUP_V_C: &str = "CREATE DEFERRED VIEW vns_drop_c::v { id: int4 } AS { FROM vns_drop_c::src MAP { id: id } }";
const SETUP_V_D: &str = "CREATE DEFERRED VIEW vns_drop_d::v { id: int4 } AS { FROM vns_drop_d::src MAP { id: id } }";

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_drop_a");
	t.admin("CREATE TABLE vns_drop_a::src { id: int4 }");
	t.admin(SETUP_V_A);

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "vns_drop_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP VIEW vns_drop_a::v", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let found = catalog.find_view_by_name(&mut Transaction::Admin(&mut txn), ns_id, "v").unwrap();
	assert!(found.is_none());

	let all = catalog.list_views_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(!all.iter().any(|x| x.namespace() == ns_id && x.name() == "v"));
}

#[test]
fn rolled_back_drop_leaves_view_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_drop_b");
	t.admin("CREATE TABLE vns_drop_b::src { id: int4 }");
	t.admin(SETUP_V_B);

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "vns_drop_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP VIEW vns_drop_b::v", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_view_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "v").unwrap();
	assert!(found.is_some());
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_drop_c");
	t.admin("CREATE TABLE vns_drop_c::src { id: int4 }");
	t.admin(SETUP_V_C);

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "vns_drop_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP VIEW vns_drop_c::v", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_view_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "v").unwrap();
	assert!(found.is_none());
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_drop_d");
	t.admin("CREATE TABLE vns_drop_d::src { id: int4 }");
	t.admin(SETUP_V_D);

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "vns_drop_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("DROP VIEW vns_drop_d::v", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn2 = catalog.find_view_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "v").unwrap();
	assert!(found_in_txn2.is_some());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn3 = catalog.find_view_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "v").unwrap();
	assert!(found_in_txn3.is_none());
}
