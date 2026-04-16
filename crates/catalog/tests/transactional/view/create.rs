// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_create_a");
	t.admin("CREATE TABLE vns_create_a::src { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "vns_create_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE DEFERRED VIEW vns_create_a::v { id: int4 } AS { FROM vns_create_a::src MAP { id: id } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let found = catalog
		.find_view_by_name(&mut Transaction::Admin(&mut txn), ns_id, "v")
		.unwrap();
	assert!(found.is_some());

	let all = catalog.list_views_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(all.iter().any(|x| x.namespace() == ns_id && x.name() == "v"));
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_create_b");
	t.admin("CREATE TABLE vns_create_b::src { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "vns_create_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE DEFERRED VIEW vns_create_b::v { id: int4 } AS { FROM vns_create_b::src MAP { id: id } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_view_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "v")
		.unwrap();
	assert!(found.is_none());
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_create_c");
	t.admin("CREATE TABLE vns_create_c::src { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "vns_create_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE DEFERRED VIEW vns_create_c::v { id: int4 } AS { FROM vns_create_c::src MAP { id: id } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_view_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "v")
		.unwrap();
	assert!(found.is_some());
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE vns_create_d");
	t.admin("CREATE TABLE vns_create_d::src { id: int4 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "vns_create_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql(
		"CREATE DEFERRED VIEW vns_create_d::v { id: int4 } AS { FROM vns_create_d::src MAP { id: id } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn2 = catalog
		.find_view_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "v")
		.unwrap();
	assert!(found_in_txn2.is_none());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn3 = catalog
		.find_view_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "v")
		.unwrap();
	assert!(found_in_txn3.is_some());
}
