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
	t.admin("CREATE NAMESPACE tabns_create_a");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tabns_create_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql("CREATE TABLE tabns_create_a::t { id: int4 }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let found = catalog.find_table_by_name(&mut Transaction::Admin(&mut txn), ns_id, "t").unwrap();
	assert!(found.is_some());

	let all = catalog.list_tables(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name() == "t"));
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tabns_create_b");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tabns_create_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql("CREATE TABLE tabns_create_b::t { id: int4 }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found = catalog.find_table_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "t").unwrap();
	assert!(found.is_none());
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tabns_create_c");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tabns_create_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql("CREATE TABLE tabns_create_c::t { id: int4 }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found = catalog.find_table_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "t").unwrap();
	assert!(found.is_some());
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tabns_create_d");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "tabns_create_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn1.rql("CREATE TABLE tabns_create_d::t { id: int4 }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found_in_txn2 = catalog.find_table_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "t").unwrap();
	assert!(found_in_txn2.is_none());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found_in_txn3 = catalog.find_table_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "t").unwrap();
	assert!(found_in_txn3.is_some());
}

#[test]
fn create_drop_create_same_name_in_one_txn_succeeds() {
	// A drop must hide an earlier in-txn create, otherwise the recreate fails or resolves to the dropped table.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE tabns_create_rec");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let ns_id = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "tabns_create_rec")
		.unwrap()
		.unwrap()
		.id();
	let r = txn.rql("CREATE TABLE tabns_create_rec::t { id: int4 }", Params::None);
	assert!(r.error.is_none(), "first create failed: {:?}", r.error);
	let first = catalog
		.find_table_by_name(&mut Transaction::Admin(&mut txn), ns_id, "t")
		.unwrap()
		.expect("in-txn create must be findable by name")
		.id;
	let r = txn.rql("DROP TABLE tabns_create_rec::t", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	assert!(
		catalog.find_table_by_name(&mut Transaction::Admin(&mut txn), ns_id, "t").unwrap().is_none(),
		"in-txn drop must hide the in-txn create"
	);
	let r = txn.rql("CREATE TABLE tabns_create_rec::t { id: int4 }", Params::None);
	assert!(r.error.is_none(), "recreate after in-txn drop failed: {:?}", r.error);
	let second = catalog
		.find_table_by_name(&mut Transaction::Admin(&mut txn), ns_id, "t")
		.unwrap()
		.expect("recreated table must be findable by name")
		.id;
	assert_ne!(first, second, "lookup by name must return the recreated object, never the dropped one");
}
