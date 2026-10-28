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
	t.admin("CREATE NAMESPACE stns_create_a");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stns_create_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql("CREATE ENUM stns_create_a::status { Active, Inactive }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	assert!(catalog.find_sumtype_by_name(&mut Transaction::Admin(&mut txn), ns_id, "status").unwrap().is_some());
	let all = catalog.list_sumtypes(&mut Transaction::Admin(&mut txn), ns_id).unwrap();
	assert!(all.iter().any(|x| x.name == "status"));
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE stns_create_b");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stns_create_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql("CREATE ENUM stns_create_b::status { Active, Inactive }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	assert!(catalog.find_sumtype_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "status").unwrap().is_none());
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE stns_create_c");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stns_create_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql("CREATE ENUM stns_create_c::status { Active, Inactive }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	assert!(catalog.find_sumtype_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "status").unwrap().is_some());
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE stns_create_d");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stns_create_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn1.rql("CREATE ENUM stns_create_d::status { Active, Inactive }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	assert!(catalog.find_sumtype_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "status").unwrap().is_none());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	assert!(catalog.find_sumtype_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "status").unwrap().is_some());
}

#[test]
fn create_drop_create_same_name_in_one_txn_succeeds() {
	// A drop must hide an earlier in-txn create, otherwise the recreate fails or resolves to the dropped sumtype.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE stns_create_rec");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let ns_id = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "stns_create_rec")
		.unwrap()
		.unwrap()
		.id();
	let r = txn.rql("CREATE ENUM stns_create_rec::status { Active, Inactive }", Params::None);
	assert!(r.error.is_none(), "first create failed: {:?}", r.error);
	let first = catalog
		.find_sumtype_by_name(&mut Transaction::Admin(&mut txn), ns_id, "status")
		.unwrap()
		.expect("in-txn create must be findable by name")
		.id;
	let r = txn.rql("DROP ENUM stns_create_rec::status", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	assert!(
		catalog.find_sumtype_by_name(&mut Transaction::Admin(&mut txn), ns_id, "status").unwrap().is_none(),
		"in-txn drop must hide the in-txn create"
	);
	let r = txn.rql("CREATE ENUM stns_create_rec::status { Active, Inactive }", Params::None);
	assert!(r.error.is_none(), "recreate after in-txn drop failed: {:?}", r.error);
	let second = catalog
		.find_sumtype_by_name(&mut Transaction::Admin(&mut txn), ns_id, "status")
		.unwrap()
		.expect("recreated sumtype must be findable by name")
		.id;
	assert_ne!(first, second, "lookup by name must return the recreated object, never the dropped one");
}
