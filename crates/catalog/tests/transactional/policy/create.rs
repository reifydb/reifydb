// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// Policies are named globally, not namespace-scoped. `list_all_policies` bypasses the
// admin-transaction change log, so uncommitted state is cross-checked through
// `find_policy_by_name` instead.

use reifydb_core::durability::Durability;
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{params::Params, value::identity::IdentityId};

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_create_a");
	t.admin("CREATE TABLE pol_create_a::t { id: int4 }");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql(
		"CREATE TABLE POLICY pol_create_a_policy ON pol_create_a::t { from: { filter { true } } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let found = catalog.find_policy_by_name(&mut Transaction::Admin(&mut txn), "pol_create_a_policy").unwrap();
	assert!(found.is_some());
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_create_b");
	t.admin("CREATE TABLE pol_create_b::t { id: int4 }");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql(
		"CREATE TABLE POLICY pol_create_b_policy ON pol_create_b::t { from: { filter { true } } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found = catalog.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_create_b_policy").unwrap();
	assert!(found.is_none());
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_create_c");
	t.admin("CREATE TABLE pol_create_c::t { id: int4 }");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql(
		"CREATE TABLE POLICY pol_create_c_policy ON pol_create_c::t { from: { filter { true } } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found = catalog.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_create_c_policy").unwrap();
	assert!(found.is_some());
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_create_d");
	t.admin("CREATE TABLE pol_create_d::t { id: int4 }");

	let mut txn1 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn1.rql(
		"CREATE TABLE POLICY pol_create_d_policy ON pol_create_d::t { from: { filter { true } } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found_in_txn2 =
		catalog.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_create_d_policy").unwrap();
	assert!(found_in_txn2.is_none());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let found_in_txn3 =
		catalog.find_policy_by_name(&mut Transaction::Admin(&mut txn3), "pol_create_d_policy").unwrap();
	assert!(found_in_txn3.is_some());
}

#[test]
fn create_drop_create_same_name_in_one_txn_succeeds() {
	// A drop must hide an earlier in-txn create, otherwise the recreate fails or resolves to the dropped policy.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_create_rec");
	t.admin("CREATE TABLE pol_create_rec::t { id: int4 }");

	let mut txn = t.begin_admin(IdentityId::system(), Durability::Async).unwrap();
	let r = txn.rql(
		"CREATE TABLE POLICY pol_create_rec_policy ON pol_create_rec::t { from: { filter { true } } }",
		Params::None,
	);
	assert!(r.error.is_none(), "first create failed: {:?}", r.error);
	let first = catalog
		.find_policy_by_name(&mut Transaction::Admin(&mut txn), "pol_create_rec_policy")
		.unwrap()
		.expect("in-txn create must be findable by name")
		.id;
	let r = txn.rql("DROP TABLE POLICY pol_create_rec_policy", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	assert!(
		catalog.find_policy_by_name(&mut Transaction::Admin(&mut txn), "pol_create_rec_policy")
			.unwrap()
			.is_none(),
		"in-txn drop must hide the in-txn create"
	);
	let r = txn.rql(
		"CREATE TABLE POLICY pol_create_rec_policy ON pol_create_rec::t { from: { filter { true } } }",
		Params::None,
	);
	assert!(r.error.is_none(), "recreate after in-txn drop failed: {:?}", r.error);
	let second = catalog
		.find_policy_by_name(&mut Transaction::Admin(&mut txn), "pol_create_rec_policy")
		.unwrap()
		.expect("recreated policy must be findable by name")
		.id;
	assert_ne!(first, second, "lookup by name must return the recreated object, never the dropped one");
}
