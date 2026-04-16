// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_drop_a");
	t.admin("CREATE TABLE pol_drop_a::t { id: int4 }");
	t.admin("CREATE TABLE POLICY pol_drop_a_policy ON pol_drop_a::t { read: { filter { true } } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP TABLE POLICY pol_drop_a_policy", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let found = catalog
		.find_policy_by_name(&mut Transaction::Admin(&mut txn), "pol_drop_a_policy")
		.unwrap();
	assert!(found.is_none());
}

#[test]
fn rolled_back_drop_leaves_policy_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_drop_b");
	t.admin("CREATE TABLE pol_drop_b::t { id: int4 }");
	t.admin("CREATE TABLE POLICY pol_drop_b_policy ON pol_drop_b::t { read: { filter { true } } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP TABLE POLICY pol_drop_b_policy", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_drop_b_policy")
		.unwrap();
	assert!(found.is_some());
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_drop_c");
	t.admin("CREATE TABLE pol_drop_c::t { id: int4 }");
	t.admin("CREATE TABLE POLICY pol_drop_c_policy ON pol_drop_c::t { read: { filter { true } } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP TABLE POLICY pol_drop_c_policy", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_drop_c_policy")
		.unwrap();
	assert!(found.is_none());
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_drop_d");
	t.admin("CREATE TABLE pol_drop_d::t { id: int4 }");
	t.admin("CREATE TABLE POLICY pol_drop_d_policy ON pol_drop_d::t { read: { filter { true } } }");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("DROP TABLE POLICY pol_drop_d_policy", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn2 = catalog
		.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_drop_d_policy")
		.unwrap();
	assert!(found_in_txn2.is_some());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn3 = catalog
		.find_policy_by_name(&mut Transaction::Admin(&mut txn3), "pol_drop_d_policy")
		.unwrap();
	assert!(found_in_txn3.is_none());
}
