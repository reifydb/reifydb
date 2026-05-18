// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// No list_all method exists for policies on the Catalog, so this file tracks
// each policy individually via `find_policy_by_name`.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_list_a");
	t.admin("CREATE TABLE pol_list_a::t { id: int4 }");
	t.admin("CREATE TABLE POLICY pol_list_a_keep ON pol_list_a::t { from: { filter { true } } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql(
		"CREATE TABLE POLICY pol_list_a_new ON pol_list_a::t { from: { filter { true } } }",
		Params::None,
	);
	txn.rql("DROP TABLE POLICY pol_list_a_keep", Params::None);

	let new_found = catalog
		.find_policy_by_name(&mut Transaction::Admin(&mut txn), "pol_list_a_new")
		.unwrap();
	assert!(new_found.is_some(), "within-txn created policy must be findable");
	let keep_found = catalog
		.find_policy_by_name(&mut Transaction::Admin(&mut txn), "pol_list_a_keep")
		.unwrap();
	assert!(keep_found.is_none(), "within-txn dropped policy must not be findable");
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_list_b");
	t.admin("CREATE TABLE pol_list_b::t { id: int4 }");
	t.admin("CREATE TABLE POLICY pol_list_b_keep ON pol_list_b::t { from: { filter { true } } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql(
		"CREATE TABLE POLICY pol_list_b_new ON pol_list_b::t { from: { filter { true } } }",
		Params::None,
	);
	txn.rql("DROP TABLE POLICY pol_list_b_keep", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_list_b_new")
			.unwrap()
			.is_none()
	);
	assert!(
		catalog
			.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_list_b_keep")
			.unwrap()
			.is_some()
	);
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_list_c");
	t.admin("CREATE TABLE pol_list_c::t { id: int4 }");
	t.admin("CREATE TABLE POLICY pol_list_c_keep ON pol_list_c::t { from: { filter { true } } }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql(
		"CREATE TABLE POLICY pol_list_c_new ON pol_list_c::t { from: { filter { true } } }",
		Params::None,
	);
	txn.rql("DROP TABLE POLICY pol_list_c_keep", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_list_c_new")
			.unwrap()
			.is_some()
	);
	assert!(
		catalog
			.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_list_c_keep")
			.unwrap()
			.is_none()
	);
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_list_d");
	t.admin("CREATE TABLE pol_list_d::t { id: int4 }");
	t.admin("CREATE TABLE POLICY pol_list_d_keep ON pol_list_d::t { from: { filter { true } } }");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql(
		"CREATE TABLE POLICY pol_list_d_new ON pol_list_d::t { from: { filter { true } } }",
		Params::None,
	);
	txn1.rql("DROP TABLE POLICY pol_list_d_keep", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_list_d_new")
			.unwrap()
			.is_none()
	);
	assert!(
		catalog
			.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_list_d_keep")
			.unwrap()
			.is_some()
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_policy_by_name(&mut Transaction::Admin(&mut txn3), "pol_list_d_new")
			.unwrap()
			.is_some()
	);
	assert!(
		catalog
			.find_policy_by_name(&mut Transaction::Admin(&mut txn3), "pol_list_d_keep")
			.unwrap()
			.is_none()
	);
}
