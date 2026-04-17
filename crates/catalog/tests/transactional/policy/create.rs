// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Policies are named globally (not namespace-scoped). Only `find_policy_by_name`
// exists on the Catalog; there's no `list_policies_all`. So these tests only
// cross-check via `find_policy_by_name`.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_create_a");
	t.admin("CREATE TABLE pol_create_a::t { id: int4 }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE TABLE POLICY pol_create_a_policy ON pol_create_a::t { read: { filter { true } } }",
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

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE TABLE POLICY pol_create_b_policy ON pol_create_b::t { read: { filter { true } } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_create_b_policy").unwrap();
	assert!(found.is_none());
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_create_c");
	t.admin("CREATE TABLE pol_create_c::t { id: int4 }");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE TABLE POLICY pol_create_c_policy ON pol_create_c::t { read: { filter { true } } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_create_c_policy").unwrap();
	assert!(found.is_some());
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pol_create_d");
	t.admin("CREATE TABLE pol_create_d::t { id: int4 }");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql(
		"CREATE TABLE POLICY pol_create_d_policy ON pol_create_d::t { read: { filter { true } } }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn2 =
		catalog.find_policy_by_name(&mut Transaction::Admin(&mut txn2), "pol_create_d_policy").unwrap();
	assert!(found_in_txn2.is_none());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn3 =
		catalog.find_policy_by_name(&mut Transaction::Admin(&mut txn3), "pol_create_d_policy").unwrap();
	assert!(found_in_txn3.is_some());
}
