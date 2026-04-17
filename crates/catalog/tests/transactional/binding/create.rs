// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_create_a");
	t.admin("CREATE PROCEDURE bnd_create_a::greet AS { \"hi\" }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_create_a")
			.unwrap()
			.unwrap();
		let id = ns.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE HTTP BINDING bnd_create_a::greet_http FOR bnd_create_a::greet WITH { method: \"POST\", path: \"/bnd_create_a/greet\", format: \"json\" }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let found = catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn), ns_id, "greet_http").unwrap();
	assert!(found.is_some(), "uncommitted CREATE BINDING must be visible within its creating txn");
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_create_b");
	t.admin("CREATE PROCEDURE bnd_create_b::greet AS { \"hi\" }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_create_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE HTTP BINDING bnd_create_b::greet_http FOR bnd_create_b::greet WITH { method: \"POST\", path: \"/bnd_create_b/greet\", format: \"json\" }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "greet_http").unwrap();
	assert!(found.is_none(), "rolled-back binding must not persist");
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_create_c");
	t.admin("CREATE PROCEDURE bnd_create_c::greet AS { \"hi\" }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_create_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE HTTP BINDING bnd_create_c::greet_http FOR bnd_create_c::greet WITH { method: \"POST\", path: \"/bnd_create_c/greet\", format: \"json\" }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "greet_http").unwrap();
	assert!(found.is_some(), "committed binding must be findable in new txn");
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_create_d");
	t.admin("CREATE PROCEDURE bnd_create_d::greet AS { \"hi\" }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_create_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql(
		"CREATE HTTP BINDING bnd_create_d::greet_http FOR bnd_create_d::greet WITH { method: \"POST\", path: \"/bnd_create_d/greet\", format: \"json\" }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let seen_in_concurrent =
		catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "greet_http").unwrap();
	assert!(seen_in_concurrent.is_none(), "concurrent txn must not see uncommitted binding");

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let seen_after_commit =
		catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "greet_http").unwrap();
	assert!(seen_after_commit.is_some(), "after commit, binding must be findable in a fresh txn");
}
