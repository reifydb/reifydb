// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pns_create_a");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "pns_create_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE PROCEDURE pns_create_a::greet AS { \"hi\" }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let found = catalog.find_procedure_by_name(&mut Transaction::Admin(&mut txn), ns_id, "greet").unwrap();
	assert!(found.is_some(), "uncommitted CREATE PROCEDURE must be visible within its creating txn");
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pns_create_b");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "pns_create_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE PROCEDURE pns_create_b::greet AS { \"hi\" }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_procedure_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "greet").unwrap();
	assert!(found.is_none(), "rolled-back procedure must not be visible in a later txn");
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pns_create_c");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "pns_create_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE PROCEDURE pns_create_c::greet AS { \"hi\" }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_procedure_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "greet").unwrap();
	assert!(found.is_some(), "committed procedure must be visible in a new txn");
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pns_create_d");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "pns_create_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("CREATE PROCEDURE pns_create_d::greet AS { \"hi\" }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn2 = catalog.find_procedure_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "greet").unwrap();
	assert!(found_in_txn2.is_none(), "txn2 must not observe txn1's uncommitted CREATE PROCEDURE");

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn3 = catalog.find_procedure_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "greet").unwrap();
	assert!(found_in_txn3.is_some(), "after txn1 commits, the procedure must be visible");
}
