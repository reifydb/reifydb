// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pns_drop_a");
	t.admin("CREATE PROCEDURE pns_drop_a::greet AS { \"hi\" }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "pns_drop_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP PROCEDURE pns_drop_a::greet", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let found = catalog
		.find_procedure_by_name(&mut Transaction::Admin(&mut txn), ns_id, "greet")
		.unwrap();
	assert!(found.is_none(), "uncommitted DROP PROCEDURE must hide the procedure within its dropping txn");
}

#[test]
fn rolled_back_drop_leaves_procedure_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pns_drop_b");
	t.admin("CREATE PROCEDURE pns_drop_b::greet AS { \"hi\" }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "pns_drop_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP PROCEDURE pns_drop_b::greet", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_procedure_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "greet")
		.unwrap();
	assert!(found.is_some(), "rolled-back DROP must leave the procedure visible in a later txn");
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pns_drop_c");
	t.admin("CREATE PROCEDURE pns_drop_c::greet AS { \"hi\" }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "pns_drop_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP PROCEDURE pns_drop_c::greet", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_procedure_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "greet")
		.unwrap();
	assert!(found.is_none(), "committed DROP PROCEDURE must not be visible in a new txn");
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pns_drop_d");
	t.admin("CREATE PROCEDURE pns_drop_d::greet AS { \"hi\" }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "pns_drop_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("DROP PROCEDURE pns_drop_d::greet", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn2 = catalog
		.find_procedure_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "greet")
		.unwrap();
	assert!(found_in_txn2.is_some(), "txn2 must still observe the procedure while txn1's DROP is uncommitted");

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn3 = catalog
		.find_procedure_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "greet")
		.unwrap();
	assert!(found_in_txn3.is_none(), "after txn1 commits, the procedure must not be visible in a later txn");
}
