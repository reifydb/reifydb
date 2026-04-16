// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Combined create+drop within a single txn; asserts via all find methods:
// `find_procedure_by_name`, `find_procedure` (by id).

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pns_find_a");
	t.admin("CREATE PROCEDURE pns_find_a::keep AS { \"k\" }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "pns_find_a")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_procedure_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id());
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE PROCEDURE pns_find_a::new AS { \"n\" }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP PROCEDURE pns_find_a::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let new_proc = catalog
		.find_procedure_by_name(&mut Transaction::Admin(&mut txn), ns_id, "new")
		.unwrap()
		.expect("within-txn created procedure must be findable by name");
	
	let new_id = new_proc.id();
	assert!(
		catalog.find_procedure(&mut Transaction::Admin(&mut txn), new_id).unwrap().is_some(),
		"within-txn created procedure must be findable by id"
	);

	assert!(
		catalog.find_procedure_by_name(&mut Transaction::Admin(&mut txn), ns_id, "keep").unwrap().is_none(),
		"within-txn dropped procedure must not be findable by name"
	);
	assert!(
		catalog.find_procedure(&mut Transaction::Admin(&mut txn), keep_id).unwrap().is_none(),
		"within-txn dropped procedure must not be findable by id"
	);
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pns_find_b");
	t.admin("CREATE PROCEDURE pns_find_b::keep AS { \"k\" }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "pns_find_b")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_procedure_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id());
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE PROCEDURE pns_find_b::new AS { \"n\" }", Params::None);
	txn.rql("DROP PROCEDURE pns_find_b::keep", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_procedure_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new").unwrap().is_none()
	);
	assert!(
		catalog.find_procedure_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep").unwrap().is_some()
	);
	assert!(
		catalog.find_procedure(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some(),
		"rolled-back drop must leave procedure findable by id"
	);
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pns_find_c");
	t.admin("CREATE PROCEDURE pns_find_c::keep AS { \"k\" }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "pns_find_c")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_procedure_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id());
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE PROCEDURE pns_find_c::new AS { \"n\" }", Params::None);
	txn.rql("DROP PROCEDURE pns_find_c::keep", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let new_proc = catalog
		.find_procedure_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new")
		.unwrap()
		.expect("committed create must be findable by name");
	let new_id = new_proc.id();
	assert!(
		catalog.find_procedure(&mut Transaction::Admin(&mut txn2), new_id).unwrap().is_some(),
		"committed create must be findable by id"
	);
	assert!(
		catalog.find_procedure_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep").unwrap().is_none()
	);
	assert!(
		catalog.find_procedure(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_none(),
		"committed drop must not be findable by id"
	);
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE pns_find_d");
	t.admin("CREATE PROCEDURE pns_find_d::keep AS { \"k\" }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "pns_find_d")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_procedure_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id());
		drop(probe);
		ids
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE PROCEDURE pns_find_d::new AS { \"n\" }", Params::None);
	txn1.rql("DROP PROCEDURE pns_find_d::keep", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_procedure_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new").unwrap().is_none()
	);
	assert!(
		catalog.find_procedure_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep").unwrap().is_some()
	);
	assert!(
		catalog.find_procedure(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some(),
		"txn2 must see keep by id while txn1 is uncommitted"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let new_proc = catalog
		.find_procedure_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "new")
		.unwrap()
		.expect("after commit, new procedure must be findable by name");
	let new_id = new_proc.id();
	assert!(catalog.find_procedure(&mut Transaction::Admin(&mut txn3), new_id).unwrap().is_some());
	assert!(
		catalog.find_procedure_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "keep").unwrap().is_none()
	);
	assert!(
		catalog.find_procedure(&mut Transaction::Admin(&mut txn3), keep_id).unwrap().is_none()
	);
}
