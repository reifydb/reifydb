// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Combined create+drop within a single txn; asserts via all find methods:
// `find_handler_by_name`, `find_handler_by_id`.
//
// All scenarios are ignored until `DROP HANDLER` lands in the RQL grammar.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hnf_ns_a");
	t.admin("CREATE EVENT hnf_ns_a::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hnf_ns_a::sink { id: int4 }");
	t.admin("CREATE HANDLER hnf_ns_a::keep ON hnf_ns_a::evt::Foo { INSERT hnf_ns_a::sink [{ id: 1 }] }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hnf_ns_a")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_handler_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE HANDLER hnf_ns_a::new ON hnf_ns_a::evt::Foo { INSERT hnf_ns_a::sink [{ id: 2 }] }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP HANDLER hnf_ns_a::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let new_handler = catalog
		.find_handler_by_name(&mut Transaction::Admin(&mut txn), ns_id, "new")
		.unwrap()
		.expect("within-txn created handler must be findable by name");
	let new_id = new_handler.id;
	assert!(
		catalog.find_handler_by_id(&mut Transaction::Admin(&mut txn), new_id).unwrap().is_some(),
		"within-txn created handler must be findable by id"
	);

	assert!(
		catalog.find_handler_by_name(&mut Transaction::Admin(&mut txn), ns_id, "keep").unwrap().is_none(),
		"within-txn dropped handler must not be findable by name"
	);
	assert!(
		catalog.find_handler_by_id(&mut Transaction::Admin(&mut txn), keep_id).unwrap().is_none(),
		"within-txn dropped handler must not be findable by id"
	);
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hnf_ns_b");
	t.admin("CREATE EVENT hnf_ns_b::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hnf_ns_b::sink { id: int4 }");
	t.admin("CREATE HANDLER hnf_ns_b::keep ON hnf_ns_b::evt::Foo { INSERT hnf_ns_b::sink [{ id: 1 }] }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hnf_ns_b")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_handler_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql(
		"CREATE HANDLER hnf_ns_b::new ON hnf_ns_b::evt::Foo { INSERT hnf_ns_b::sink [{ id: 2 }] }",
		Params::None,
	);
	txn.rql("DROP HANDLER hnf_ns_b::keep", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_handler_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new").unwrap().is_none());
	assert!(catalog.find_handler_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep").unwrap().is_some());
	assert!(
		catalog.find_handler_by_id(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some(),
		"rolled-back drop must leave handler findable by id"
	);
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hnf_ns_c");
	t.admin("CREATE EVENT hnf_ns_c::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hnf_ns_c::sink { id: int4 }");
	t.admin("CREATE HANDLER hnf_ns_c::keep ON hnf_ns_c::evt::Foo { INSERT hnf_ns_c::sink [{ id: 1 }] }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hnf_ns_c")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_handler_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql(
		"CREATE HANDLER hnf_ns_c::new ON hnf_ns_c::evt::Foo { INSERT hnf_ns_c::sink [{ id: 2 }] }",
		Params::None,
	);
	txn.rql("DROP HANDLER hnf_ns_c::keep", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let new_handler = catalog
		.find_handler_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new")
		.unwrap()
		.expect("committed create must be findable by name");
	let new_id = new_handler.id;
	assert!(
		catalog.find_handler_by_id(&mut Transaction::Admin(&mut txn2), new_id).unwrap().is_some(),
		"committed create must be findable by id"
	);
	assert!(catalog.find_handler_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep").unwrap().is_none());
	assert!(
		catalog.find_handler_by_id(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_none(),
		"committed drop must not be findable by id"
	);
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hnf_ns_d");
	t.admin("CREATE EVENT hnf_ns_d::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hnf_ns_d::sink { id: int4 }");
	t.admin("CREATE HANDLER hnf_ns_d::keep ON hnf_ns_d::evt::Foo { INSERT hnf_ns_d::sink [{ id: 1 }] }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hnf_ns_d")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_handler_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql(
		"CREATE HANDLER hnf_ns_d::new ON hnf_ns_d::evt::Foo { INSERT hnf_ns_d::sink [{ id: 2 }] }",
		Params::None,
	);
	txn1.rql("DROP HANDLER hnf_ns_d::keep", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_handler_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new").unwrap().is_none());
	assert!(catalog.find_handler_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep").unwrap().is_some());
	assert!(
		catalog.find_handler_by_id(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some(),
		"txn2 must see keep by id while txn1 is uncommitted"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let new_handler = catalog
		.find_handler_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "new")
		.unwrap()
		.expect("after commit, new handler must be findable by name");
	let new_id = new_handler.id;
	assert!(catalog.find_handler_by_id(&mut Transaction::Admin(&mut txn3), new_id).unwrap().is_some());
	assert!(catalog.find_handler_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "keep").unwrap().is_none());
	assert!(catalog.find_handler_by_id(&mut Transaction::Admin(&mut txn3), keep_id).unwrap().is_none());
}
