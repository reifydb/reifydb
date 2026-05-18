// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Combined create+drop within a single txn; asserts via `find_binding` (by id).

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_find_a");
	t.admin("CREATE PROCEDURE bnd_find_a::greet AS { \"hi\" }");
	t.admin(
		"CREATE HTTP BINDING bnd_find_a::keep FOR bnd_find_a::greet WITH { method: \"POST\", path: \"/keep/a\", format: \"json\" }",
	);

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_find_a")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_binding_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE HTTP BINDING bnd_find_a::new_bnd FOR bnd_find_a::greet WITH { method: \"GET\", path: \"/new/a\", format: \"json\" }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP BINDING bnd_find_a::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let new_id = catalog
		.find_binding_by_name(&mut Transaction::Admin(&mut txn), ns_id, "new_bnd")
		.unwrap()
		.expect("within-txn created binding must be findable by name")
		.id;
	assert!(
		catalog.find_binding(&mut Transaction::Admin(&mut txn), new_id).unwrap().is_some(),
		"within-txn created binding must be findable by id"
	);
	assert!(
		catalog.find_binding(&mut Transaction::Admin(&mut txn), keep_id).unwrap().is_none(),
		"within-txn dropped binding must not be findable"
	);
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_find_b");
	t.admin("CREATE PROCEDURE bnd_find_b::greet AS { \"hi\" }");
	t.admin(
		"CREATE HTTP BINDING bnd_find_b::keep FOR bnd_find_b::greet WITH { method: \"POST\", path: \"/keep/b\", format: \"json\" }",
	);

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_find_b")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_binding_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE HTTP BINDING bnd_find_b::new_bnd FOR bnd_find_b::greet WITH { method: \"GET\", path: \"/new/b\", format: \"json\" }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP BINDING bnd_find_b::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new_bnd").unwrap().is_none());
	assert!(
		catalog.find_binding(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some(),
		"rolled-back drop must leave binding findable"
	);
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_find_c");
	t.admin("CREATE PROCEDURE bnd_find_c::greet AS { \"hi\" }");
	t.admin(
		"CREATE HTTP BINDING bnd_find_c::keep FOR bnd_find_c::greet WITH { method: \"POST\", path: \"/keep/c\", format: \"json\" }",
	);

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_find_c")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_binding_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE HTTP BINDING bnd_find_c::new_bnd FOR bnd_find_c::greet WITH { method: \"GET\", path: \"/new/c\", format: \"json\" }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP BINDING bnd_find_c::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let new_binding = catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new_bnd").unwrap();
	assert!(new_binding.is_some(), "committed create must be findable");
	assert!(
		catalog.find_binding(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_none(),
		"committed drop must not be findable"
	);
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_find_d");
	t.admin("CREATE PROCEDURE bnd_find_d::greet AS { \"hi\" }");
	t.admin(
		"CREATE HTTP BINDING bnd_find_d::keep FOR bnd_find_d::greet WITH { method: \"POST\", path: \"/keep/d\", format: \"json\" }",
	);

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_find_d")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_binding_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql(
		"CREATE HTTP BINDING bnd_find_d::new_bnd FOR bnd_find_d::greet WITH { method: \"GET\", path: \"/new/d\", format: \"json\" }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn1.rql("DROP BINDING bnd_find_d::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new_bnd").unwrap().is_none());
	assert!(
		catalog.find_binding(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some(),
		"txn2 must see keep while txn1 is uncommitted"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "new_bnd").unwrap().is_some());
	assert!(catalog.find_binding(&mut Transaction::Admin(&mut txn3), keep_id).unwrap().is_none());
}
