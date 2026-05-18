// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_list_a");
	t.admin("CREATE PROCEDURE bnd_list_a::greet AS { \"hi\" }");
	t.admin(
		"CREATE HTTP BINDING bnd_list_a::keep FOR bnd_list_a::greet WITH { method: \"POST\", path: \"/keep/a\", format: \"json\" }",
	);

	let (ns_id, proc_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_list_a")
			.unwrap()
			.unwrap();
		let proc = catalog
			.find_procedure_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "greet")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_binding_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), proc.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE HTTP BINDING bnd_list_a::new_bnd FOR bnd_list_a::greet WITH { method: \"GET\", path: \"/new/a\", format: \"json\" }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP BINDING bnd_list_a::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let new_id =
		catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn), ns_id, "new_bnd").unwrap().unwrap().id;
	let bindings = catalog.list_bindings_for_procedure(&mut Transaction::Admin(&mut txn), proc_id).unwrap();
	assert!(bindings.iter().any(|b| b.id == new_id));
	assert!(!bindings.iter().any(|b| b.id == keep_id));
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_list_b");
	t.admin("CREATE PROCEDURE bnd_list_b::greet AS { \"hi\" }");
	t.admin(
		"CREATE HTTP BINDING bnd_list_b::keep FOR bnd_list_b::greet WITH { method: \"POST\", path: \"/keep/b\", format: \"json\" }",
	);

	let (ns_id, proc_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_list_b")
			.unwrap()
			.unwrap();
		let proc = catalog
			.find_procedure_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "greet")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_binding_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), proc.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE HTTP BINDING bnd_list_b::new_bnd FOR bnd_list_b::greet WITH { method: \"GET\", path: \"/new/b\", format: \"json\" }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP BINDING bnd_list_b::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let bindings = catalog.list_bindings_for_procedure(&mut Transaction::Admin(&mut txn2), proc_id).unwrap();
	assert!(catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new_bnd").unwrap().is_none());
	assert!(bindings.iter().any(|b| b.id == keep_id));
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_list_c");
	t.admin("CREATE PROCEDURE bnd_list_c::greet AS { \"hi\" }");
	t.admin(
		"CREATE HTTP BINDING bnd_list_c::keep FOR bnd_list_c::greet WITH { method: \"POST\", path: \"/keep/c\", format: \"json\" }",
	);

	let (ns_id, proc_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_list_c")
			.unwrap()
			.unwrap();
		let proc = catalog
			.find_procedure_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "greet")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_binding_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), proc.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE HTTP BINDING bnd_list_c::new_bnd FOR bnd_list_c::greet WITH { method: \"GET\", path: \"/new/c\", format: \"json\" }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP BINDING bnd_list_c::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let new_id =
		catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new_bnd").unwrap().unwrap().id;
	let bindings = catalog.list_bindings_for_procedure(&mut Transaction::Admin(&mut txn2), proc_id).unwrap();
	assert!(bindings.iter().any(|b| b.id == new_id));
	assert!(!bindings.iter().any(|b| b.id == keep_id));
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_list_d");
	t.admin("CREATE PROCEDURE bnd_list_d::greet AS { \"hi\" }");
	t.admin(
		"CREATE HTTP BINDING bnd_list_d::keep FOR bnd_list_d::greet WITH { method: \"POST\", path: \"/keep/d\", format: \"json\" }",
	);

	let (ns_id, proc_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_list_d")
			.unwrap()
			.unwrap();
		let proc = catalog
			.find_procedure_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "greet")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_binding_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), proc.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql(
		"CREATE HTTP BINDING bnd_list_d::new_bnd FOR bnd_list_d::greet WITH { method: \"GET\", path: \"/new/d\", format: \"json\" }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn1.rql("DROP BINDING bnd_list_d::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let bindings_txn2 = catalog.list_bindings_for_procedure(&mut Transaction::Admin(&mut txn2), proc_id).unwrap();
	assert!(catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new_bnd").unwrap().is_none());
	assert!(bindings_txn2.iter().any(|b| b.id == keep_id));

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let new_id =
		catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "new_bnd").unwrap().unwrap().id;
	let bindings_txn3 = catalog.list_bindings_for_procedure(&mut Transaction::Admin(&mut txn3), proc_id).unwrap();
	assert!(bindings_txn3.iter().any(|b| b.id == new_id));
	assert!(!bindings_txn3.iter().any(|b| b.id == keep_id));
}
