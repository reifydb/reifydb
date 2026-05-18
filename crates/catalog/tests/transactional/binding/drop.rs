// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_drop_a");
	t.admin("CREATE PROCEDURE bnd_drop_a::greet AS { \"hi\" }");
	t.admin(
		"CREATE HTTP BINDING bnd_drop_a::greet_http FOR bnd_drop_a::greet WITH { method: \"POST\", path: \"/bnd_drop_a/greet\", format: \"json\" }",
	);

	let (ns_id, proc_id, binding_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_drop_a")
			.unwrap()
			.unwrap();
		let proc = catalog
			.find_procedure_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "greet")
			.unwrap()
			.unwrap();
		let binding = catalog
			.find_binding_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "greet_http")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), proc.id(), binding.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP BINDING bnd_drop_a::greet_http", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	assert!(
		catalog.find_binding(&mut Transaction::Admin(&mut txn), binding_id).unwrap().is_none(),
		"within-txn dropped binding must not be findable by id"
	);
	assert!(
		catalog.find_binding_by_name(&mut Transaction::Admin(&mut txn), ns_id, "greet_http").unwrap().is_none(),
		"within-txn dropped binding must not be findable by name"
	);
	let bindings = catalog.list_bindings_for_procedure(&mut Transaction::Admin(&mut txn), proc_id).unwrap();
	assert!(!bindings.iter().any(|b| b.id == binding_id));
}

#[test]
fn rolled_back_drop_leaves_binding_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_drop_b");
	t.admin("CREATE PROCEDURE bnd_drop_b::greet AS { \"hi\" }");
	t.admin(
		"CREATE HTTP BINDING bnd_drop_b::greet_http FOR bnd_drop_b::greet WITH { method: \"POST\", path: \"/bnd_drop_b/greet\", format: \"json\" }",
	);

	let (ns_id, proc_id, binding_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_drop_b")
			.unwrap()
			.unwrap();
		let proc = catalog
			.find_procedure_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "greet")
			.unwrap()
			.unwrap();
		let binding = catalog
			.find_binding_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "greet_http")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), proc.id(), binding.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP BINDING bnd_drop_b::greet_http", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_binding(&mut Transaction::Admin(&mut txn2), binding_id).unwrap().is_some(),
		"rolled-back drop must leave binding intact"
	);
	assert!(catalog
		.find_binding_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "greet_http")
		.unwrap()
		.is_some());
	let bindings = catalog.list_bindings_for_procedure(&mut Transaction::Admin(&mut txn2), proc_id).unwrap();
	assert!(bindings.iter().any(|b| b.id == binding_id));
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_drop_c");
	t.admin("CREATE PROCEDURE bnd_drop_c::greet AS { \"hi\" }");
	t.admin(
		"CREATE HTTP BINDING bnd_drop_c::greet_http FOR bnd_drop_c::greet WITH { method: \"POST\", path: \"/bnd_drop_c/greet\", format: \"json\" }",
	);

	let (ns_id, proc_id, binding_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_drop_c")
			.unwrap()
			.unwrap();
		let proc = catalog
			.find_procedure_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "greet")
			.unwrap()
			.unwrap();
		let binding = catalog
			.find_binding_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "greet_http")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), proc.id(), binding.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP BINDING bnd_drop_c::greet_http", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_binding(&mut Transaction::Admin(&mut txn2), binding_id).unwrap().is_none(),
		"committed drop must not be findable in new txn"
	);
	assert!(catalog
		.find_binding_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "greet_http")
		.unwrap()
		.is_none());
	let bindings = catalog.list_bindings_for_procedure(&mut Transaction::Admin(&mut txn2), proc_id).unwrap();
	assert!(!bindings.iter().any(|b| b.id == binding_id));
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE bnd_drop_d");
	t.admin("CREATE PROCEDURE bnd_drop_d::greet AS { \"hi\" }");
	t.admin(
		"CREATE HTTP BINDING bnd_drop_d::greet_http FOR bnd_drop_d::greet WITH { method: \"POST\", path: \"/bnd_drop_d/greet\", format: \"json\" }",
	);

	let (proc_id, binding_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "bnd_drop_d")
			.unwrap()
			.unwrap();
		let proc = catalog
			.find_procedure_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "greet")
			.unwrap()
			.unwrap();
		let binding = catalog
			.find_binding_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "greet_http")
			.unwrap()
			.unwrap();
		let ids = (proc.id(), binding.id);
		drop(probe);
		ids
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("DROP BINDING bnd_drop_d::greet_http", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_binding(&mut Transaction::Admin(&mut txn2), binding_id).unwrap().is_some(),
		"concurrent txn must still see binding while drop is uncommitted"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_binding(&mut Transaction::Admin(&mut txn3), binding_id).unwrap().is_none(),
		"after commit, binding must be gone"
	);
	let bindings = catalog.list_bindings_for_procedure(&mut Transaction::Admin(&mut txn3), proc_id).unwrap();
	assert!(!bindings.iter().any(|b| b.id == binding_id));
}
