// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Combined create+drop within a single txn; currently ignored because
// `DROP HANDLER` is not in the RQL grammar. When it lands, scenario A is the
// canonical reproducer for the `.retain()` omission in
// `list_procedures_for_variant` at
// `crates/catalog/src/catalog/procedure.rs:273-325`.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::VariantRef;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hns_list_a");
	t.admin("CREATE EVENT hns_list_a::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hns_list_a::sink { id: int4 }");
	t.admin("CREATE HANDLER hns_list_a::keep ON hns_list_a::evt::Foo { INSERT hns_list_a::sink [{ id: 1 }] }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hns_list_a")
			.unwrap()
			.unwrap();
		let sumtype = catalog
			.find_sumtype_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "evt")
			.unwrap()
			.unwrap();
		let variant = sumtype.variants.iter().find(|v| v.name == "foo").unwrap();
		let v = VariantRef {
			sumtype_id: sumtype.id,
			variant_tag: variant.tag,
		};
		drop(probe);
		v
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE HANDLER hns_list_a::fresh ON hns_list_a::evt::Foo { INSERT hns_list_a::sink [{ id: 2 }] }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP HANDLER hns_list_a::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let procs = catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn), variant_ref).unwrap();
	assert!(
		procs.iter().any(|p| p.name() == "fresh"),
		"within-txn created handler must appear in list_procedures_for_variant"
	);
	assert!(
		!procs.iter().any(|p| p.name() == "keep"),
		"within-txn dropped handler must not appear in list_procedures_for_variant"
	);
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hns_list_b");
	t.admin("CREATE EVENT hns_list_b::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hns_list_b::sink { id: int4 }");
	t.admin("CREATE HANDLER hns_list_b::keep ON hns_list_b::evt::Foo { INSERT hns_list_b::sink [{ id: 1 }] }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hns_list_b")
			.unwrap()
			.unwrap();
		let sumtype = catalog
			.find_sumtype_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "evt")
			.unwrap()
			.unwrap();
		let variant = sumtype.variants.iter().find(|v| v.name == "foo").unwrap();
		let v = VariantRef {
			sumtype_id: sumtype.id,
			variant_tag: variant.tag,
		};
		drop(probe);
		v
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql(
		"CREATE HANDLER hns_list_b::fresh ON hns_list_b::evt::Foo { INSERT hns_list_b::sink [{ id: 2 }] }",
		Params::None,
	);
	txn.rql("DROP HANDLER hns_list_b::keep", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let procs = catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn2), variant_ref).unwrap();
	assert!(!procs.iter().any(|p| p.name() == "fresh"));
	assert!(procs.iter().any(|p| p.name() == "keep"));
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hns_list_c");
	t.admin("CREATE EVENT hns_list_c::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hns_list_c::sink { id: int4 }");
	t.admin("CREATE HANDLER hns_list_c::keep ON hns_list_c::evt::Foo { INSERT hns_list_c::sink [{ id: 1 }] }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hns_list_c")
			.unwrap()
			.unwrap();
		let sumtype = catalog
			.find_sumtype_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "evt")
			.unwrap()
			.unwrap();
		let variant = sumtype.variants.iter().find(|v| v.name == "foo").unwrap();
		let v = VariantRef {
			sumtype_id: sumtype.id,
			variant_tag: variant.tag,
		};
		drop(probe);
		v
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql(
		"CREATE HANDLER hns_list_c::fresh ON hns_list_c::evt::Foo { INSERT hns_list_c::sink [{ id: 2 }] }",
		Params::None,
	);
	txn.rql("DROP HANDLER hns_list_c::keep", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let procs = catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn2), variant_ref).unwrap();
	assert!(procs.iter().any(|p| p.name() == "fresh"));
	assert!(!procs.iter().any(|p| p.name() == "keep"));
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hns_list_d");
	t.admin("CREATE EVENT hns_list_d::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hns_list_d::sink { id: int4 }");
	t.admin("CREATE HANDLER hns_list_d::keep ON hns_list_d::evt::Foo { INSERT hns_list_d::sink [{ id: 1 }] }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hns_list_d")
			.unwrap()
			.unwrap();
		let sumtype = catalog
			.find_sumtype_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "evt")
			.unwrap()
			.unwrap();
		let variant = sumtype.variants.iter().find(|v| v.name == "foo").unwrap();
		let v = VariantRef {
			sumtype_id: sumtype.id,
			variant_tag: variant.tag,
		};
		drop(probe);
		v
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql(
		"CREATE HANDLER hns_list_d::fresh ON hns_list_d::evt::Foo { INSERT hns_list_d::sink [{ id: 2 }] }",
		Params::None,
	);
	txn1.rql("DROP HANDLER hns_list_d::keep", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn2 = catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn2), variant_ref).unwrap();
	assert!(!in_txn2.iter().any(|p| p.name() == "fresh"));
	assert!(in_txn2.iter().any(|p| p.name() == "keep"));

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn3 = catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn3), variant_ref).unwrap();
	assert!(in_txn3.iter().any(|p| p.name() == "fresh"));
	assert!(!in_txn3.iter().any(|p| p.name() == "keep"));
}
