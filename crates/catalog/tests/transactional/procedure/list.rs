// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Combined create+drop within a single txn; asserts via
// `list_procedures_for_variant`.  Scenario A is the canonical reproducer for
// the `.retain()` omission at
// `crates/catalog/src/catalog/procedure.rs:273-325`: uncommitted deletions are
// not subtracted from the result, so `keep` still appears after being dropped
// within the same transaction.
//
// Mutations use `CREATE HANDLER` (creates an event-bound procedure) and
// `DROP PROCEDURE` (the generic drop that works for any persistent procedure).

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::VariantRef;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE plist_a");
	t.admin("CREATE EVENT plist_a::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE plist_a::sink { id: int4 }");
	t.admin("CREATE HANDLER plist_a::keep ON plist_a::evt::Foo { INSERT plist_a::sink [{ id: 1 }] }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "plist_a")
			.unwrap()
			.unwrap();
		let sumtype = catalog
			.find_sumtype_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "evt")
			.unwrap()
			.unwrap();
		let variant = sumtype.variants.iter().find(|v| v.name == "foo").unwrap();
		let v = VariantRef { sumtype_id: sumtype.id, variant_tag: variant.tag };
		drop(probe);
		v
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		"CREATE HANDLER plist_a::new ON plist_a::evt::Foo { INSERT plist_a::sink [{ id: 2 }] }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	let r = txn.rql("DROP PROCEDURE plist_a::keep", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let procs =
		catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn), variant_ref).unwrap();
	assert!(
		procs.iter().any(|p| p.name() == "new"),
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
	t.admin("CREATE NAMESPACE plist_b");
	t.admin("CREATE EVENT plist_b::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE plist_b::sink { id: int4 }");
	t.admin("CREATE HANDLER plist_b::keep ON plist_b::evt::Foo { INSERT plist_b::sink [{ id: 1 }] }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "plist_b")
			.unwrap()
			.unwrap();
		let sumtype = catalog
			.find_sumtype_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "evt")
			.unwrap()
			.unwrap();
		let variant = sumtype.variants.iter().find(|v| v.name == "foo").unwrap();
		let v = VariantRef { sumtype_id: sumtype.id, variant_tag: variant.tag };
		drop(probe);
		v
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql(
		"CREATE HANDLER plist_b::new ON plist_b::evt::Foo { INSERT plist_b::sink [{ id: 2 }] }",
		Params::None,
	);
	txn.rql("DROP PROCEDURE plist_b::keep", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let procs =
		catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn2), variant_ref).unwrap();
	assert!(!procs.iter().any(|p| p.name() == "new"));
	assert!(procs.iter().any(|p| p.name() == "keep"));
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE plist_c");
	t.admin("CREATE EVENT plist_c::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE plist_c::sink { id: int4 }");
	t.admin("CREATE HANDLER plist_c::keep ON plist_c::evt::Foo { INSERT plist_c::sink [{ id: 1 }] }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "plist_c")
			.unwrap()
			.unwrap();
		let sumtype = catalog
			.find_sumtype_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "evt")
			.unwrap()
			.unwrap();
		let variant = sumtype.variants.iter().find(|v| v.name == "foo").unwrap();
		let v = VariantRef { sumtype_id: sumtype.id, variant_tag: variant.tag };
		drop(probe);
		v
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql(
		"CREATE HANDLER plist_c::new ON plist_c::evt::Foo { INSERT plist_c::sink [{ id: 2 }] }",
		Params::None,
	);
	txn.rql("DROP PROCEDURE plist_c::keep", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let procs =
		catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn2), variant_ref).unwrap();
	assert!(procs.iter().any(|p| p.name() == "new"));
	assert!(!procs.iter().any(|p| p.name() == "keep"));
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE plist_d");
	t.admin("CREATE EVENT plist_d::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE plist_d::sink { id: int4 }");
	t.admin("CREATE HANDLER plist_d::keep ON plist_d::evt::Foo { INSERT plist_d::sink [{ id: 1 }] }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "plist_d")
			.unwrap()
			.unwrap();
		let sumtype = catalog
			.find_sumtype_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "evt")
			.unwrap()
			.unwrap();
		let variant = sumtype.variants.iter().find(|v| v.name == "foo").unwrap();
		let v = VariantRef { sumtype_id: sumtype.id, variant_tag: variant.tag };
		drop(probe);
		v
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql(
		"CREATE HANDLER plist_d::new ON plist_d::evt::Foo { INSERT plist_d::sink [{ id: 2 }] }",
		Params::None,
	);
	txn1.rql("DROP PROCEDURE plist_d::keep", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn2 =
		catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn2), variant_ref).unwrap();
	assert!(!in_txn2.iter().any(|p| p.name() == "new"));
	assert!(in_txn2.iter().any(|p| p.name() == "keep"));

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn3 =
		catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn3), variant_ref).unwrap();
	assert!(in_txn3.iter().any(|p| p.name() == "new"));
	assert!(!in_txn3.iter().any(|p| p.name() == "keep"));
}
