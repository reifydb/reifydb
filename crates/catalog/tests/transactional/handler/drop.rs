// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// All tests here are ignored until RQL gains `DROP HANDLER ns::name`. When it
// lands, scenario A is the canonical reproducer for the missing `.retain()` in
// `list_procedures_for_variant` at
// `crates/catalog/src/catalog/procedure.rs:273-325`.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::VariantRef;

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hns_drop_a");
	t.admin("CREATE EVENT hns_drop_a::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hns_drop_a::sink { id: int4 }");
	t.admin("CREATE HANDLER hns_drop_a::h1 ON hns_drop_a::evt::Foo { INSERT hns_drop_a::sink [{ id: 1 }] }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hns_drop_a")
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
	let r = txn.rql("DROP HANDLER hns_drop_a::h1", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let procs = catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn), variant_ref).unwrap();
	assert!(
		!procs.iter().any(|p| p.name() == "h1"),
		"uncommitted DROP HANDLER must remove the handler from list_procedures_for_variant within its dropping txn"
	);
}

#[test]
fn rolled_back_drop_leaves_handler_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hns_drop_b");
	t.admin("CREATE EVENT hns_drop_b::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hns_drop_b::sink { id: int4 }");
	t.admin("CREATE HANDLER hns_drop_b::h1 ON hns_drop_b::evt::Foo { INSERT hns_drop_b::sink [{ id: 1 }] }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hns_drop_b")
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
	let r = txn.rql("DROP HANDLER hns_drop_b::h1", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let procs = catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn2), variant_ref).unwrap();
	assert!(procs.iter().any(|p| p.name() == "h1"));
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hns_drop_c");
	t.admin("CREATE EVENT hns_drop_c::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hns_drop_c::sink { id: int4 }");
	t.admin("CREATE HANDLER hns_drop_c::h1 ON hns_drop_c::evt::Foo { INSERT hns_drop_c::sink [{ id: 1 }] }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hns_drop_c")
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
	let r = txn.rql("DROP HANDLER hns_drop_c::h1", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let procs = catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn2), variant_ref).unwrap();
	assert!(!procs.iter().any(|p| p.name() == "h1"));
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hns_drop_d");
	t.admin("CREATE EVENT hns_drop_d::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hns_drop_d::sink { id: int4 }");
	t.admin("CREATE HANDLER hns_drop_d::h1 ON hns_drop_d::evt::Foo { INSERT hns_drop_d::sink [{ id: 1 }] }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hns_drop_d")
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
	let r = txn1.rql("DROP HANDLER hns_drop_d::h1", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn2 = catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn2), variant_ref).unwrap();
	assert!(
		in_txn2.iter().any(|p| p.name() == "h1"),
		"txn2 must still observe the handler while txn1's DROP is uncommitted"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn3 = catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn3), variant_ref).unwrap();
	assert!(!in_txn3.iter().any(|p| p.name() == "h1"));
}
