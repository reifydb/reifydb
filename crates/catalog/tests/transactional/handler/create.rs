// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// `CREATE HANDLER ns::name ON ns::event::Variant { body }` stores a Procedure
// with `RqlTrigger::Event { variant }`. The bug lives in
// `list_procedures_for_variant` at `crates/catalog/src/catalog/procedure.rs:273-325`:
// uncommitted creates are added but uncommitted deletions are not removed.
// Only the CREATE side is exercised here; the DROP side is in `drop.rs` and
// currently ignored pending RQL `DROP HANDLER`.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;
use reifydb_type::value::sumtype::VariantRef;

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hns_create_a");
	t.admin("CREATE EVENT hns_create_a::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hns_create_a::sink { id: int4 }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hns_create_a")
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
		"CREATE HANDLER hns_create_a::h1 ON hns_create_a::evt::Foo { INSERT hns_create_a::sink [{ id: 1 }] }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let procs = catalog.list_procedures_for_variant(&mut Transaction::Admin(&mut txn), variant_ref).unwrap();
	assert!(
		procs.iter().any(|p| p.name() == "h1"),
		"uncommitted CREATE HANDLER must appear in list_procedures_for_variant within its creating txn"
	);
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hns_create_b");
	t.admin("CREATE EVENT hns_create_b::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hns_create_b::sink { id: int4 }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hns_create_b")
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
		"CREATE HANDLER hns_create_b::h1 ON hns_create_b::evt::Foo { INSERT hns_create_b::sink [{ id: 1 }] }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let procs = catalog
		.list_procedures_for_variant(&mut Transaction::Admin(&mut txn2), variant_ref)
		.unwrap();
	assert!(!procs.iter().any(|p| p.name() == "h1"));
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hns_create_c");
	t.admin("CREATE EVENT hns_create_c::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hns_create_c::sink { id: int4 }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hns_create_c")
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
		"CREATE HANDLER hns_create_c::h1 ON hns_create_c::evt::Foo { INSERT hns_create_c::sink [{ id: 1 }] }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let procs = catalog
		.list_procedures_for_variant(&mut Transaction::Admin(&mut txn2), variant_ref)
		.unwrap();
	assert!(procs.iter().any(|p| p.name() == "h1"));
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE hns_create_d");
	t.admin("CREATE EVENT hns_create_d::evt { Foo { id: int4 } }");
	t.admin("CREATE TABLE hns_create_d::sink { id: int4 }");

	let variant_ref = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "hns_create_d")
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
	let r = txn1.rql(
		"CREATE HANDLER hns_create_d::h1 ON hns_create_d::evt::Foo { INSERT hns_create_d::sink [{ id: 1 }] }",
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn2 = catalog
		.list_procedures_for_variant(&mut Transaction::Admin(&mut txn2), variant_ref)
		.unwrap();
	assert!(!in_txn2.iter().any(|p| p.name() == "h1"), "txn2 must not observe txn1's uncommitted CREATE HANDLER");

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn3 = catalog
		.list_procedures_for_variant(&mut Transaction::Admin(&mut txn3), variant_ref)
		.unwrap();
	assert!(in_txn3.iter().any(|p| p.name() == "h1"));
}
