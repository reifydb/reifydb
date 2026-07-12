// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::segment_tree::{SegmentTreeColumnToCreate, SegmentTreeToCreate};
use reifydb_core::interface::catalog::{id::NamespaceId, key::KeySpec};
use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{
	fragment::Fragment,
	value::{constraint::TypeConstraint, value_type::ValueType},
};

fn to_create(namespace: NamespaceId, name: &str) -> SegmentTreeToCreate {
	SegmentTreeToCreate {
		name: Fragment::internal(name),
		namespace,
		columns: vec![
			SegmentTreeColumnToCreate {
				name: Fragment::internal("ts"),
				fragment: Fragment::None,
				constraint: TypeConstraint::unconstrained(ValueType::Uint8),
				properties: vec![],
				auto_increment: false,
				dictionary_id: None,
			},
			SegmentTreeColumnToCreate {
				name: Fragment::internal("val"),
				fragment: Fragment::None,
				constraint: TypeConstraint::unconstrained(ValueType::Float8),
				properties: vec![],
				auto_increment: false,
				dictionary_id: None,
			},
		],
		key: KeySpec::Integer {
			column: "ts".to_string(),
		},
		aggregates: vec![],
		partition_by: vec![],
		underlying: false,
	}
}

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE stcr_create_a");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stcr_create_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_segment_tree(&mut txn, to_create(ns_id, "s")).unwrap();

	let found = catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns_id, "s").unwrap();
	assert!(found.is_some());

	let all = catalog.list_segment_tree_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name == "s"));
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE stcr_create_b");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stcr_create_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_segment_tree(&mut txn, to_create(ns_id, "s")).unwrap();
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "s").unwrap();
	assert!(found.is_none());
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE stcr_create_c");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stcr_create_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_segment_tree(&mut txn, to_create(ns_id, "s")).unwrap();
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "s").unwrap();
	assert!(found.is_some());
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE stcr_create_d");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stcr_create_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_segment_tree(&mut txn1, to_create(ns_id, "s")).unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn2 = catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "s").unwrap();
	assert!(found_in_txn2.is_none());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found_in_txn3 = catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "s").unwrap();
	assert!(found_in_txn3.is_some());
}

#[test]
fn duplicate_name_create_fails() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE stcr_create_e");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stcr_create_e")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_segment_tree(&mut txn, to_create(ns_id, "s")).unwrap();
	let err = catalog.create_segment_tree(&mut txn, to_create(ns_id, "s")).unwrap_err();
	assert_eq!(err.diagnostic().code, "CA_003");
}
