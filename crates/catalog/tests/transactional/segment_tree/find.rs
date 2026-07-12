// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// Combined create+drop within a single txn; asserts via all find methods:
// `find_segment_tree_by_name`, `find_segment_tree` (by id). Also covers
// find_by_name across namespaces.

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
		columns: vec![SegmentTreeColumnToCreate {
			name: Fragment::internal("ts"),
			fragment: Fragment::None,
			constraint: TypeConstraint::unconstrained(ValueType::Uint8),
			properties: vec![],
			auto_increment: false,
			dictionary_id: None,
		}],
		key: KeySpec::Integer {
			column: "ts".to_string(),
		},
		aggregates: vec![],
		partition_by: vec![],
		underlying: false,
	}
}

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE stfi_ns_a");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stfi_ns_a")
			.unwrap()
			.unwrap();
		let keep = catalog.create_segment_tree(&mut probe, to_create(ns.id(), "keep")).unwrap();
		let ids = (ns.id(), keep.id);
		probe.commit().unwrap();
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_segment_tree(&mut txn, to_create(ns_id, "new")).unwrap();
	let keep =
		catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns_id, "keep").unwrap().unwrap();
	catalog.drop_segment_tree(&mut txn, keep).unwrap();

	let new_tree = catalog
		.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns_id, "new")
		.unwrap()
		.expect("within-txn created segment tree must be findable by name");
	let new_id = new_tree.id;
	assert!(
		catalog.find_segment_tree(&mut Transaction::Admin(&mut txn), new_id).unwrap().is_some(),
		"within-txn created segment tree must be findable by id"
	);

	assert!(
		catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns_id, "keep").unwrap().is_none(),
		"within-txn dropped segment tree must not be findable by name"
	);
	assert!(
		catalog.find_segment_tree(&mut Transaction::Admin(&mut txn), keep_id).unwrap().is_none(),
		"within-txn dropped segment tree must not be findable by id"
	);
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE stfi_ns_b");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stfi_ns_b")
			.unwrap()
			.unwrap();
		let keep = catalog.create_segment_tree(&mut probe, to_create(ns.id(), "keep")).unwrap();
		let ids = (ns.id(), keep.id);
		probe.commit().unwrap();
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_segment_tree(&mut txn, to_create(ns_id, "new")).unwrap();
	let keep =
		catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns_id, "keep").unwrap().unwrap();
	catalog.drop_segment_tree(&mut txn, keep).unwrap();
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new").unwrap().is_none());
	assert!(catalog
		.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep")
		.unwrap()
		.is_some());
	assert!(catalog.find_segment_tree(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some());
}

#[test]
fn find_by_name_across_namespaces() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE stfi_ns_c1");
	t.admin("CREATE NAMESPACE stfi_ns_c2");

	let (ns1, ns2) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns1 = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stfi_ns_c1")
			.unwrap()
			.unwrap()
			.id();
		let ns2 = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "stfi_ns_c2")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		(ns1, ns2)
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let tree1 = catalog.create_segment_tree(&mut txn, to_create(ns1, "shared_name")).unwrap();
	let tree2 = catalog.create_segment_tree(&mut txn, to_create(ns2, "shared_name")).unwrap();
	assert_ne!(tree1.id, tree2.id);

	let found1 = catalog
		.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns1, "shared_name")
		.unwrap()
		.unwrap();
	let found2 = catalog
		.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns2, "shared_name")
		.unwrap()
		.unwrap();
	assert_eq!(found1.id, tree1.id);
	assert_eq!(found2.id, tree2.id);
	assert!(catalog
		.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns1, "nonexistent")
		.unwrap()
		.is_none());
}
