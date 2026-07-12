// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB
//
// Txn-visibility semantics for drop. Node-key cleanup on drop is pinned at
// the store level (store/segment_tree/drop.rs::test_drop_segment_tree_removes_node_entries).

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

fn setup(t: &TestEngine, namespace: &str, name: &str) -> NamespaceId {
	t.admin(&format!("CREATE NAMESPACE {namespace}"));
	let catalog = t.catalog();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let ns_id = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut txn), namespace).unwrap().unwrap().id();
	catalog.create_segment_tree(&mut txn, to_create(ns_id, name)).unwrap();
	txn.commit().unwrap();
	ns_id
}

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let ns_id = setup(&t, "stdr_drop_a", "s");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let tree = catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns_id, "s").unwrap().unwrap();
	catalog.drop_segment_tree(&mut txn, tree).unwrap();

	assert!(catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns_id, "s").unwrap().is_none());
	let all = catalog.list_segment_tree_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name == "s"));
}

#[test]
fn rolled_back_drop_leaves_segment_tree_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let ns_id = setup(&t, "stdr_drop_b", "s");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let tree = catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns_id, "s").unwrap().unwrap();
	catalog.drop_segment_tree(&mut txn, tree).unwrap();
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "s").unwrap().is_some());
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let ns_id = setup(&t, "stdr_drop_c", "s");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let tree = catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns_id, "s").unwrap().unwrap();
	catalog.drop_segment_tree(&mut txn, tree).unwrap();
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "s").unwrap().is_none());
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let ns_id = setup(&t, "stdr_drop_d", "s");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let tree = catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn1), ns_id, "s").unwrap().unwrap();
	catalog.drop_segment_tree(&mut txn1, tree).unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "s").unwrap().is_some());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "s").unwrap().is_none());
}
