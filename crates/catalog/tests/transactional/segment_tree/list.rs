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
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let ns_id = setup(&t, "stli_list_a", "keep");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_segment_tree(&mut txn, to_create(ns_id, "new")).unwrap();
	let keep =
		catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns_id, "keep").unwrap().unwrap();
	catalog.drop_segment_tree(&mut txn, keep).unwrap();

	let all = catalog.list_segment_tree_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name == "new"));
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name == "keep"));
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let ns_id = setup(&t, "stli_list_b", "keep");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_segment_tree(&mut txn, to_create(ns_id, "new")).unwrap();
	let keep =
		catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns_id, "keep").unwrap().unwrap();
	catalog.drop_segment_tree(&mut txn, keep).unwrap();
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_segment_tree_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name == "new"));
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name == "keep"));
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let ns_id = setup(&t, "stli_list_c", "keep");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_segment_tree(&mut txn, to_create(ns_id, "new")).unwrap();
	let keep =
		catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn), ns_id, "keep").unwrap().unwrap();
	catalog.drop_segment_tree(&mut txn, keep).unwrap();
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_segment_tree_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name == "new"));
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name == "keep"));
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let ns_id = setup(&t, "stli_list_d", "keep");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	catalog.create_segment_tree(&mut txn1, to_create(ns_id, "new")).unwrap();
	let keep =
		catalog.find_segment_tree_by_name(&mut Transaction::Admin(&mut txn1), ns_id, "keep").unwrap().unwrap();
	catalog.drop_segment_tree(&mut txn1, keep).unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn2 = catalog.list_segment_tree_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(!in_txn2.iter().any(|x| x.namespace == ns_id && x.name == "new"));
	assert!(in_txn2.iter().any(|x| x.namespace == ns_id && x.name == "keep"));

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let in_txn3 = catalog.list_segment_tree_all(&mut Transaction::Admin(&mut txn3)).unwrap();
	assert!(in_txn3.iter().any(|x| x.namespace == ns_id && x.name == "new"));
	assert!(!in_txn3.iter().any(|x| x.namespace == ns_id && x.name == "keep"));
}
