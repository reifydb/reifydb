// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::id::NamespaceId;
use reifydb_engine::test_harness::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::{params::Params, value::identity::IdentityId};

fn namespace_id(t: &TestEngine, name: &str) -> NamespaceId {
	let catalog = t.catalog();
	let mut probe = t.begin_admin(IdentityId::system()).unwrap();
	let id = catalog.find_namespace_by_name(&mut Transaction::Admin(&mut probe), name).unwrap().unwrap().id();
	drop(probe);
	id
}

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_drop_a");
	t.admin("CREATE QUEUE qns_drop_a::jobs { msg: utf8 }");
	let ns_id = namespace_id(&t, "qns_drop_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP QUEUE qns_drop_a::jobs", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	assert!(catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn), ns_id, "jobs").unwrap().is_none());
	let all = catalog.list_queues_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name() == "jobs"));
}

/// A rolled-back drop must leave the definition fully intact; a half-applied
/// drop would leave the queue unreachable by name while still holding its id.
#[test]
fn rolled_back_drop_leaves_queue_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_drop_b");
	t.admin("CREATE QUEUE qns_drop_b::jobs { msg: utf8 } WITH { partitions: 8 }");
	let ns_id = namespace_id(&t, "qns_drop_b");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP QUEUE qns_drop_b::jobs", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let queue = catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "jobs").unwrap().unwrap();
	assert_eq!(queue.partitions, 8);
	assert_eq!(queue.columns.len(), 1);
}

#[test]
fn committed_drop_is_gone_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_drop_c");
	t.admin("CREATE QUEUE qns_drop_c::jobs { msg: utf8 }");
	let ns_id = namespace_id(&t, "qns_drop_c");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP QUEUE qns_drop_c::jobs", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "jobs").unwrap().is_none());
}

/// Dropping then recreating the same name inside one transaction must end with
/// the new definition visible: the delete overlay must not shadow the create
/// that follows it.
#[test]
fn drop_then_recreate_in_same_txn_ends_with_the_new_definition() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_drop_d");
	t.admin("CREATE QUEUE qns_drop_d::jobs { msg: utf8 } WITH { partitions: 2 }");
	let ns_id = namespace_id(&t, "qns_drop_d");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("DROP QUEUE qns_drop_d::jobs", Params::None);
	let r = txn.rql("CREATE QUEUE qns_drop_d::jobs { msg: utf8 } WITH { partitions: 4 }", Params::None);
	assert!(r.error.is_none(), "recreate failed: {:?}", r.error);

	let queue = catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn), ns_id, "jobs").unwrap().unwrap();
	assert_eq!(queue.partitions, 4);

	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let queue = catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "jobs").unwrap().unwrap();
	assert_eq!(queue.partitions, 4);
}
