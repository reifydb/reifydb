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

/// A queue created earlier in the same admin transaction must be visible to the
/// rest of that transaction; without the change-tracking overlay a follow-up
/// statement in the same DDL script could not see what it just created.
#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_create_a");
	let ns_id = namespace_id(&t, "qns_create_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE QUEUE qns_create_a::jobs { msg: utf8 }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	assert!(catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn), ns_id, "jobs").unwrap().is_some());
	let all = catalog.list_queues_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name() == "jobs"));
}

/// Change tracking is abort-coupled: a rolled-back CREATE must leave no trace,
/// otherwise the cache would serve a queue that does not exist on disk.
#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_create_b");
	let ns_id = namespace_id(&t, "qns_create_b");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE QUEUE qns_create_b::jobs { msg: utf8 }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "jobs").unwrap().is_none());
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_create_c");
	let ns_id = namespace_id(&t, "qns_create_c");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("CREATE QUEUE qns_create_c::jobs { msg: utf8 }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "jobs").unwrap().is_some());
}

/// The overlay must be private to its own transaction: a concurrent reader must
/// not see an uncommitted definition.
#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_create_d");
	let ns_id = namespace_id(&t, "qns_create_d");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("CREATE QUEUE qns_create_d::jobs { msg: utf8 }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "jobs").unwrap().is_none());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "jobs").unwrap().is_some());
}

/// The declared options must survive the commit, not just the name: a queue that
/// loses its partition count on commit would be silently mis-scheduled later.
#[test]
fn committed_create_preserves_every_option() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_create_e");
	let ns_id = namespace_id(&t, "qns_create_e");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(
		r#"CREATE QUEUE qns_create_e::jobs { msg: utf8 } WITH { partitions: 64, ordered_by: msg, retention: { done: "3d" }, retry: { attempts: 2, backoff: "5s" } }"#,
		Params::None,
	);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let queue = catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "jobs").unwrap().unwrap();

	assert_eq!(queue.partitions, 64);
	assert_eq!(queue.ordered_by, Some("msg".to_string()));
	assert!(queue.retention.done.is_some());
	assert_eq!(queue.retry.attempts, 2);
}
