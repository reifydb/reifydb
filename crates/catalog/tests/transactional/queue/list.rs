// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::interface::catalog::id::NamespaceId;
use reifydb_test_harness::engine::TestEngine;
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
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_list_a");
	t.admin("CREATE QUEUE qns_list_a::keep { msg: utf8 } WITH { fifo: {} }");
	let ns_id = namespace_id(&t, "qns_list_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE QUEUE qns_list_a::fresh { msg: utf8 } WITH { fifo: {} }", Params::None);
	txn.rql("DROP QUEUE qns_list_a::keep", Params::None);

	let all = catalog.list_queues_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name() == "fresh"));
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name() == "keep"));
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_list_b");
	t.admin("CREATE QUEUE qns_list_b::keep { msg: utf8 } WITH { fifo: {} }");
	let ns_id = namespace_id(&t, "qns_list_b");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE QUEUE qns_list_b::fresh { msg: utf8 } WITH { fifo: {} }", Params::None);
	txn.rql("DROP QUEUE qns_list_b::keep", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_queues_all(&mut Transaction::Admin(&mut txn2)).unwrap();
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name() == "keep"));
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name() == "fresh"));
}

#[test]
fn list_by_namespace_excludes_other_namespaces() {
	// The namespace filter must survive the overlay, or a listing shows a sibling
	// namespace's uncommitted queue.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_list_c");
	t.admin("CREATE NAMESPACE qns_list_c_other");
	let ns_id = namespace_id(&t, "qns_list_c");
	let other_id = namespace_id(&t, "qns_list_c_other");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE QUEUE qns_list_c::here { msg: utf8 } WITH { fifo: {} }", Params::None);
	txn.rql("CREATE QUEUE qns_list_c_other::there { msg: utf8 } WITH { fifo: {} }", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let scoped = catalog.list_queues(&mut Transaction::Admin(&mut txn2), ns_id).unwrap();
	assert_eq!(scoped.len(), 1);
	assert_eq!(scoped[0].name(), "here");

	let other = catalog.list_queues(&mut Transaction::Admin(&mut txn2), other_id).unwrap();
	assert_eq!(other.len(), 1);
	assert_eq!(other[0].name(), "there");
}
