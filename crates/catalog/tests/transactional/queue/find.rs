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
fn uncommitted_create_is_findable_by_id_and_by_name() {
	// If only the name path consulted the overlay, an id captured earlier in the
	// transaction would resolve to nothing.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_find_a");
	let ns_id = namespace_id(&t, "qns_find_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE QUEUE qns_find_a::jobs { msg: utf8 } WITH { fifo: {} }", Params::None);

	let by_name = catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn), ns_id, "jobs").unwrap().unwrap();
	let by_id = catalog.find_queue(&mut Transaction::Admin(&mut txn), by_name.id).unwrap().unwrap();

	assert_eq!(by_id.id, by_name.id);
	assert_eq!(by_id.name, "jobs");
}

#[test]
fn uncommitted_drop_hides_the_queue_by_id() {
	// A stale id lookup would otherwise resurrect a queue dropped in this transaction.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_find_b");
	t.admin("CREATE QUEUE qns_find_b::jobs { msg: utf8 } WITH { fifo: {} }");
	let ns_id = namespace_id(&t, "qns_find_b");

	let queue_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_queue_by_name(&mut Transaction::Admin(&mut probe), ns_id, "jobs")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("DROP QUEUE qns_find_b::jobs", Params::None);

	assert!(catalog.find_queue(&mut Transaction::Admin(&mut txn), queue_id).unwrap().is_none());
	assert!(catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn), ns_id, "jobs").unwrap().is_none());
}

#[test]
fn uncommitted_create_does_not_leak_into_another_namespace() {
	// Name lookups stay namespace-scoped through the overlay as well.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE qns_find_c");
	t.admin("CREATE NAMESPACE qns_find_c_other");
	let ns_id = namespace_id(&t, "qns_find_c");
	let other_id = namespace_id(&t, "qns_find_c_other");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE QUEUE qns_find_c::jobs { msg: utf8 } WITH { fifo: {} }", Params::None);

	assert!(catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn), ns_id, "jobs").unwrap().is_some());
	assert!(catalog.find_queue_by_name(&mut Transaction::Admin(&mut txn), other_id, "jobs").unwrap().is_none());
}
