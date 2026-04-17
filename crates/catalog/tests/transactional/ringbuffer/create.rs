// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

const CREATE_A: &str = "CREATE RINGBUFFER rbns_create_a::rb { msg: utf8 } WITH { capacity: 32 }";
const CREATE_B: &str = "CREATE RINGBUFFER rbns_create_b::rb { msg: utf8 } WITH { capacity: 32 }";
const CREATE_C: &str = "CREATE RINGBUFFER rbns_create_c::rb { msg: utf8 } WITH { capacity: 32 }";
const CREATE_D: &str = "CREATE RINGBUFFER rbns_create_d::rb { msg: utf8 } WITH { capacity: 32 }";

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rbns_create_a");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "rbns_create_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(CREATE_A, Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	assert!(catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn), ns_id, "rb").unwrap().is_some());
	let all = catalog.list_ringbuffers_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(all.iter().any(|x| x.namespace == ns_id && x.name() == "rb"));
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rbns_create_b");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "rbns_create_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(CREATE_B, Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "rb").unwrap().is_none());
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rbns_create_c");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "rbns_create_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql(CREATE_C, Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "rb").unwrap().is_some());
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rbns_create_d");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "rbns_create_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql(CREATE_D, Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "rb").unwrap().is_none());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "rb").unwrap().is_some());
}
