// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rbns_drop_a");
	t.admin("CREATE RINGBUFFER rbns_drop_a::rb { msg: utf8 } WITH { capacity: 32 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "rbns_drop_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP RINGBUFFER rbns_drop_a::rb", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	assert!(
		catalog
			.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn), ns_id, "rb")
			.unwrap()
			.is_none()
	);
	let all = catalog.list_ringbuffers_all(&mut Transaction::Admin(&mut txn)).unwrap();
	assert!(!all.iter().any(|x| x.namespace == ns_id && x.name() == "rb"));
}

#[test]
fn rolled_back_drop_leaves_ringbuffer_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rbns_drop_b");
	t.admin("CREATE RINGBUFFER rbns_drop_b::rb { msg: utf8 } WITH { capacity: 32 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "rbns_drop_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP RINGBUFFER rbns_drop_b::rb", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "rb")
			.unwrap()
			.is_some()
	);
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rbns_drop_c");
	t.admin("CREATE RINGBUFFER rbns_drop_c::rb { msg: utf8 } WITH { capacity: 32 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "rbns_drop_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP RINGBUFFER rbns_drop_c::rb", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "rb")
			.unwrap()
			.is_none()
	);
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rbns_drop_d");
	t.admin("CREATE RINGBUFFER rbns_drop_d::rb { msg: utf8 } WITH { capacity: 32 }");

	let ns_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "rbns_drop_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("DROP RINGBUFFER rbns_drop_d::rb", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "rb")
			.unwrap()
			.is_some()
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "rb")
			.unwrap()
			.is_none()
	);
}
