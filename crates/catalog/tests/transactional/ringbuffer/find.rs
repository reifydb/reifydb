// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Combined create+drop within a single txn; asserts via all find methods:
// `find_ringbuffer_by_name`, `find_ringbuffer` (by id).

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rbf_ns_a");
	t.admin("CREATE RINGBUFFER rbf_ns_a::keep { msg: utf8 } WITH { capacity: 32 }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "rbf_ns_a")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_ringbuffer_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE RINGBUFFER rbf_ns_a::new { msg: utf8 } WITH { capacity: 32 }", Params::None);
	txn.rql("DROP RINGBUFFER rbf_ns_a::keep", Params::None);

	let new_rb = catalog
		.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn), ns_id, "new")
		.unwrap()
		.expect("within-txn created ringbuffer must be findable by name");
	let new_id = new_rb.id;
	assert!(
		catalog.find_ringbuffer(&mut Transaction::Admin(&mut txn), new_id).unwrap().is_some(),
		"within-txn created ringbuffer must be findable by id"
	);

	assert!(
		catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn), ns_id, "keep").unwrap().is_none(),
		"within-txn dropped ringbuffer must not be findable by name"
	);
	assert!(
		catalog.find_ringbuffer(&mut Transaction::Admin(&mut txn), keep_id).unwrap().is_none(),
		"within-txn dropped ringbuffer must not be findable by id"
	);
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rbf_ns_b");
	t.admin("CREATE RINGBUFFER rbf_ns_b::keep { msg: utf8 } WITH { capacity: 32 }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "rbf_ns_b")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_ringbuffer_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE RINGBUFFER rbf_ns_b::new { msg: utf8 } WITH { capacity: 32 }", Params::None);
	txn.rql("DROP RINGBUFFER rbf_ns_b::keep", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new").unwrap().is_none());
	assert!(catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep").unwrap().is_some());
	assert!(catalog.find_ringbuffer(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some());
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rbf_ns_c");
	t.admin("CREATE RINGBUFFER rbf_ns_c::keep { msg: utf8 } WITH { capacity: 32 }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "rbf_ns_c")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_ringbuffer_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE RINGBUFFER rbf_ns_c::new { msg: utf8 } WITH { capacity: 32 }", Params::None);
	txn.rql("DROP RINGBUFFER rbf_ns_c::keep", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let new_rb = catalog
		.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new")
		.unwrap()
		.expect("committed create must be findable by name");
	let new_id = new_rb.id;
	assert!(catalog.find_ringbuffer(&mut Transaction::Admin(&mut txn2), new_id).unwrap().is_some());
	assert!(catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep").unwrap().is_none());
	assert!(catalog.find_ringbuffer(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_none());
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE rbf_ns_d");
	t.admin("CREATE RINGBUFFER rbf_ns_d::keep { msg: utf8 } WITH { capacity: 32 }");

	let (ns_id, keep_id) = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let ns = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "rbf_ns_d")
			.unwrap()
			.unwrap();
		let keep = catalog
			.find_ringbuffer_by_name(&mut Transaction::Admin(&mut probe), ns.id(), "keep")
			.unwrap()
			.unwrap();
		let ids = (ns.id(), keep.id);
		drop(probe);
		ids
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE RINGBUFFER rbf_ns_d::new { msg: utf8 } WITH { capacity: 32 }", Params::None);
	txn1.rql("DROP RINGBUFFER rbf_ns_d::keep", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "new").unwrap().is_none());
	assert!(catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn2), ns_id, "keep").unwrap().is_some());
	assert!(catalog.find_ringbuffer(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let new_rb = catalog
		.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "new")
		.unwrap()
		.expect("after commit, new ringbuffer must be findable by name");
	let new_id = new_rb.id;
	assert!(catalog.find_ringbuffer(&mut Transaction::Admin(&mut txn3), new_id).unwrap().is_some());
	assert!(catalog.find_ringbuffer_by_name(&mut Transaction::Admin(&mut txn3), ns_id, "keep").unwrap().is_none());
	assert!(catalog.find_ringbuffer(&mut Transaction::Admin(&mut txn3), keep_id).unwrap().is_none());
}
