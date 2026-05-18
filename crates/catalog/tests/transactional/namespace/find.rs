// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Combined create+drop within a single txn; asserts via all find methods:
// `find_namespace_by_name`, `find_namespace` (by id), `find_namespace_by_path`,
// `find_namespace_by_segments`.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE nsf_keep_a");

	let keep_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "nsf_keep_a")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE NAMESPACE nsf_new_a", Params::None);
	txn.rql("DROP NAMESPACE nsf_keep_a", Params::None);

	let new_ns = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "nsf_new_a")
		.unwrap()
		.expect("within-txn created namespace must be findable by name");
	let new_id = new_ns.id();
	assert!(
		catalog.find_namespace(&mut Transaction::Admin(&mut txn), new_id).unwrap().is_some(),
		"within-txn created namespace must be findable by id"
	);
	assert!(
		catalog
			.find_namespace_by_path(&mut Transaction::Admin(&mut txn), "nsf_new_a")
			.unwrap()
			.is_some(),
		"within-txn created namespace must be findable by path"
	);
	assert!(
		catalog
			.find_namespace_by_segments(&mut Transaction::Admin(&mut txn), &["nsf_new_a"])
			.unwrap()
			.is_some(),
		"within-txn created namespace must be findable by segments"
	);

	assert!(
		catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut txn), "nsf_keep_a")
			.unwrap()
			.is_none(),
		"within-txn dropped namespace must not be findable by name"
	);

	assert!(
		catalog.find_namespace(&mut Transaction::Admin(&mut txn), keep_id).unwrap().is_none(),
		"within-txn dropped namespace must not be findable by id"
	);
	assert!(
		catalog
			.find_namespace_by_path(&mut Transaction::Admin(&mut txn), "nsf_keep_a")
			.unwrap()
			.is_none(),
		"within-txn dropped namespace must not be findable by path"
	);
	assert!(
		catalog
			.find_namespace_by_segments(&mut Transaction::Admin(&mut txn), &["nsf_keep_a"])
			.unwrap()
			.is_none(),
		"within-txn dropped namespace must not be findable by segments"
	);
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE nsf_keep_b");

	let keep_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "nsf_keep_b")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE NAMESPACE nsf_new_b", Params::None);
	txn.rql("DROP NAMESPACE nsf_keep_b", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "nsf_new_b")
			.unwrap()
			.is_none()
	);
	assert!(
		catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "nsf_keep_b")
			.unwrap()
			.is_some()
	);
	assert!(
		catalog.find_namespace(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some(),
		"rolled-back drop must leave namespace findable by id"
	);
	assert!(
		catalog
			.find_namespace_by_path(&mut Transaction::Admin(&mut txn2), "nsf_keep_b")
			.unwrap()
			.is_some()
	);
	assert!(
		catalog
			.find_namespace_by_segments(&mut Transaction::Admin(&mut txn2), &["nsf_keep_b"])
			.unwrap()
			.is_some()
	);
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE nsf_keep_c");

	let keep_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "nsf_keep_c")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE NAMESPACE nsf_new_c", Params::None);
	txn.rql("DROP NAMESPACE nsf_keep_c", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let new_ns = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "nsf_new_c")
		.unwrap()
		.expect("committed create must be findable by name");
	let new_id = new_ns.id();
	assert!(
		catalog.find_namespace(&mut Transaction::Admin(&mut txn2), new_id).unwrap().is_some(),
		"committed create must be findable by id"
	);
	assert!(
		catalog
			.find_namespace_by_path(&mut Transaction::Admin(&mut txn2), "nsf_new_c")
			.unwrap()
			.is_some()
	);
	assert!(
		catalog
			.find_namespace_by_segments(&mut Transaction::Admin(&mut txn2), &["nsf_new_c"])
			.unwrap()
			.is_some()
	);

	assert!(
		catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "nsf_keep_c")
			.unwrap()
			.is_none()
	);
	assert!(
		catalog.find_namespace(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_none(),
		"committed drop must not be findable by id"
	);
	assert!(
		catalog
			.find_namespace_by_path(&mut Transaction::Admin(&mut txn2), "nsf_keep_c")
			.unwrap()
			.is_none()
	);
	assert!(
		catalog
			.find_namespace_by_segments(&mut Transaction::Admin(&mut txn2), &["nsf_keep_c"])
			.unwrap()
			.is_none()
	);
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE NAMESPACE nsf_keep_d");

	let keep_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut probe), "nsf_keep_d")
			.unwrap()
			.unwrap()
			.id();
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE NAMESPACE nsf_new_d", Params::None);
	txn1.rql("DROP NAMESPACE nsf_keep_d", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "nsf_new_d")
			.unwrap()
			.is_none()
	);
	assert!(
		catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut txn2), "nsf_keep_d")
			.unwrap()
			.is_some()
	);
	assert!(
		catalog.find_namespace(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some(),
		"txn2 must see keep by id while txn1 is uncommitted"
	);
	assert!(
		catalog
			.find_namespace_by_path(&mut Transaction::Admin(&mut txn2), "nsf_keep_d")
			.unwrap()
			.is_some()
	);
	assert!(
		catalog
			.find_namespace_by_segments(&mut Transaction::Admin(&mut txn2), &["nsf_keep_d"])
			.unwrap()
			.is_some()
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let new_ns = catalog
		.find_namespace_by_name(&mut Transaction::Admin(&mut txn3), "nsf_new_d")
		.unwrap()
		.expect("after commit, new namespace must be findable by name");
	let new_id = new_ns.id();
	assert!(
		catalog.find_namespace(&mut Transaction::Admin(&mut txn3), new_id).unwrap().is_some()
	);
	assert!(
		catalog
			.find_namespace_by_path(&mut Transaction::Admin(&mut txn3), "nsf_new_d")
			.unwrap()
			.is_some()
	);
	assert!(
		catalog
			.find_namespace_by_segments(&mut Transaction::Admin(&mut txn3), &["nsf_new_d"])
			.unwrap()
			.is_some()
	);

	assert!(
		catalog
			.find_namespace_by_name(&mut Transaction::Admin(&mut txn3), "nsf_keep_d")
			.unwrap()
			.is_none()
	);
	assert!(
		catalog.find_namespace(&mut Transaction::Admin(&mut txn3), keep_id).unwrap().is_none()
	);
	assert!(
		catalog
			.find_namespace_by_path(&mut Transaction::Admin(&mut txn3), "nsf_keep_d")
			.unwrap()
			.is_none()
	);
	assert!(
		catalog
			.find_namespace_by_segments(&mut Transaction::Admin(&mut txn3), &["nsf_keep_d"])
			.unwrap()
			.is_none()
	);
}
