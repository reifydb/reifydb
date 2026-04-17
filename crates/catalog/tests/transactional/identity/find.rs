// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Combined create+drop within a single txn; asserts via `find_identity_by_name`
// and `find_identity` (by id).

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER icf_keep_a");

	let keep_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "icf_keep_a")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER icf_new_a", Params::None);
	txn.rql("DROP USER icf_keep_a", Params::None);

	let new_ident = catalog
		.find_identity_by_name(&mut Transaction::Admin(&mut txn), "icf_new_a")
		.unwrap()
		.expect("within-txn created identity must be findable by name");
	let new_id = new_ident.id;
	assert!(
		catalog.find_identity(&mut Transaction::Admin(&mut txn), new_id).unwrap().is_some(),
		"within-txn created identity must be findable by id"
	);

	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "icf_keep_a").unwrap().is_none(),
		"within-txn dropped identity must not be findable by name"
	);
	assert!(
		catalog.find_identity(&mut Transaction::Admin(&mut txn), keep_id).unwrap().is_none(),
		"within-txn dropped identity must not be findable by id"
	);
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER icf_keep_b");

	let keep_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "icf_keep_b")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER icf_new_b", Params::None);
	txn.rql("DROP USER icf_keep_b", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "icf_new_b").unwrap().is_none());
	assert!(catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "icf_keep_b").unwrap().is_some());
	assert!(
		catalog.find_identity(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some(),
		"rolled-back drop must leave identity findable by id"
	);
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER icf_keep_c");

	let keep_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "icf_keep_c")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER icf_new_c", Params::None);
	txn.rql("DROP USER icf_keep_c", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let new_ident = catalog
		.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "icf_new_c")
		.unwrap()
		.expect("committed create must be findable by name");
	let new_id = new_ident.id;
	assert!(
		catalog.find_identity(&mut Transaction::Admin(&mut txn2), new_id).unwrap().is_some(),
		"committed create must be findable by id"
	);
	assert!(catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "icf_keep_c").unwrap().is_none());
	assert!(
		catalog.find_identity(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_none(),
		"committed drop must not be findable by id"
	);
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER icf_keep_d");

	let keep_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "icf_keep_d")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE USER icf_new_d", Params::None);
	txn1.rql("DROP USER icf_keep_d", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "icf_new_d").unwrap().is_none());
	assert!(catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "icf_keep_d").unwrap().is_some());
	assert!(
		catalog.find_identity(&mut Transaction::Admin(&mut txn2), keep_id).unwrap().is_some(),
		"txn2 must see keep by id while txn1 is uncommitted"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let new_ident = catalog
		.find_identity_by_name(&mut Transaction::Admin(&mut txn3), "icf_new_d")
		.unwrap()
		.expect("after commit, new identity must be findable by name");
	let new_id = new_ident.id;
	assert!(catalog.find_identity(&mut Transaction::Admin(&mut txn3), new_id).unwrap().is_some());
	assert!(catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn3), "icf_keep_d").unwrap().is_none());
	assert!(catalog.find_identity(&mut Transaction::Admin(&mut txn3), keep_id).unwrap().is_none());
}
