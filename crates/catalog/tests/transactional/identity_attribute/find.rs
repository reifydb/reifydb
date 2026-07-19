// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_engine::test_harness::TestEngine;
use reifydb_value::{params::Params, value::{identity::IdentityId}};
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iaf_keep_a: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER ATTRIBUTE iaf_new_a: utf8", Params::None);
	txn.rql("DROP USER ATTRIBUTE iaf_keep_a", Params::None);

	assert!(catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), "iaf_new_a")
		.unwrap()
		.is_some());
	assert!(catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), "iaf_keep_a")
		.unwrap()
		.is_none());
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iaf_keep_b: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER ATTRIBUTE iaf_new_b: utf8", Params::None);
	txn.rql("DROP USER ATTRIBUTE iaf_keep_b", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn2), "iaf_new_b")
		.unwrap()
		.is_none());
	assert!(catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn2), "iaf_keep_b")
		.unwrap()
		.is_some());
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iaf_keep_c: utf8");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER ATTRIBUTE iaf_new_c: utf8", Params::None);
	txn.rql("DROP USER ATTRIBUTE iaf_keep_c", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn2), "iaf_new_c")
		.unwrap()
		.is_some());
	assert!(catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn2), "iaf_keep_c")
		.unwrap()
		.is_none());
}

#[test]
fn create_then_drop_same_txn_is_not_findable_by_name() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER ATTRIBUTE iaf_ephem: utf8", Params::None);
	txn.rql("DROP USER ATTRIBUTE iaf_ephem", Params::None);

	assert!(
		catalog.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn), "iaf_ephem")
			.unwrap()
			.is_none(),
		"an attribute created then dropped in one txn must not resurrect through the overlay"
	);
}

#[test]
fn create_user_with_same_txn_dropped_attribute_is_rejected() {
	let t = TestEngine::new();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE USER ATTRIBUTE iaf_gone: utf8", Params::None);
	txn.rql("DROP USER ATTRIBUTE iaf_gone", Params::None);
	let r = txn.rql("CREATE USER iaf_victim { iaf_gone: 'x' }", Params::None);
	let error = r.error.expect("using a same-txn dropped attribute must be rejected, not resurrected");
	assert_eq!(error.diagnostic().code, "CA_091");
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER ATTRIBUTE iaf_keep_d: utf8");

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE USER ATTRIBUTE iaf_new_d: utf8", Params::None);
	txn1.rql("DROP USER ATTRIBUTE iaf_keep_d", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn2), "iaf_new_d")
		.unwrap()
		.is_none());
	assert!(catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn2), "iaf_keep_d")
		.unwrap()
		.is_some());

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn3), "iaf_new_d")
		.unwrap()
		.is_some());
	assert!(catalog
		.find_identity_attribute_by_name(&mut Transaction::Admin(&mut txn3), "iaf_keep_d")
		.unwrap()
		.is_none());
}
