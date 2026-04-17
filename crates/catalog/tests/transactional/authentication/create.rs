// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_create_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aut_create_a");

	let ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aut_create_a")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn
		.rql("CREATE AUTHENTICATION FOR aut_create_a { method: password; password: 'secret' }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn),
			ident_id,
			"password"
		)
		.unwrap()
		.is_some(),
		"within-txn created authentication must be findable"
	);
}

#[test]
fn rolled_back_create_is_not_visible() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aut_create_b");

	let ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aut_create_b")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn
		.rql("CREATE AUTHENTICATION FOR aut_create_b { method: password; password: 'secret' }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn2),
			ident_id,
			"password"
		)
		.unwrap()
		.is_none(),
		"rolled-back authentication must not persist"
	);
}

#[test]
fn committed_create_is_visible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aut_create_c");

	let ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aut_create_c")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn
		.rql("CREATE AUTHENTICATION FOR aut_create_c { method: password; password: 'secret' }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn2),
			ident_id,
			"password"
		)
		.unwrap()
		.is_some(),
		"committed authentication must be visible in new txn"
	);
}

#[test]
fn uncommitted_create_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aut_create_d");

	let ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aut_create_d")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1
		.rql("CREATE AUTHENTICATION FOR aut_create_d { method: password; password: 'secret' }", Params::None);
	assert!(r.error.is_none(), "create failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn2),
			ident_id,
			"password"
		)
		.unwrap()
		.is_none(),
		"concurrent txn must not see uncommitted authentication"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn3),
			ident_id,
			"password"
		)
		.unwrap()
		.is_some(),
		"after commit, authentication must be findable"
	);
}
