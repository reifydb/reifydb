// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn uncommitted_drop_is_reflected_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aut_drop_a");
	t.admin("CREATE AUTHENTICATION FOR aut_drop_a { method: password; password: 'secret' }");

	let ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aut_drop_a")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP AUTHENTICATION FOR aut_drop_a { method: password }", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn),
			ident_id,
			"password"
		)
		.unwrap()
		.is_none(),
		"within-txn dropped authentication must not be findable"
	);
}

#[test]
fn rolled_back_drop_leaves_authentication_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aut_drop_b");
	t.admin("CREATE AUTHENTICATION FOR aut_drop_b { method: password; password: 'secret' }");

	let ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aut_drop_b")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP AUTHENTICATION FOR aut_drop_b { method: password }", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn2),
			ident_id,
			"password"
		)
		.unwrap()
		.is_some(),
		"rolled-back drop must leave authentication intact"
	);
}

#[test]
fn committed_drop_is_invisible_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aut_drop_c");
	t.admin("CREATE AUTHENTICATION FOR aut_drop_c { method: password; password: 'secret' }");

	let ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aut_drop_c")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn.rql("DROP AUTHENTICATION FOR aut_drop_c { method: password }", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn2),
			ident_id,
			"password"
		)
		.unwrap()
		.is_none(),
		"committed drop must not be findable in new txn"
	);
}

#[test]
fn uncommitted_drop_is_isolated_from_concurrent_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aut_drop_d");
	t.admin("CREATE AUTHENTICATION FOR aut_drop_d { method: password; password: 'secret' }");

	let ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aut_drop_d")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	let r = txn1.rql("DROP AUTHENTICATION FOR aut_drop_d { method: password }", Params::None);
	assert!(r.error.is_none(), "drop failed: {:?}", r.error);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn2),
			ident_id,
			"password"
		)
		.unwrap()
		.is_some(),
		"concurrent txn must still see authentication while drop is uncommitted"
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
		.is_none(),
		"after commit, authentication must be gone"
	);
}
