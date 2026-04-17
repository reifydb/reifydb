// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Combined create+drop within a single txn; asserts via
// `find_authentication_by_identity_and_method`. The authentication catalog API
// currently skips transactional-changes checks, so several of these scenarios
// are expected to fail until that is fixed.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aut_find_a");
	t.admin("CREATE AUTHENTICATION FOR aut_find_a { method: password; password: 'secret' }");

	let ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aut_find_a")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE AUTHENTICATION FOR aut_find_a { method: token; token: 'abc' }", Params::None);
	txn.rql("DROP AUTHENTICATION FOR aut_find_a { method: password }", Params::None);

	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn),
			ident_id,
			"token"
		)
		.unwrap()
		.is_some(),
		"within-txn created authentication must be findable"
	);
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
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aut_find_b");
	t.admin("CREATE AUTHENTICATION FOR aut_find_b { method: password; password: 'secret' }");

	let ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aut_find_b")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE AUTHENTICATION FOR aut_find_b { method: token; token: 'abc' }", Params::None);
	txn.rql("DROP AUTHENTICATION FOR aut_find_b { method: password }", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn2),
			ident_id,
			"token"
		)
		.unwrap()
		.is_none(),
		"rolled-back create must not leave authentication behind"
	);
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn2),
			ident_id,
			"password"
		)
		.unwrap()
		.is_some(),
		"rolled-back drop must leave original authentication intact"
	);
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aut_find_c");
	t.admin("CREATE AUTHENTICATION FOR aut_find_c { method: password; password: 'secret' }");

	let ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aut_find_c")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE AUTHENTICATION FOR aut_find_c { method: token; token: 'abc' }", Params::None);
	txn.rql("DROP AUTHENTICATION FOR aut_find_c { method: password }", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn2),
			ident_id,
			"token"
		)
		.unwrap()
		.is_some(),
		"committed create must be findable"
	);
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn2),
			ident_id,
			"password"
		)
		.unwrap()
		.is_none(),
		"committed drop must not be findable"
	);
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aut_find_d");
	t.admin("CREATE AUTHENTICATION FOR aut_find_d { method: password; password: 'secret' }");

	let ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aut_find_d")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE AUTHENTICATION FOR aut_find_d { method: token; token: 'abc' }", Params::None);
	txn1.rql("DROP AUTHENTICATION FOR aut_find_d { method: password }", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn2),
			ident_id,
			"token"
		)
		.unwrap()
		.is_none(),
		"concurrent txn must not see uncommitted authentication"
	);
	assert!(
		catalog.find_authentication_by_identity_and_method(
			&mut Transaction::Admin(&mut txn2),
			ident_id,
			"password"
		)
		.unwrap()
		.is_some(),
		"concurrent txn must still see original authentication while drop is uncommitted"
	);

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(catalog
		.find_authentication_by_identity_and_method(&mut Transaction::Admin(&mut txn3), ident_id, "token")
		.unwrap()
		.is_some());
	assert!(catalog
		.find_authentication_by_identity_and_method(&mut Transaction::Admin(&mut txn3), ident_id, "password")
		.unwrap()
		.is_none());
}
