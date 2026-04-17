// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB
//
// Asserts via `list_authentications_by_method`. Like `find_*`, the current
// implementation delegates straight to storage and does not consult
// transactional changes; several scenarios here are expected to fail until
// that is fixed.

use reifydb_engine::test_prelude::*;
use reifydb_transaction::transaction::Transaction;

#[test]
fn create_and_drop_in_same_txn_reflects_both() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aul_keep_a");
	t.admin("CREATE AUTHENTICATION FOR aul_keep_a { method: password; password: 'secret' }");
	t.admin("CREATE USER aul_new_a");

	let keep_ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aul_keep_a")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};
	let new_ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aul_new_a")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE AUTHENTICATION FOR aul_new_a { method: password; password: 'x' }", Params::None);
	txn.rql("DROP AUTHENTICATION FOR aul_keep_a { method: password }", Params::None);

	let all = catalog.list_authentications_by_method(&mut Transaction::Admin(&mut txn), "password").unwrap();
	assert!(all.iter().any(|a| a.identity == new_ident_id), "within-txn created authentication must be listed");
	assert!(
		!all.iter().any(|a| a.identity == keep_ident_id),
		"within-txn dropped authentication must not be listed"
	);
}

#[test]
fn rolled_back_create_and_drop_leave_committed_state_intact() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aul_keep_b");
	t.admin("CREATE AUTHENTICATION FOR aul_keep_b { method: password; password: 'secret' }");
	t.admin("CREATE USER aul_new_b");

	let keep_ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aul_keep_b")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};
	let new_ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aul_new_b")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE AUTHENTICATION FOR aul_new_b { method: password; password: 'x' }", Params::None);
	txn.rql("DROP AUTHENTICATION FOR aul_keep_b { method: password }", Params::None);
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_authentications_by_method(&mut Transaction::Admin(&mut txn2), "password").unwrap();
	assert!(!all.iter().any(|a| a.identity == new_ident_id));
	assert!(all.iter().any(|a| a.identity == keep_ident_id));
}

#[test]
fn committed_create_and_drop_are_reflected_in_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aul_keep_c");
	t.admin("CREATE AUTHENTICATION FOR aul_keep_c { method: password; password: 'secret' }");
	t.admin("CREATE USER aul_new_c");

	let keep_ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aul_keep_c")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};
	let new_ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aul_new_c")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	txn.rql("CREATE AUTHENTICATION FOR aul_new_c { method: password; password: 'x' }", Params::None);
	txn.rql("DROP AUTHENTICATION FOR aul_keep_c { method: password }", Params::None);
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all = catalog.list_authentications_by_method(&mut Transaction::Admin(&mut txn2), "password").unwrap();
	assert!(all.iter().any(|a| a.identity == new_ident_id));
	assert!(!all.iter().any(|a| a.identity == keep_ident_id));
}

#[test]
fn concurrent_txn_sees_only_committed_state() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER aul_keep_d");
	t.admin("CREATE AUTHENTICATION FOR aul_keep_d { method: password; password: 'secret' }");
	t.admin("CREATE USER aul_new_d");

	let keep_ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aul_keep_d")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};
	let new_ident_id = {
		let mut probe = t.begin_admin(IdentityId::system()).unwrap();
		let id = catalog
			.find_identity_by_name(&mut Transaction::Admin(&mut probe), "aul_new_d")
			.unwrap()
			.unwrap()
			.id;
		drop(probe);
		id
	};

	let mut txn1 = t.begin_admin(IdentityId::system()).unwrap();
	txn1.rql("CREATE AUTHENTICATION FOR aul_new_d { method: password; password: 'x' }", Params::None);
	txn1.rql("DROP AUTHENTICATION FOR aul_keep_d { method: password }", Params::None);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let all_txn2 = catalog.list_authentications_by_method(&mut Transaction::Admin(&mut txn2), "password").unwrap();
	assert!(!all_txn2.iter().any(|a| a.identity == new_ident_id));
	assert!(all_txn2.iter().any(|a| a.identity == keep_ident_id));

	txn1.commit().unwrap();
	drop(txn2);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let all_txn3 = catalog.list_authentications_by_method(&mut Transaction::Admin(&mut txn3), "password").unwrap();
	assert!(all_txn3.iter().any(|a| a.identity == new_ident_id));
	assert!(!all_txn3.iter().any(|a| a.identity == keep_ident_id));
}
