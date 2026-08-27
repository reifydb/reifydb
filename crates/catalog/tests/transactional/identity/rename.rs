// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::transaction::Transaction;
use reifydb_value::value::identity::{IdentityId, IdentityKind};

fn identity_id(t: &TestEngine, name: &str) -> IdentityId {
	let catalog = t.catalog();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), name)
		.unwrap()
		.unwrap_or_else(|| panic!("identity `{name}` not found"))
		.id
}

#[test]
fn uncommitted_rename_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER idn_rename_a");
	let id = identity_id(&t, "idn_rename_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.rename_identity(&mut txn, id, "idn_rename_a_new").unwrap();

	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "idn_rename_a_new").unwrap().is_some(),
		"within-txn renamed identity must be findable under the new name"
	);
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "idn_rename_a").unwrap().is_none(),
		"within-txn renamed identity must no longer be findable under the old name"
	);
}

#[test]
fn rolled_back_rename_leaves_the_old_name() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER idn_rename_b");
	let id = identity_id(&t, "idn_rename_b");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.rename_identity(&mut txn, id, "idn_rename_b_new").unwrap();
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "idn_rename_b").unwrap().is_some(),
		"rolled-back rename must leave the original name in place"
	);
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "idn_rename_b_new")
			.unwrap()
			.is_none(),
		"rolled-back rename must not leave the new name behind"
	);
}

#[test]
fn committed_rename_reindexes_the_name_in_a_new_txn() {
	// The catalog cache keeps a name -> id index. If a rename fails to evict the
	// old entry, a stale name keeps resolving to a live identity - which for
	// password login means signing in under a name the identity no longer has.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER idn_rename_c");
	let id = identity_id(&t, "idn_rename_c");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.rename_identity(&mut txn, id, "idn_rename_c_new").unwrap();
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog
		.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "idn_rename_c_new")
		.unwrap()
		.expect("renamed identity must be findable under the new name in a fresh txn");
	assert_eq!(found.id, id, "rename must not change the identity id");
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn2), "idn_rename_c").unwrap().is_none(),
		"the old name must not resolve after a committed rename"
	);
}

#[test]
fn rename_preserves_id_kind_and_enabled() {
	// Promotion of a guest renames in place: everything the identity owns is
	// keyed by its id, so the id must survive, and the rename must not quietly
	// reset the other fields on the row it rewrites.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE SERVICE idn_rename_d");
	let id = identity_id(&t, "idn_rename_d");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let renamed = catalog.rename_identity(&mut txn, id, "idn_rename_d_new").unwrap();
	txn.commit().unwrap();

	assert_eq!(renamed.id, id);
	assert_eq!(renamed.kind, IdentityKind::Service);
	assert!(renamed.enabled);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn2), id).unwrap().unwrap();
	assert_eq!(found.name, "idn_rename_d_new");
	assert_eq!(found.kind, IdentityKind::Service);
	assert!(found.enabled);
}

#[test]
fn rename_onto_a_taken_name_is_rejected() {
	// The unique name index is what guarantees one identity per email. If a
	// rename could collide, two identities would answer to the same login.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER idn_rename_e");
	t.admin("CREATE USER idn_rename_e_taken");
	let id = identity_id(&t, "idn_rename_e");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = catalog.rename_identity(&mut txn, id, "idn_rename_e_taken").unwrap_err();
	assert_eq!(err.diagnostic().code, "CA_040");

	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn), id).unwrap().unwrap();
	assert_eq!(found.name, "idn_rename_e", "a rejected rename must not have touched the row");
}

#[test]
fn rename_to_the_same_name_is_a_noop() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER idn_rename_f");
	let id = identity_id(&t, "idn_rename_f");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let renamed = catalog.rename_identity(&mut txn, id, "idn_rename_f").unwrap();
	assert_eq!(renamed.id, id);
	assert_eq!(renamed.name, "idn_rename_f");
	assert!(
		catalog.find_identity_by_name(&mut Transaction::Admin(&mut txn), "idn_rename_f").unwrap().is_some(),
		"a self-rename must leave the identity findable"
	);
}

#[test]
fn rename_of_a_builtin_identity_is_rejected() {
	// root/system/anonymous carry their kind in the id itself; renaming one
	// would produce a row whose name no longer matches the sentinel it serves.
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = catalog.rename_identity(&mut txn, IdentityId::root(), "not_root").unwrap_err();
	assert_eq!(err.diagnostic().code, "CA_095");
}

#[test]
fn rename_of_an_unknown_identity_is_rejected() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let unknown = IdentityId::generate(t.clock(), t.rng());

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = catalog.rename_identity(&mut txn, unknown, "idn_rename_ghost").unwrap_err();
	assert_eq!(err.diagnostic().code, "CA_043");
}
