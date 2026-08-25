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
fn disable_clears_enabled_and_preserves_the_rest_of_the_row() {
	// Disabling rebuilds the whole row, so it must flip exactly one field and leave the rest untouched.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE SERVICE idn_enabled_a");
	let id = identity_id(&t, "idn_enabled_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let disabled = catalog.disable_identity(&mut txn, id).unwrap();
	txn.commit().unwrap();

	assert!(!disabled.enabled);
	assert_eq!(disabled.id, id);
	assert_eq!(disabled.name, "idn_enabled_a");
	assert_eq!(disabled.kind, IdentityKind::Service);

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn2), id).unwrap().unwrap();
	assert!(!found.enabled, "a committed disable must be visible in a fresh txn");
	assert_eq!(found.name, "idn_enabled_a");
	assert_eq!(found.kind, IdentityKind::Service);
}

#[test]
fn enable_restores_a_disabled_identity() {
	// Disabling is a lockout, not a deletion, so the account must come back without a new id.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER idn_enabled_b");
	let id = identity_id(&t, "idn_enabled_b");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.disable_identity(&mut txn, id).unwrap();
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let enabled = catalog.enable_identity(&mut txn2, id).unwrap();
	txn2.commit().unwrap();

	assert!(enabled.enabled);
	assert_eq!(enabled.id, id);

	let mut txn3 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn3), id).unwrap().unwrap();
	assert!(found.enabled, "a committed enable must be visible in a fresh txn");
}

#[test]
fn rolled_back_disable_leaves_the_identity_enabled() {
	// A failed multi-step admin operation must not lock an account out on its way to rolling back.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER idn_enabled_c");
	let id = identity_id(&t, "idn_enabled_c");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.disable_identity(&mut txn, id).unwrap();
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn2), id).unwrap().unwrap();
	assert!(found.enabled, "a rolled-back disable must not lock the account out");
}

#[test]
fn disable_of_a_builtin_identity_is_rejected() {
	// Disabling root or system would lock the database out of its own administration with no path back in.
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = catalog.disable_identity(&mut txn, IdentityId::root()).unwrap_err();
	assert_eq!(err.diagnostic().code, "CA_095");
}

#[test]
fn enable_of_a_builtin_identity_is_rejected() {
	// Enable carries its own guard call, so a missing one here would be invisible to the disable test.
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = catalog.enable_identity(&mut txn, IdentityId::root()).unwrap_err();
	assert_eq!(err.diagnostic().code, "CA_095");
}

#[test]
fn disable_of_an_unknown_identity_is_rejected() {
	// Writing the row blind would materialise a disabled identity that never existed.
	let t = TestEngine::new();
	let catalog = t.catalog();
	let unknown = IdentityId::generate(t.clock(), t.rng());

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = catalog.disable_identity(&mut txn, unknown).unwrap_err();
	assert_eq!(err.diagnostic().code, "CA_043");
}
