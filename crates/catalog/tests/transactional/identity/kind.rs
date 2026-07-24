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

// RQL only knows CREATE USER and CREATE SERVICE, so a guest can only be built
// through the catalog directly - which is also the only way uptime makes one.
fn guest_id(t: &TestEngine, name: &str) -> IdentityId {
	let catalog = t.catalog();
	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let id = catalog.create_identity(&mut txn, name, IdentityKind::Guest, t.clock(), t.rng()).unwrap().id;
	txn.commit().unwrap();
	id
}

#[test]
fn uncommitted_promotion_is_visible_within_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let id = guest_id(&t, "idn_kind_a");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.promote_guest_to_user(&mut txn, id).unwrap();

	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn), id).unwrap().unwrap();
	assert_eq!(found.kind, IdentityKind::User);
}

#[test]
fn rolled_back_promotion_keeps_the_guest_kind() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let id = guest_id(&t, "idn_kind_b");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.promote_guest_to_user(&mut txn, id).unwrap();
	txn.rollback().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn2), id).unwrap().unwrap();
	assert_eq!(found.kind, IdentityKind::Guest, "a rolled-back promotion must not persist");
}

#[test]
fn committed_promotion_is_visible_in_a_new_txn() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let id = guest_id(&t, "idn_kind_c");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.promote_guest_to_user(&mut txn, id).unwrap();
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn2), id).unwrap().unwrap();
	assert_eq!(found.kind, IdentityKind::User);
	assert_eq!(found.name, "idn_kind_c", "a promotion must not disturb the name");
}

#[test]
fn promotion_is_reflected_by_the_system_identities_vtable() {
	// `$identity.kind` in policy predicates and the system::identities column
	// both read the stored kind. A guest promoted to a full user must stop
	// matching anything written for guests the moment the change commits.
	let t = TestEngine::new();
	let catalog = t.catalog();
	let id = guest_id(&t, "idn_kind_d");

	let frames = t.query("from system::identities filter { name == 'idn_kind_d' } map { kind }");
	assert!(frames[0].to_string().contains("guest"), "vtable must report the guest kind: {}", frames[0]);

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.promote_guest_to_user(&mut txn, id).unwrap();
	txn.commit().unwrap();

	let frames = t.query("from system::identities filter { name == 'idn_kind_d' } map { kind }");
	assert!(frames[0].to_string().contains("user"), "vtable must report the promoted kind: {}", frames[0]);
}

#[test]
fn a_user_cannot_be_promoted() {
	// Guest -> User is the only kind transition the catalog exposes, so `guest`
	// is unreachable for an identity that is already a user. This is the other
	// half of that invariant: promotion must not double as a way to re-run the
	// guest claim flow. If it returned Ok, a replayed guest token would rename a
	// real account and attach a fresh password credential to it.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE USER idn_kind_user");
	let id = identity_id(&t, "idn_kind_user");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = catalog.promote_guest_to_user(&mut txn, id).unwrap_err();
	assert_eq!(err.diagnostic().code, "CA_095");

	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn), id).unwrap().unwrap();
	assert_eq!(found.kind, IdentityKind::User, "a rejected promotion must not have touched the row");
}

#[test]
fn a_service_cannot_be_promoted() {
	// Probe identities are services. Promotion must not launder one into a user
	// that can then hold a password credential.
	let t = TestEngine::new();
	let catalog = t.catalog();
	t.admin("CREATE SERVICE idn_kind_probe");
	let id = identity_id(&t, "idn_kind_probe");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = catalog.promote_guest_to_user(&mut txn, id).unwrap_err();
	assert_eq!(err.diagnostic().code, "CA_095");

	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn), id).unwrap().unwrap();
	assert_eq!(found.kind, IdentityKind::Service, "a probe identity must keep its service kind");
}

#[test]
fn a_guest_cannot_be_promoted_twice() {
	// The second promotion is exactly what replaying a stale guest token does.
	// It has to fail the guest check rather than quietly succeed as a no-op.
	let t = TestEngine::new();
	let catalog = t.catalog();
	let id = guest_id(&t, "idn_kind_once");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.promote_guest_to_user(&mut txn, id).unwrap();
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let err = catalog.promote_guest_to_user(&mut txn2, id).unwrap_err();
	assert_eq!(err.diagnostic().code, "CA_095");
}

#[test]
fn renaming_a_guest_keeps_it_a_guest() {
	// uptime renames the identity before promoting it, in one txn. If the rename
	// disturbed the kind, the promotion behind it would fail its guest check and
	// no account could ever be claimed.
	let t = TestEngine::new();
	let catalog = t.catalog();
	let id = guest_id(&t, "idn_kind_guest_rename");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let renamed = catalog.rename_identity(&mut txn, id, "claimed@example.com").unwrap();
	assert_eq!(renamed.kind, IdentityKind::Guest, "a rename must not promote by itself");

	catalog.promote_guest_to_user(&mut txn, id).unwrap();
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn2), id).unwrap().unwrap();
	assert_eq!(found.kind, IdentityKind::User);
}

#[test]
fn renaming_a_user_cannot_move_it_back_to_guest() {
	// rename_identity rebuilds the row with `..pre.clone()`. If that ever became
	// a partial construction, a rename could reset the kind field - guest and
	// user differ by one byte in the stored row, and there is no API to undo it.
	let t = TestEngine::new();
	let catalog = t.catalog();
	let id = guest_id(&t, "idn_kind_rename");

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	catalog.promote_guest_to_user(&mut txn, id).unwrap();
	catalog.rename_identity(&mut txn, id, "renamed@example.com").unwrap();
	txn.commit().unwrap();

	let mut txn2 = t.begin_admin(IdentityId::system()).unwrap();
	let found = catalog.find_identity(&mut Transaction::Admin(&mut txn2), id).unwrap().unwrap();
	assert_eq!(found.kind, IdentityKind::User, "a rename must never move an identity back to guest");
	assert_eq!(found.name, "renamed@example.com");
}

#[test]
fn promotion_of_a_builtin_identity_is_rejected() {
	let t = TestEngine::new();
	let catalog = t.catalog();

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	for id in [IdentityId::root(), IdentityId::system(), IdentityId::anonymous()] {
		let err = catalog.promote_guest_to_user(&mut txn, id).unwrap_err();
		assert_eq!(err.diagnostic().code, "CA_095", "identity {id} must be rejected");
	}
}

#[test]
fn promotion_of_an_unknown_identity_is_rejected() {
	let t = TestEngine::new();
	let catalog = t.catalog();
	let unknown = IdentityId::generate(t.clock(), t.rng());

	let mut txn = t.begin_admin(IdentityId::system()).unwrap();
	let err = catalog.promote_guest_to_user(&mut txn, unknown).unwrap_err();
	assert_eq!(err.diagnostic().code, "CA_043");
}
