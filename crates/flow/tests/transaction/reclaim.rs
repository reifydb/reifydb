// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::{operator::state::OperatorState, pod::EncodedPodRow},
};
use reifydb_core::{
	actors::pending::PendingLayers,
	common::CommitVersion,
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey, group_inner_range},
};
use reifydb_flow::transaction::{
	ChangeCoordinate, DeferredParams, FlowTransaction, deferred::DeferredTransaction, group::GroupExtension,
	reclaim::ReclaimExtension, state::StateExtension, substrate::FlowSubstrate,
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{
	count::Count,
	value::{datetime::DateTime, identity::IdentityId},
};

const NODE: OperatorId = OperatorId(1);

fn payload() -> EncodedPodRow {
	1u64.encode_state().unwrap()
}

fn deferred(engine: &TestEngine) -> DeferredTransaction {
	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();
	let mut txn = DeferredTransaction::new(DeferredParams {
		version,
		pending: PendingLayers::empty(),
		query: parent.multi.begin_query().unwrap(),
		state_query: parent.multi.begin_query().unwrap(),
		catalog: Catalog::testing(),
		interceptors: Interceptors::new(),
		clock: Clock::Mock(MockClock::from_millis(0)),
		substrate: FlowSubstrate::with_dictionary(
			engine.inner().dictionary_allocators(),
			engine.inner().operator_state(),
		),
	});
	// The substrate derives an intern's position from the change coordinate, so it is set here.
	txn.set_change_coordinate(ChangeCoordinate {
		at: Some(DateTime::from_millis(0)),
		version: CommitVersion(0),
	});
	txn
}

fn seed_identity(txn: &mut DeferredTransaction, id: GroupId) {
	write(txn, id, Keyspace::GROUP_RECORD, 1);
	write(txn, id, Keyspace::ROW_NUMBER_MAPPING, 1);
}

fn write(txn: &mut DeferredTransaction, group: GroupId, keyspace: Keyspace, suffix: u8) {
	let key = OperatorStateKey::inner_encoded(group, keyspace, vec![suffix]);
	txn.state_set(NODE, &key, payload()).unwrap();
}

fn count(txn: &mut DeferredTransaction, range: EncodedKeyRange) -> usize {
	txn.state_range(NODE, range, None, "test").unwrap().items.len()
}

#[test]
fn the_identity_reclaim_erases_the_range_and_stops_the_group_resolving() {
	// The append operator erases a removed source row's identity inline, so a surviving
	// dictionary entry would let the next event on the same key resolve to a group whose
	// record and mapping are gone, stranding it forever.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let group_bytes = EncodedKey::new(b"a-group");
	let (id, _) = txn.intern_groups(NODE, &[group_bytes.clone()]).unwrap().remove(0);
	seed_identity(&mut txn, id);

	let outcome = txn.reclaim_group_identity(NODE, id, 100).unwrap();

	assert_eq!(outcome.removed, Count::new(3), "the substrate record, the seeded record row and the mapping");
	assert!(!outcome.more);
	assert_eq!(count(&mut txn, group_inner_range(id)), 0, "the group's range must be empty");
	assert_eq!(
		txn.lookup_groups(NODE, &[group_bytes]).unwrap().remove(0),
		None,
		"the dictionary entry must go with the identity"
	);
}

#[test]
fn a_bounded_identity_reclaim_keeps_the_dictionary_until_the_range_is_drained() {
	// The append remove path runs with a small fixed limit on the apply hot path. Dropping the
	// dictionary entry while identity rows remain would strand them: nothing could resolve the
	// group to finish the erase.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let group_bytes = EncodedKey::new(b"chunky");
	let (id, _) = txn.intern_groups(NODE, &[group_bytes.clone()]).unwrap().remove(0);
	for suffix in 0..4u8 {
		write(&mut txn, id, Keyspace::ROW_NUMBER_MAPPING, suffix);
	}

	let partial = txn.reclaim_group_identity(NODE, id, 2).unwrap();
	assert_eq!(partial.removed, Count::new(2));
	assert!(partial.more, "the caller must learn the group is not drained");
	assert_eq!(
		txn.lookup_groups(NODE, &[group_bytes.clone()]).unwrap().remove(0),
		Some(id),
		"a half-drained group must still resolve so a later pass can finish it"
	);

	let rest = txn.reclaim_group_identity(NODE, id, 100).unwrap();
	assert!(!rest.more);
	assert_eq!(txn.lookup_groups(NODE, &[group_bytes]).unwrap().remove(0), None);
}

#[test]
fn reclaiming_one_groups_identity_leaves_its_neighbour_untouched() {
	// An off-by-one in the identity range bounds silently destroys a live group's mapping. The
	// neighbour is the adjacent id precisely because that is where a bad upper bound would bleed.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let (id, _) = txn.intern_groups(NODE, &[EncodedKey::new(b"doomed")]).unwrap().remove(0);
	let (neighbour, _) = txn.intern_groups(NODE, &[EncodedKey::new(b"alive")]).unwrap().remove(0);
	seed_identity(&mut txn, id);
	seed_identity(&mut txn, neighbour);

	txn.reclaim_group_identity(NODE, id, 100).unwrap();

	assert_eq!(count(&mut txn, group_inner_range(id)), 0);
	assert_eq!(count(&mut txn, group_inner_range(neighbour)), 3, "the neighbour must be whole");
	assert_eq!(txn.lookup_groups(NODE, &[EncodedKey::new(b"alive")]).unwrap().remove(0), Some(neighbour));
}

#[test]
fn a_reclaimed_group_reborn_mints_a_fresh_id() {
	// A reclaimed id handed back out would collide the reborn key's state with any stale rows
	// still addressed by the old id.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let bytes = EncodedKey::new(b"reborn");
	let (id, _) = txn.intern_groups(NODE, &[bytes.clone()]).unwrap().remove(0);
	seed_identity(&mut txn, id);
	txn.reclaim_group_identity(NODE, id, 100).unwrap();

	let (reborn, is_new) = txn.intern_groups(NODE, &[bytes]).unwrap().remove(0);

	assert!(is_new, "the key is unknown again, so it must mint afresh");
	assert_ne!(reborn, id, "a reclaimed id must never be handed back out");
}
