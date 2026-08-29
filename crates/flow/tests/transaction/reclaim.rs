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
	key::operator_state::{GroupId, KeyspaceId, OperatorStateKey, group_inner_range},
};
use reifydb_flow::transaction::{
	ChangeCoordinate, DeferredParams, FlowTransaction,
	deferred::DeferredTransaction,
	reclaim::ReclaimExtension,
	state::{StateExtension, StateRange},
	substrate::FlowSubstrate,
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
		query: Some(parent.multi.begin_query().unwrap()),
		state_query: Some(parent.multi.begin_query().unwrap()),
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
	write(txn, id, KeyspaceId::ROW_NUMBER_MAPPING, 1);
	write(txn, id, KeyspaceId::ROW_NUMBER_MAPPING, 2);
}

fn write(txn: &mut DeferredTransaction, group: GroupId, keyspace: KeyspaceId, suffix: u8) {
	let key = OperatorStateKey::inner_encoded(group, keyspace, vec![suffix]);
	txn.state_set(NODE, &key, payload()).unwrap();
}

fn count(txn: &mut DeferredTransaction, range: EncodedKeyRange) -> usize {
	txn.state_range(NODE, StateRange::forward(range, "test")).unwrap().items.len()
}

#[test]
fn the_identity_reclaim_erases_the_whole_range() {
	// the append operator erases a removed source row's identity inline, so a surviving mapping row would strand it
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let id = GroupId::of(&EncodedKey::new(b"a-group"));
	seed_identity(&mut txn, id);

	let outcome = txn.reclaim_group_identity(NODE, id, 100).unwrap();

	assert_eq!(outcome.removed, Count::new(2), "both seeded mapping rows");
	assert!(!outcome.more);
	assert_eq!(count(&mut txn, group_inner_range(id)), 0, "the group's range must be empty");
}

#[test]
fn a_bounded_identity_reclaim_reports_that_rows_remain() {
	// the append remove path runs with a small fixed limit, so a caller told "drained" too early strands the rest
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let id = GroupId::of(&EncodedKey::new(b"chunky"));
	for suffix in 0..4u8 {
		write(&mut txn, id, KeyspaceId::ROW_NUMBER_MAPPING, suffix);
	}

	let partial = txn.reclaim_group_identity(NODE, id, 2).unwrap();
	assert_eq!(partial.removed, Count::new(2));
	assert!(partial.more, "the caller must learn the group is not drained");

	let rest = txn.reclaim_group_identity(NODE, id, 100).unwrap();
	assert_eq!(rest.removed, Count::new(2));
	assert!(!rest.more);
	assert_eq!(count(&mut txn, group_inner_range(id)), 0);
}

#[test]
fn a_budget_stopping_between_keyspaces_resumes_where_it_left_off() {
	// mappings sort before timers, so a pass that ends mid-range must not skip the tail on the next pass
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let id = GroupId::of(&EncodedKey::new(b"outlives-its-budget"));
	write(&mut txn, id, KeyspaceId::ROW_NUMBER_MAPPING, 1);
	write(&mut txn, id, KeyspaceId::ROW_NUMBER_MAPPING, 2);
	write(&mut txn, id, KeyspaceId::TIMER_WHEEL, 1);
	write(&mut txn, id, KeyspaceId::TIMER_WHEEL, 2);

	let partial = txn.reclaim_group_identity(NODE, id, 3).unwrap();

	assert_eq!(partial.removed, Count::new(3));
	assert!(partial.more, "the fourth row must still be pending");

	let rest = txn.reclaim_group_identity(NODE, id, 100).unwrap();

	assert_eq!(rest.removed, Count::new(1), "exactly the row the budget could not reach");
	assert!(!rest.more);
	assert_eq!(count(&mut txn, group_inner_range(id)), 0, "and nothing may be left under the id");
}

#[test]
fn reclaiming_one_groups_identity_leaves_its_neighbour_untouched() {
	// the ids are deliberately adjacent because that is exactly where a bad upper bound bleeds across
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let id = GroupId(0x5EED_0000_0000_0000_0000_0000_0000_0001);
	let neighbour = GroupId(id.0 + 1);
	seed_identity(&mut txn, id);
	seed_identity(&mut txn, neighbour);

	txn.reclaim_group_identity(NODE, id, 100).unwrap();

	assert_eq!(count(&mut txn, group_inner_range(id)), 0);
	assert_eq!(count(&mut txn, group_inner_range(neighbour)), 2, "the neighbour must be whole");
}

#[test]
fn a_reclaimed_group_reborn_reuses_its_id() {
	// the id is a function of the key alone, so a reborn key must land back in the scope it was erased from
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let bytes = EncodedKey::new(b"reborn");
	let id = GroupId::of(&bytes);
	seed_identity(&mut txn, id);
	txn.reclaim_group_identity(NODE, id, 100).unwrap();

	assert_eq!(GroupId::of(&bytes), id);
	assert_eq!(count(&mut txn, group_inner_range(id)), 0, "and the reborn scope must start empty");
}
