// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::operator::EncodedOperatorRow;
use reifydb_core::key::operator_state::Keyspace;
use reifydb_value::value::datetime::DateTime;

use super::*;
use crate::testing::store::MockStore;

const DOOMED: GroupId = GroupId(7);
const BYSTANDER: GroupId = GroupId(8);

fn key(group: GroupId, keyspace: Keyspace, suffix: u8) -> GroupStateKey {
	OperatorStateKey::inner_encoded(group, keyspace, vec![suffix])
}

fn seed(store: &mut MockStore, key: &GroupStateKey) {
	store.state_set(key, EncodedOperatorRow::new(&[0u8], DateTime::EPOCH)).unwrap();
}

fn present(store: &mut MockStore, key: &GroupStateKey) -> bool {
	store.state_get(key).unwrap().is_some()
}

#[test]
fn reaps_the_data_phase_and_spares_the_identity_phase_of_the_same_group() {
	let mut store = MockStore::default();
	let accumulator = key(DOOMED, Keyspace::ACCUMULATOR, 1);
	let mapping = key(DOOMED, Keyspace::ROW_NUMBER_MAPPING, 1);
	seed(&mut store, &accumulator);
	seed(&mut store, &mapping);

	let freed = reap_group(&mut store, DOOMED, &mut StoreReaper, 256).unwrap();

	assert_eq!(freed, 1, "only the data-phase key counts as freed");
	assert!(!present(&mut store, &accumulator), "the accumulator is data phase and must be gone");
	assert!(present(&mut store, &mapping), "the row-number mapping is identity phase and must survive");
}

#[test]
fn reaps_nothing_outside_the_named_group() {
	let mut store = MockStore::default();
	let doomed = key(DOOMED, Keyspace::ACCUMULATOR, 1);
	let bystander = key(BYSTANDER, Keyspace::ACCUMULATOR, 1);
	seed(&mut store, &doomed);
	seed(&mut store, &bystander);

	let freed = reap_group(&mut store, DOOMED, &mut StoreReaper, 256).unwrap();

	assert_eq!(freed, 1);
	assert!(!present(&mut store, &doomed));
	assert!(present(&mut store, &bystander), "a neighbouring group's state must outlive the reap");
}

#[test]
fn spares_the_root_group_so_the_expiry_index_drains_on_its_own() {
	let mut store = MockStore::default();
	let doomed = key(DOOMED, Keyspace::ACCUMULATOR, 1);
	let index = key(GroupId::ROOT, Keyspace::EXPIRY, 1);
	seed(&mut store, &doomed);
	seed(&mut store, &index);

	reap_group(&mut store, DOOMED, &mut StoreReaper, 256).unwrap();

	assert!(present(&mut store, &index), "the root-resident expiry index must survive a group reap");
}

#[test]
fn a_queued_group_round_trips_through_its_key() {
	let mut store = MockStore::default();
	enqueue(&mut store, DOOMED).unwrap();
	enqueue(&mut store, BYSTANDER).unwrap();

	let mut got = queued(&mut store, 256).unwrap();
	got.sort_by_key(|g| g.0);

	assert_eq!(got, vec![DOOMED, BYSTANDER], "both queued groups must decode back to the ids that were enqueued");
}

#[test]
fn draining_frees_a_queued_group_and_clears_its_queue_entry() {
	let mut store = MockStore::default();
	let accumulator = key(DOOMED, Keyspace::ACCUMULATOR, 1);
	seed(&mut store, &accumulator);
	enqueue(&mut store, DOOMED).unwrap();

	let freed = drain(&mut store, &mut StoreReaper, 256).unwrap();

	assert_eq!(freed, 1);
	assert!(!present(&mut store, &accumulator), "the queued group's data must be gone");
	assert!(queued(&mut store, 256).unwrap().is_empty(), "a fully drained group must leave the queue");
}

#[test]
fn a_group_that_hits_the_budget_stays_queued_for_the_next_tick() {
	let mut store = MockStore::default();
	for i in 0..5 {
		seed(&mut store, &key(DOOMED, Keyspace::ACCUMULATOR, i));
	}
	enqueue(&mut store, DOOMED).unwrap();

	let freed = drain(&mut store, &mut StoreReaper, 2).unwrap();

	assert_eq!(freed, 2, "the drain stops at the budget");
	assert_eq!(queued(&mut store, 256).unwrap(), vec![DOOMED], "a partly reaped group must stay queued");

	let rest = drain(&mut store, &mut StoreReaper, 256).unwrap();

	assert_eq!(rest, 3, "the next tick takes what the budget deferred");
	assert!(queued(&mut store, 256).unwrap().is_empty(), "the group leaves the queue once it is drained");
}

#[test]
fn draining_frees_the_identity_phase_once_the_data_phase_is_gone() {
	// A sealed group is unreachable, so identity left behind is dead weight nothing ever collects.
	let mut store = MockStore::default();
	let accumulator = key(DOOMED, Keyspace::ACCUMULATOR, 1);
	let mapping = key(DOOMED, Keyspace::ROW_NUMBER_MAPPING, 1);
	seed(&mut store, &accumulator);
	seed(&mut store, &mapping);
	enqueue(&mut store, DOOMED).unwrap();

	let freed = drain(&mut store, &mut StoreReaper, 256).unwrap();

	assert_eq!(freed, 2, "both phases spend from the same budget");
	assert!(!present(&mut store, &accumulator), "the data phase goes first");
	assert!(!present(&mut store, &mapping), "and the identity phase must follow it in the same drain");
	assert!(queued(&mut store, 256).unwrap().is_empty(), "a group drained of both phases leaves the queue");
}

#[test]
fn a_budget_spent_on_the_data_phase_defers_identity_to_the_next_tick() {
	// Reclaiming identity before the data is drained strands the rest: nothing resolves the group to finish it.
	let mut store = MockStore::default();
	let mapping = key(DOOMED, Keyspace::ROW_NUMBER_MAPPING, 1);
	for i in 0..2 {
		seed(&mut store, &key(DOOMED, Keyspace::ACCUMULATOR, i));
	}
	seed(&mut store, &mapping);
	enqueue(&mut store, DOOMED).unwrap();

	let freed = drain(&mut store, &mut StoreReaper, 2).unwrap();

	assert_eq!(freed, 2, "the budget is spent entirely on data");
	assert!(present(&mut store, &mapping), "identity must survive a tick that could not finish the data");
	assert_eq!(queued(&mut store, 256).unwrap(), vec![DOOMED], "so the group stays queued");

	drain(&mut store, &mut StoreReaper, 256).unwrap();

	assert!(!present(&mut store, &mapping), "the next tick finds no data left and takes the identity");
}

#[test]
fn stops_at_the_budget_and_reports_only_what_it_freed() {
	let mut store = MockStore::default();
	let keys: Vec<GroupStateKey> = (0..5).map(|i| key(DOOMED, Keyspace::ACCUMULATOR, i)).collect();
	for k in &keys {
		seed(&mut store, k);
	}

	let freed = reap_group(&mut store, DOOMED, &mut StoreReaper, 2).unwrap();

	assert_eq!(freed, 2, "the reap stops at the budget");
	let survivors = keys.iter().filter(|k| present(&mut store, k)).count();
	assert_eq!(survivors, 3, "the keys past the budget are left for the next tick");
}
