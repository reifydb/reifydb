// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::key::operator::state::KeyspaceId;

use super::*;
use crate::operator::state::mock::MockStore;

const DOOMED: GroupId = GroupId(7);
const BYSTANDER: GroupId = GroupId(8);

fn key(group: GroupId, keyspace: KeyspaceId, suffix: u8) -> GroupStateKey {
	let mut bytes = vec![0u8; 16];
	bytes[15] = suffix;
	OperatorStateKey::inner_encoded(group, keyspace, bytes)
}

fn seed(store: &mut MockStore, key: &GroupStateKey) {
	store.state_set(key, EncodedPodRow::new(&[0u8])).unwrap();
}

fn present(store: &mut MockStore, key: &GroupStateKey) -> bool {
	store.state_get(key).unwrap().is_some()
}

#[test]
fn reaps_the_data_phase_and_spares_the_identity_phase_of_the_same_group() {
	let mut store = MockStore::default();
	let accumulator = key(DOOMED, KeyspaceId::ACCUMULATOR, 1);
	let mapping = key(DOOMED, KeyspaceId::GUEST_ROW_MAPPING, 1);
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
	let doomed = key(DOOMED, KeyspaceId::ACCUMULATOR, 1);
	let bystander = key(BYSTANDER, KeyspaceId::ACCUMULATOR, 1);
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
	let doomed = key(DOOMED, KeyspaceId::ACCUMULATOR, 1);
	let index = key(GroupId::ROOT, KeyspaceId::ROLLING_EXPIRY, 1);
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

	let mut got = queued(&mut store, 256).unwrap().groups;
	got.sort_by_key(|g| g.0);

	assert_eq!(got, vec![DOOMED, BYSTANDER], "both queued groups must decode back to the ids that were enqueued");
}

#[test]
fn draining_frees_a_queued_group_and_clears_its_queue_entry() {
	let mut store = MockStore::default();
	let accumulator = key(DOOMED, KeyspaceId::ACCUMULATOR, 1);
	seed(&mut store, &accumulator);
	enqueue(&mut store, DOOMED).unwrap();

	let freed = drain(&mut store, &mut StoreReaper, 256).unwrap().freed;

	assert_eq!(freed, 1);
	assert!(!present(&mut store, &accumulator), "the queued group's data must be gone");
	assert!(queued(&mut store, 256).unwrap().groups.is_empty(), "a fully drained group must leave the queue");
}

#[test]
fn a_group_that_hits_the_budget_stays_queued_for_the_next_tick() {
	let mut store = MockStore::default();
	for i in 0..5 {
		seed(&mut store, &key(DOOMED, KeyspaceId::ACCUMULATOR, i));
	}
	enqueue(&mut store, DOOMED).unwrap();

	let freed = drain(&mut store, &mut StoreReaper, 2).unwrap().freed;

	assert_eq!(freed, 2, "the drain stops at the budget");
	assert_eq!(queued(&mut store, 256).unwrap().groups, vec![DOOMED], "a partly reaped group must stay queued");

	let rest = drain(&mut store, &mut StoreReaper, 256).unwrap().freed;

	assert_eq!(rest, 3, "the next tick takes what the budget deferred");
	assert!(queued(&mut store, 256).unwrap().groups.is_empty(), "the group leaves the queue once it is drained");
}

#[test]
fn draining_frees_the_identity_phase_once_the_data_phase_is_gone() {
	let mut store = MockStore::default();
	let accumulator = key(DOOMED, KeyspaceId::ACCUMULATOR, 1);
	let mapping = key(DOOMED, KeyspaceId::GUEST_ROW_MAPPING, 1);
	seed(&mut store, &accumulator);
	seed(&mut store, &mapping);
	enqueue(&mut store, DOOMED).unwrap();

	let freed = drain(&mut store, &mut StoreReaper, 256).unwrap().freed;

	assert_eq!(freed, 2, "both phases spend from the same budget");
	assert!(!present(&mut store, &accumulator), "the data phase goes first");
	assert!(!present(&mut store, &mapping), "and the identity phase must follow it in the same drain");
	assert!(queued(&mut store, 256).unwrap().groups.is_empty(), "a group drained of both phases leaves the queue");
}

#[test]
fn a_budget_spent_on_the_data_phase_defers_identity_to_the_next_tick() {
	let mut store = MockStore::default();
	let mapping = key(DOOMED, KeyspaceId::GUEST_ROW_MAPPING, 1);
	for i in 0..2 {
		seed(&mut store, &key(DOOMED, KeyspaceId::ACCUMULATOR, i));
	}
	seed(&mut store, &mapping);
	enqueue(&mut store, DOOMED).unwrap();

	let freed = drain(&mut store, &mut StoreReaper, 2).unwrap().freed;

	assert_eq!(freed, 2, "the budget is spent entirely on data");
	assert!(present(&mut store, &mapping), "identity must survive a tick that could not finish the data");
	assert_eq!(queued(&mut store, 256).unwrap().groups, vec![DOOMED], "so the group stays queued");

	drain(&mut store, &mut StoreReaper, 256).unwrap();

	assert!(!present(&mut store, &mapping), "the next tick finds no data left and takes the identity");
}

#[test]
fn stops_at_the_budget_and_reports_only_what_it_freed() {
	let mut store = MockStore::default();
	let keys: Vec<GroupStateKey> = (0..5).map(|i| key(DOOMED, KeyspaceId::ACCUMULATOR, i)).collect();
	for k in &keys {
		seed(&mut store, k);
	}

	let freed = reap_group(&mut store, DOOMED, &mut StoreReaper, 2).unwrap();

	assert_eq!(freed, 2, "the reap stops at the budget");
	let survivors = keys.iter().filter(|k| present(&mut store, k)).count();
	assert_eq!(survivors, 3, "the keys past the budget are left for the next tick");
}

#[test]
fn reaping_takes_both_ends_of_the_data_range_and_spares_both_ends_of_the_identity_range() {
	let mut store = MockStore::default();
	let lowest_data = key(DOOMED, KeyspaceId(0x00), 1);
	let highest_data = key(DOOMED, KeyspaceId(KeyspaceId::HIGHEST_DATA), 1);
	let lowest_identity = key(DOOMED, KeyspaceId::TIMER_INDEX, 1);
	let highest_identity = key(DOOMED, KeyspaceId::GUEST_ROW_MAPPING, 1);
	for k in [&lowest_data, &highest_data, &lowest_identity, &highest_identity] {
		seed(&mut store, k);
	}

	let freed = reap_group(&mut store, DOOMED, &mut StoreReaper, 256).unwrap();

	assert_eq!(freed, 2, "exactly the two data-phase keys are freed");
	assert!(!present(&mut store, &lowest_data), "keyspace 0x00 is data and must go");
	assert!(!present(&mut store, &highest_data), "the highest data keyspace must go with it");
	assert!(present(&mut store, &lowest_identity), "the identity keyspace nearest the boundary must survive");
	assert!(present(&mut store, &highest_identity), "so must the one furthest from it");
}

#[test]
fn a_group_larger_than_the_budget_still_drains_the_queue_to_empty() {
	let mut store = MockStore::default();
	let keys: Vec<GroupStateKey> = (0..9).map(|i| key(DOOMED, KeyspaceId::ACCUMULATOR, i)).collect();
	for k in &keys {
		seed(&mut store, k);
	}
	seed(&mut store, &key(DOOMED, KeyspaceId::GUEST_ROW_MAPPING, 1));
	enqueue(&mut store, DOOMED).unwrap();

	let mut rounds = 0;
	loop {
		let outcome = drain(&mut store, &mut StoreReaper, 2).unwrap();
		rounds += 1;
		assert!(rounds <= 32, "the drain must converge, not spin on a group it cannot shrink");
		if outcome.queue_is_empty() {
			break;
		}
	}

	assert!(keys.iter().all(|k| !present(&mut store, k)), "every data key must be gone");
	assert!(queued(&mut store, 256).unwrap().groups.is_empty(), "and the queue must be empty");
}

#[test]
fn the_reap_scan_never_fetches_an_identity_key() {
	let mut store = MockStore::default();
	for i in 0..3 {
		seed(&mut store, &key(DOOMED, KeyspaceId::ACCUMULATOR, i));
	}
	for i in 0..2 {
		seed(&mut store, &key(DOOMED, KeyspaceId::GUEST_ROW_MAPPING, i));
		seed(&mut store, &key(DOOMED, KeyspaceId::TIMER_INDEX, i));
	}
	let before = store.rows_visited();

	let freed = reap_group(&mut store, DOOMED, &mut StoreReaper, 256).unwrap();

	assert_eq!(freed, 3, "only the three data keys are reapable");
	assert_eq!(
		store.rows_visited() - before,
		3,
		"the scan must fetch the three data keys and none of the four identity keys"
	);
}

#[test]
fn the_reap_scan_stops_fetching_at_the_budget() {
	let mut store = MockStore::default();
	for i in 0..12 {
		seed(&mut store, &key(DOOMED, KeyspaceId::ACCUMULATOR, i));
	}
	let before = store.rows_visited();

	let freed = reap_group(&mut store, DOOMED, &mut StoreReaper, 3).unwrap();

	assert_eq!(freed, 3, "the reap stops at the budget");
	assert_eq!(
		store.rows_visited() - before,
		3,
		"and the scan behind it stops there too, rather than fetching all twelve"
	);
}

#[test]
fn one_merged_scan_reaps_data_and_reclaims_identity_and_dequeues_the_group() {
	let mut store = MockStore::default();
	let accumulator = key(DOOMED, KeyspaceId::ACCUMULATOR, 1);
	let mapping = key(DOOMED, KeyspaceId::GUEST_ROW_MAPPING, 1);
	seed(&mut store, &accumulator);
	seed(&mut store, &mapping);
	seed(&mut store, &queue_key(DOOMED));

	let outcome = drain_group(&mut store, DOOMED, &mut StoreReaper, 256).unwrap();

	assert!(!outcome.still_queued, "a fully drained group must not stay queued");
	assert!(!present(&mut store, &accumulator), "the data key must be reaped");
	assert!(!present(&mut store, &mapping), "the identity key must be reclaimed in the same pass");
	assert!(!present(&mut store, &queue_key(DOOMED)), "the queue entry must be removed");
}

#[test]
fn the_merged_scan_partitions_by_keyspace_not_by_scan_order() {
	let mut store = MockStore::default();
	let lowest_data = key(DOOMED, KeyspaceId(0x00), 1);
	let highest_data = key(DOOMED, KeyspaceId(KeyspaceId::HIGHEST_DATA), 1);
	let lowest_identity = key(DOOMED, KeyspaceId::TIMER_INDEX, 1);
	let highest_identity = key(DOOMED, KeyspaceId::GUEST_ROW_MAPPING, 1);
	for k in [&lowest_data, &highest_data, &lowest_identity, &highest_identity] {
		seed(&mut store, k);
	}
	seed(&mut store, &queue_key(DOOMED));

	let outcome = drain_group(&mut store, DOOMED, &mut StoreReaper, 256).unwrap();

	assert_eq!(outcome.freed, 4, "both data keys and both identity keys are accounted as freed");
	for k in [&lowest_data, &highest_data, &lowest_identity, &highest_identity] {
		assert!(!present(&mut store, k), "every key of a fully drained group must be gone");
	}
}

#[test]
fn the_merged_scan_leaves_a_neighbouring_group_untouched() {
	let mut store = MockStore::default();
	let doomed_data = key(DOOMED, KeyspaceId::ACCUMULATOR, 1);
	let neighbour_data = key(BYSTANDER, KeyspaceId::ACCUMULATOR, 1);
	let neighbour_identity = key(BYSTANDER, KeyspaceId::GUEST_ROW_MAPPING, 1);
	for k in [&doomed_data, &neighbour_data, &neighbour_identity] {
		seed(&mut store, k);
	}
	seed(&mut store, &queue_key(DOOMED));

	drain_group(&mut store, DOOMED, &mut StoreReaper, 256).unwrap();

	assert!(!present(&mut store, &doomed_data), "the doomed group's data must go");
	assert!(present(&mut store, &neighbour_data), "the neighbour's data must survive");
	assert!(present(&mut store, &neighbour_identity), "so must the neighbour's identity");
}

#[test]
fn a_group_too_large_for_the_budget_falls_back_and_keeps_its_identity() {
	let mut store = MockStore::default();
	let data: Vec<GroupStateKey> = (0..5).map(|i| key(DOOMED, KeyspaceId::ACCUMULATOR, i)).collect();
	for k in &data {
		seed(&mut store, k);
	}
	let mapping = key(DOOMED, KeyspaceId::GUEST_ROW_MAPPING, 1);
	seed(&mut store, &mapping);
	seed(&mut store, &queue_key(DOOMED));

	let outcome = drain_group(&mut store, DOOMED, &mut StoreReaper, 2).unwrap();

	assert!(outcome.still_queued, "a group that did not fit the budget must stay queued");
	assert!(present(&mut store, &mapping), "identity must survive while data is still pending");
	assert!(present(&mut store, &queue_key(DOOMED)), "the queue entry must survive too");
	let survivors = data.iter().filter(|k| present(&mut store, k)).count();
	assert_eq!(survivors, 3, "the budget bounds how much data one pass reaps");
}

#[test]
fn a_group_whose_identity_alone_exceeds_the_budget_still_makes_progress() {
	let mut store = MockStore::default();
	let identity: Vec<GroupStateKey> = (0..5).map(|i| key(DOOMED, KeyspaceId::GUEST_ROW_MAPPING, i)).collect();
	for k in &identity {
		seed(&mut store, k);
	}
	let data = key(DOOMED, KeyspaceId::ACCUMULATOR, 1);
	seed(&mut store, &data);
	seed(&mut store, &queue_key(DOOMED));

	let outcome = drain_group(&mut store, DOOMED, &mut StoreReaper, 2).unwrap();

	assert!(!present(&mut store, &data), "the fall-back reaps data first even when identity crowds the scan");
	assert!(outcome.freed > 0, "a pass that frees nothing would spin on this group forever");
}

#[derive(Default)]
struct RecordingReaper {
	seen: Vec<GroupStateKey>,
}

impl Reaper for RecordingReaper {
	fn reap(&mut self, store: &mut dyn StateStore, key: &GroupStateKey) -> Result<()> {
		self.seen.push(key.clone());
		store.state_remove(key)
	}
}

#[test]
fn the_reaper_is_handed_the_data_keys_and_never_an_identity_key() {
	let mut store = MockStore::default();
	let data = key(DOOMED, KeyspaceId::ACCUMULATOR, 1);
	let mapping = key(DOOMED, KeyspaceId::GUEST_ROW_MAPPING, 1);
	seed(&mut store, &data);
	seed(&mut store, &mapping);
	seed(&mut store, &queue_key(DOOMED));
	let mut reaper = RecordingReaper::default();

	drain_group(&mut store, DOOMED, &mut reaper, 256).unwrap();

	assert_eq!(reaper.seen, vec![data], "the reaper must receive the data key and nothing else");
}

#[test]
fn a_drainable_group_is_covered_by_a_single_scan_that_spans_both_phases() {
	let mut store = MockStore::default();
	seed(&mut store, &key(DOOMED, KeyspaceId::ACCUMULATOR, 1));
	seed(&mut store, &key(DOOMED, KeyspaceId::GUEST_ROW_MAPPING, 1));
	seed(&mut store, &queue_key(DOOMED));
	let before = store.rows_visited();

	drain_group(&mut store, DOOMED, &mut StoreReaper, 256).unwrap();

	assert_eq!(
		store.rows_visited() - before,
		2,
		"one scan must see the data row and the identity row together; a data-only scan sees one"
	);
}
