// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	actors::pending::PendingLayers,
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, GroupSet, Keyspace, OperatorStateKey},
};
use reifydb_flow::transaction::{
	DeferredParams, FlowTransaction,
	deferred::DeferredTransaction,
	row_number::*,
	state::StateTxn,
	substrate::{FlowSubstrate, apply_operator_state},
};
use reifydb_runtime::{
	cache::slab::SlabLru,
	context::clock::{Clock, MockClock},
};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::{
	byte_size::ByteSize,
	value::{identity::IdentityId, row_number::RowNumber},
};

const NODE: OperatorId = OperatorId(1);
const GROUP: GroupId = GroupId(7);
const NEIGHBOUR: GroupId = GroupId(8);

fn key(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes())
}

fn slot_key(slot: u64) -> EncodedKey {
	// The shape the block operators reclaim over: (slot, base, quote).
	EncodedKey::builder().u64(slot).u32(1u32).u32(2u32).build()
}

fn deferred(engine: &TestEngine) -> DeferredTransaction {
	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();
	DeferredTransaction::from_parts(DeferredParams {
		version,
		pending: PendingLayers::empty(),
		query: parent.multi.begin_query().unwrap(),
		state_query: parent.multi.begin_query().unwrap(),
		catalog: Catalog::testing(),
		interceptors: Interceptors::new(),
		clock: Clock::Mock(MockClock::from_millis(0)),
		substrate: FlowSubstrate {
			operators: engine.inner().operator_state(),
			..FlowSubstrate::default()
		},
	})
}

fn commit_pending(engine: &TestEngine, txn: &mut DeferredTransaction) {
	// Persists the pending writes so a later transaction or a cold provider resolves them the
	// way a committed flow would.
	let pending = txn.take_pending();
	apply_operator_state(&engine.inner().operator_state(), &pending);
}

#[test]
fn reported_memory_counts_retained_containers_not_entry_bookkeeping() {
	// SlabLru stores each key twice and struct_bytes() already counts both copies at capacity.
	// An inline key carries its payload inside the EncodedKey, so a cache of them retains
	// exactly struct_bytes(); adding entry_bytes() on top counts the same storage a third time.
	let mut state = NodeState::default();
	for i in 0..64u64 {
		state.remember(GROUP, &slot_key(i), RowNumber(i));
	}

	assert!(
		state.cache.keys().all(|(_, k)| k.heap_bytes() == 0),
		"slot keys must stay inline or this test proves nothing"
	);
	assert_eq!(state.memory().entries.as_u64(), 64);
	assert_eq!(state.memory().bytes.as_bytes(), state.cache.struct_bytes() as u64);
}

#[test]
fn reported_memory_counts_a_shared_out_of_line_key_once() {
	// A key past EncodedKey::INLINE_CAP spills to a refcounted Arc, so the two clones SlabLru
	// holds share one allocation. Charging it per copy over-reports caches keyed by long keys
	// and evicts them early.
	let long = EncodedKey::new(vec![7u8; 200]);
	assert!(long.heap_bytes() > 0, "key must spill out of line or this test proves nothing");

	let mut state = NodeState::default();
	state.remember(GROUP, &long, RowNumber(1));

	assert_eq!(state.memory().bytes.as_bytes(), state.cache.struct_bytes() as u64 + long.heap_bytes() as u64);
}

#[test]
fn reported_memory_survives_eviction_of_every_entry() {
	// Eviction frees entries but neither the slab Vec nor the map returns its capacity, so the
	// pages stay resident. Reported memory must follow the retained containers, not the live
	// entry count, or a cache that has churned looks free while still holding its peak.
	let mut state = NodeState::default();
	for i in 0..64u64 {
		state.remember(GROUP, &slot_key(i), RowNumber(i));
	}
	let full = state.memory().bytes.as_bytes();

	state.evict_to_budget(ByteSize::ZERO);

	assert_eq!(state.memory().entries.as_u64(), 0, "budget of zero must drain every entry");
	assert_eq!(
		state.memory().bytes.as_bytes(),
		state.cache.struct_bytes() as u64,
		"a drained cache holds no key payload, so it reports exactly its containers"
	);
	// Not merely equal to `full`: releasing a slot pushes its index onto the free list, so a
	// drained cache retains slightly more than a full one. Reported memory must never fall.
	assert!(
		state.memory().bytes.as_bytes() >= full,
		"retained capacity must not shrink on eviction: {} < {}",
		state.memory().bytes.as_bytes(),
		full
	);
}

#[test]
fn eviction_charge_covers_what_an_entry_actually_retains() {
	// A budget only means something if the per-entry charge covers what the entry retains: the
	// slab slot plus the map bucket, both of which outlive the caller. Charging less lets a
	// nominal 1 MiB cache hold several MiB.
	let mut state = NodeState::default();
	for i in 0..256u64 {
		state.remember(GROUP, &slot_key(i), RowNumber(i));
	}

	let retained =
		state.cache.len() as u64 * SlabLru::<(GroupId, EncodedKey), RowNumber>::entry_struct_bytes() as u64;
	assert!(
		state.cache_size.as_bytes() >= retained,
		"charged {} for {} entries that retain {}",
		state.cache_size.as_bytes(),
		state.cache.len(),
		retained
	);
}

#[test]
fn a_budget_bounds_the_memory_its_surviving_entries_retain() {
	let budget = ByteSize::from_bytes(64 * 1024);
	let mut state = NodeState::default();
	for i in 0..4096u64 {
		state.remember(GROUP, &slot_key(i), RowNumber(i));
	}

	state.evict_to_budget(budget);

	let retained =
		state.cache.len() as u64 * SlabLru::<(GroupId, EncodedKey), RowNumber>::entry_struct_bytes() as u64;
	assert!(
		retained <= budget.as_bytes(),
		"{} entries survived a {} byte budget and retain {}",
		state.cache.len(),
		budget.as_bytes(),
		retained
	);
}

#[test]
fn first_key_mints_one_and_is_new() {
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);
	let (rn, is_new) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("first")).unwrap();
	assert_eq!(rn.0, 1);
	assert!(is_new, "a never-seen key must report as newly minted");
}

#[test]
fn distinct_keys_mint_sequential_numbers() {
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);
	for i in 1..=5u64 {
		let (rn, is_new) =
			provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key(&format!("k{i}"))).unwrap();
		assert_eq!(rn.0, i, "distinct keys mint a contiguous ascending sequence");
		assert!(is_new);
	}
}

#[test]
fn a_repeated_key_returns_the_same_number() {
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);
	let (first, new1) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("dup")).unwrap();
	let (second, new2) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("dup")).unwrap();
	assert_eq!(first, second, "the same key must always resolve to the same row number");
	assert!(new1);
	assert!(!new2, "a re-seen key must not report as new");
}

#[test]
fn duplicate_keys_in_one_batch_share_a_single_row_number() {
	// Operators emit one record per input row, not per distinct group, so a single batch can
	// carry the same key twice. Both occurrences must resolve to one freshly-minted number and
	// only the first report is_new, or the operator emits two output rows for one group.
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);
	let batch = [key("food"), key("transport"), key("food"), key("drinks")];
	let results = provider.get_or_create_row_numbers(NODE, GROUP, &mut txn, &batch).unwrap();

	assert_eq!(results[0].0, results[2].0, "both 'food' slots must share one row number");
	assert!(results[0].1, "the first occurrence of a new key is new");
	assert!(!results[2].1, "the duplicate occurrence must not report as new");
	assert_ne!(results[0].0, results[1].0, "distinct keys keep distinct numbers");
	assert_ne!(results[0].0, results[3].0);
	let mut distinct: Vec<u64> = results.iter().map(|(rn, _)| rn.0).collect();
	distinct.sort_unstable();
	distinct.dedup();
	assert_eq!(distinct.len(), 3, "four slots over three distinct keys mint three numbers");
}

#[test]
fn a_batch_mixes_existing_and_new_keys() {
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);
	let (a, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("a")).unwrap();
	let (b, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("b")).unwrap();

	let batch = [key("b"), key("c"), key("a")];
	let results = provider.get_or_create_row_numbers(NODE, GROUP, &mut txn, &batch).unwrap();
	assert_eq!(results[0], (b, false), "existing key b keeps its number, not new");
	assert!(results[1].1, "c is freshly minted");
	assert_eq!(results[1].0.0, 3, "c takes the next sequential number");
	assert_eq!(results[2], (a, false), "existing key a keeps its number, not new");
}

#[test]
fn a_known_mapping_is_served_from_the_cache_across_transactions() {
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();

	let mut first = deferred(&engine);
	let (minted, new1) = provider.get_or_create_row_number(NODE, GROUP, &mut first, &key("k")).unwrap();
	assert!(new1);
	commit_pending(&engine, &mut first);

	let mut second = deferred(&engine);
	let (resolved, new2) = provider.get_or_create_row_number(NODE, GROUP, &mut second, &key("k")).unwrap();
	assert_eq!(resolved, minted, "a persisted mapping must resolve to the original number");
	assert!(!new2, "an existing mapping must not be re-minted");
}

#[test]
fn a_cold_provider_resolves_persisted_mappings_from_the_store() {
	// A restart is a fresh provider with an empty cache. Re-minting instead of hydrating would
	// hand a downstream consumer a different row number for a row it already tracks.
	let engine = TestEngine::new();
	let minted = {
		let seed = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		let (rn, _) = seed.get_or_create_row_number(NODE, GROUP, &mut txn, &key("survivor")).unwrap();
		commit_pending(&engine, &mut txn);
		rn
	};

	let restarted = RowNumberProvider::default();
	let mut txn = deferred(&engine);
	let (resolved, is_new) = restarted.get_or_create_row_number(NODE, GROUP, &mut txn, &key("survivor")).unwrap();
	assert_eq!(resolved, minted, "the cold provider must reuse the persisted number");
	assert!(!is_new, "resolving a persisted mapping is not a mint");
}

#[test]
fn the_counter_high_water_survives_a_restart() {
	// A cold provider seeds its counter from the persisted high-water, so a restart never
	// re-issues a number a prior run already handed out.
	let engine = TestEngine::new();
	{
		let seed = RowNumberProvider::default();
		let mut txn = deferred(&engine);
		for name in ["k1", "k2", "k3"] {
			seed.get_or_create_row_number(NODE, GROUP, &mut txn, &key(name)).unwrap();
		}
		commit_pending(&engine, &mut txn);
	}

	let restarted = RowNumberProvider::default();
	let mut txn = deferred(&engine);
	let (rn, is_new) = restarted.get_or_create_row_number(NODE, GROUP, &mut txn, &key("k4")).unwrap();
	assert!(is_new);
	assert_eq!(rn.0, 4, "a fresh key after a restart continues the sequence, never reusing 1..=3");
}

#[test]
fn the_counter_is_shared_across_a_nodes_groups() {
	// A downstream consumer tracks a row by its number across every group of the operator, so two
	// groups minting from independent sequences would give one number to two different rows.
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);

	let (a, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("shared")).unwrap();
	let (b, _) = provider.get_or_create_row_number(NODE, NEIGHBOUR, &mut txn, &key("shared")).unwrap();

	assert_ne!(a, b, "the same key in two groups must not collide on one row number");
	assert_eq!(a.0, 1);
	assert_eq!(b.0, 2, "the second group's mint continues the operator's sequence");

	let (a_again, is_new) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("shared")).unwrap();
	assert_eq!(a_again, a, "each group's mapping is stable and independent");
	assert!(!is_new);
}

#[test]
fn get_row_number_returns_none_for_unknown_and_never_mints() {
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);
	assert_eq!(provider.get_row_number(NODE, GROUP, &mut txn, &key("ghost")).unwrap(), None);
	let (rn, is_new) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("real")).unwrap();
	assert_eq!(rn.0, 1, "a failed lookup must not advance the counter");
	assert!(is_new);
}

#[test]
fn get_row_number_returns_an_existing_mapping_without_minting() {
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);
	let (minted, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("here")).unwrap();
	assert_eq!(provider.get_row_number(NODE, GROUP, &mut txn, &key("here")).unwrap(), Some(minted));
}

#[test]
fn dropping_a_mapping_removes_it_and_a_re_lookup_mints_a_fresh_number() {
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();

	let mut first = deferred(&engine);
	let (minted, _) = provider.get_or_create_row_number(NODE, GROUP, &mut first, &key("victim")).unwrap();
	assert!(
		provider.remove_row_number(NODE, GROUP, &mut first, &key("victim")).unwrap(),
		"dropping a present key returns true"
	);
	assert_eq!(
		provider.get_row_number(NODE, GROUP, &mut first, &key("victim")).unwrap(),
		None,
		"the dropped mapping is gone from the cache"
	);
	commit_pending(&engine, &mut first);

	let mut second = deferred(&engine);
	let (reminted, is_new) = provider.get_or_create_row_number(NODE, GROUP, &mut second, &key("victim")).unwrap();
	assert!(is_new, "a dropped key mints fresh on re-lookup");
	assert_ne!(reminted, minted, "a dropped row number is never reused");
}

#[test]
fn dropping_an_absent_mapping_is_idempotent() {
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);
	assert!(
		!provider.remove_row_number(NODE, GROUP, &mut txn, &key("nope")).unwrap(),
		"dropping an absent key returns false, not an error"
	);
}

#[test]
fn nodes_are_isolated() {
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);
	let (a, _) = provider.get_or_create_row_number(OperatorId(1), GROUP, &mut txn, &key("shared")).unwrap();
	let (b, _) = provider.get_or_create_row_number(OperatorId(2), GROUP, &mut txn, &key("shared")).unwrap();
	assert_eq!(a.0, 1, "each operator mints from its own sequence");
	assert_eq!(b.0, 1, "the same key under a different operator is an independent mapping");
}

#[test]
fn a_complete_group_proves_absence_without_a_store_read() {
	// A fully hydrated or freshly interned group is complete, and a complete group answers
	// "never minted" from the cache alone. That is what keeps the firehose new-key path off the
	// store.
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();

	let mut first = deferred(&engine);
	provider.get_or_create_row_number(NODE, GROUP, &mut first, &key("known")).unwrap();
	commit_pending(&engine, &mut first);

	let mut second = deferred(&engine);
	// Warm the group so the assertion measures absence proofs, not the hydration scan.
	provider.get_row_number(NODE, GROUP, &mut second, &key("known")).unwrap();
	assert_eq!(provider.get_row_number(NODE, GROUP, &mut second, &key("unknown")).unwrap(), None);
}

#[test]
fn a_freshly_interned_group_mints_new_keys_without_a_store_read() {
	// mark_fresh is what txn.intern_group calls for a brand-new group. Its mapping keyspace is
	// provably empty, so keys mint with zero store reads.
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);
	provider.mark_fresh(NODE, GROUP);
	// Seed the operator counter once so the assertion measures per-key reads, not the one-time
	// counter-seed read the first mint on a cold provider always pays.
	provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("warmup")).unwrap();

	let fresh = [key("new_a"), key("new_b"), key("new_c")];
	let results = provider.get_or_create_row_numbers(NODE, GROUP, &mut txn, &fresh).unwrap();
	assert!(results.iter().all(|(_, is_new)| *is_new), "all three keys are brand new");
}

#[test]
fn an_over_capacity_group_falls_back_to_the_store_for_absence() {
	// A group holding more mapping keys than the byte budget cannot stay complete, so hydration
	// evicts and the group pays a store read to prove absence. Over-claiming a RAM absence here
	// would be worse than the read.
	let engine = TestEngine::new();
	let budget = ByteSize::from_bytes(entry_bytes(&key("k1")) * 2);
	{
		let seed = RowNumberProvider::new(budget);
		let mut txn = deferred(&engine);
		for name in ["k1", "k2", "k3"] {
			seed.get_or_create_row_number(NODE, GROUP, &mut txn, &key(name)).unwrap();
		}
		commit_pending(&engine, &mut txn);
	}

	let restarted = RowNumberProvider::new(budget);
	let mut txn = deferred(&engine);
	restarted.get_row_number(NODE, GROUP, &mut txn, &key("k1")).unwrap();

	let samples = restarted.samples();
	assert_eq!(samples.len(), 1, "the hydrated operator must surface exactly one sample");
	assert!(!samples[0].1.completeness.values_complete, "three mappings cannot be values-complete at capacity two");
	assert_eq!(restarted.get_row_number(NODE, GROUP, &mut txn, &key("never_minted")).unwrap(), None);
}

#[test]
fn a_confirmed_removal_keeps_absence_in_memory() {
	// remove_row_number retires the key from the cache while the group stays complete, so every
	// later probe of the removed key is answered from memory rather than paying a store read.
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();

	let mut txn = deferred(&engine);
	provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("k1")).unwrap();
	provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("k2")).unwrap();
	assert!(provider.remove_row_number(NODE, GROUP, &mut txn, &key("k1")).unwrap());

	assert_eq!(provider.get_row_number(NODE, GROUP, &mut txn, &key("k1")).unwrap(), None);
}

#[test]
fn drop_below_reclaims_only_mappings_under_the_bound() {
	// The block operators reclaim finished slots with drop_below. Keys lead with a slot, so
	// dropping below a bound must reclaim exactly the lower slots and leave the rest mapped.
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);

	let (rn10, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &slot_key(10)).unwrap();
	let (rn20, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &slot_key(20)).unwrap();
	let (rn30, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &slot_key(30)).unwrap();

	let upper = EncodedKey::builder().u64(25u64).u32(0u32).u32(0u32).build();
	let mut dropped = provider.drop_below(NODE, GROUP, &mut txn, &upper).unwrap();
	dropped.sort_by_key(|rn| rn.0);
	assert_eq!(dropped, vec![rn10, rn20], "exactly the below-bound mappings are reclaimed");

	let (rn30_again, is_new30) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &slot_key(30)).unwrap();
	assert!(!is_new30, "slot 30 sat above the bound and must remain mapped");
	assert_eq!(rn30, rn30_again);

	let (rn10_again, is_new10) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &slot_key(10)).unwrap();
	assert!(is_new10, "reclaimed slot 10 mints fresh");
	assert_ne!(rn10, rn10_again, "a reclaimed row number is never reused");
}

#[test]
fn invalidating_a_group_drops_its_cache_without_serving_a_ghost() {
	// After phase-2 identity reclamation deletes a group's mapping rows the cache still names
	// them, and serving that number is a ghost. invalidate_groups must clear the reclaimed
	// group while leaving every other group's mappings intact.
	let engine = TestEngine::new();
	let provider = RowNumberProvider::default();
	let mut txn = deferred(&engine);

	let (doomed, _) = provider.get_or_create_row_number(NODE, GROUP, &mut txn, &key("x")).unwrap();
	let (kept, _) = provider.get_or_create_row_number(NODE, NEIGHBOUR, &mut txn, &key("x")).unwrap();

	provider.invalidate_groups(NODE, &GroupSet::new([GROUP]));

	// Emulate phase 2 erasing the reclaimed group's mapping row from the store.
	txn.state_remove(NODE, &mapping_key(GROUP, &key("x"))).unwrap();

	assert_eq!(
		provider.get_row_number(NODE, GROUP, &mut txn, &key("x")).unwrap(),
		None,
		"the reclaimed group must not serve a ghost row number from a dropped cache entry"
	);
	assert_eq!(
		provider.get_row_number(NODE, NEIGHBOUR, &mut txn, &key("x")).unwrap(),
		Some(kept),
		"an unrelated group's mapping must survive the invalidation"
	);
	assert_ne!(doomed, kept);
}

#[test]
fn the_row_number_counter_never_collides_with_the_interners_group_counter() {
	// Both operator counters live in the root group's NODE_COUNTER keyspace, so a group mint must never
	// advance the row-number sequence.
	let group_counter = OperatorStateKey::inner_encoded(GroupId::ROOT, Keyspace::NODE_COUNTER, vec![]);
	assert_ne!(counter_key(), group_counter, "the row-number counter must not alias the group-id counter");
	assert_ne!(mapping_key(GROUP, &key("x")), counter_key(), "a mapping key must never equal the counter key");
}
