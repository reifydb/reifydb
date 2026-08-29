// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_catalog::catalog::Catalog;
use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	actors::pending::PendingLayers,
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, KeyspaceId, OperatorStateKey},
};
use reifydb_flow::transaction::{
	DeferredParams, FlowTransaction,
	deferred::DeferredTransaction,
	row_number::*,
	substrate::{FlowSubstrate, apply_operator_state},
};
use reifydb_runtime::context::clock::{Clock, MockClock};
use reifydb_test_harness::engine::TestEngine;
use reifydb_transaction::interceptor::interceptors::Interceptors;
use reifydb_value::value::identity::IdentityId;

const NODE: OperatorId = OperatorId(1);
const GROUP: GroupId = GroupId(7);
const NEIGHBOUR: GroupId = GroupId(8);

fn key(s: &str) -> EncodedKey {
	EncodedKey::new(s.as_bytes())
}

fn deferred(engine: &TestEngine) -> DeferredTransaction {
	let parent = engine.begin_admin(IdentityId::system()).unwrap();
	let version = parent.version();
	DeferredTransaction::new(DeferredParams {
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
	})
}

fn commit_pending(engine: &TestEngine, txn: &mut DeferredTransaction) {
	// Persists pending writes so a later transaction resolves them the way a committed flow would.
	let pending = txn.take_pending();
	apply_operator_state(&engine.inner().operator_state(), &pending);
}

#[test]
fn first_key_mints_one_and_is_new() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let (rn, is_new) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("first")]).unwrap().remove(0);
	assert_eq!(rn.0, 1);
	assert!(is_new, "a never-seen key must report as newly minted");
}

#[test]
fn distinct_keys_mint_sequential_numbers() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	for i in 1..=5u64 {
		let (rn, is_new) =
			txn.get_or_create_row_numbers(NODE, GROUP, &[key(&format!("k{i}"))]).unwrap().remove(0);
		assert_eq!(rn.0, i, "distinct keys mint a contiguous ascending sequence");
		assert!(is_new);
	}
}

#[test]
fn a_repeated_key_returns_the_same_number() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let (first, new1) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("dup")]).unwrap().remove(0);
	let (second, new2) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("dup")]).unwrap().remove(0);
	assert_eq!(first, second, "the same key must always resolve to the same row number");
	assert!(new1);
	assert!(!new2, "a re-seen key must not report as new");
}

#[test]
fn duplicate_keys_in_one_batch_share_a_single_row_number() {
	// One record per input row, so a batch carrying a key twice must not emit two output rows.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let batch = [key("food"), key("transport"), key("food"), key("drinks")];
	let results = txn.get_or_create_row_numbers(NODE, GROUP, &batch).unwrap();

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
	let mut txn = deferred(&engine);
	let (a, _) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("a")]).unwrap().remove(0);
	let (b, _) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("b")]).unwrap().remove(0);

	let batch = [key("b"), key("c"), key("a")];
	let results = txn.get_or_create_row_numbers(NODE, GROUP, &batch).unwrap();
	assert_eq!(results[0], (b, false), "existing key b keeps its number, not new");
	assert!(results[1].1, "c is freshly minted");
	assert_eq!(results[1].0.0, 3, "c takes the next sequential number");
	assert_eq!(results[2], (a, false), "existing key a keeps its number, not new");
}

#[test]
fn a_known_mapping_resolves_across_transactions() {
	let engine = TestEngine::new();

	let mut first = deferred(&engine);
	let (minted, new1) = first.get_or_create_row_numbers(NODE, GROUP, &[key("k")]).unwrap().remove(0);
	assert!(new1);
	commit_pending(&engine, &mut first);

	let mut second = deferred(&engine);
	let (resolved, new2) = second.get_or_create_row_numbers(NODE, GROUP, &[key("k")]).unwrap().remove(0);
	assert_eq!(resolved, minted, "a persisted mapping must resolve to the original number");
	assert!(!new2, "an existing mapping must not be re-minted");
}

#[test]
fn persisted_mappings_resolve_after_a_restart() {
	// Re-minting would hand a downstream consumer a different number for a row it already tracks.
	let engine = TestEngine::new();
	let minted = {
		let mut txn = deferred(&engine);
		let (rn, _) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("survivor")]).unwrap().remove(0);
		commit_pending(&engine, &mut txn);
		rn
	};

	let mut txn = deferred(&engine);
	let (resolved, is_new) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("survivor")]).unwrap().remove(0);
	assert_eq!(resolved, minted, "a later transaction must reuse the persisted number");
	assert!(!is_new, "resolving a persisted mapping is not a mint");
}

#[test]
fn the_counter_high_water_survives_a_restart() {
	// The counter is read back from the store, so a restart never re-issues a handed-out number.
	let engine = TestEngine::new();
	{
		let mut txn = deferred(&engine);
		for name in ["k1", "k2", "k3"] {
			txn.get_or_create_row_numbers(NODE, GROUP, &[key(name)]).unwrap().remove(0);
		}
		commit_pending(&engine, &mut txn);
	}

	let mut txn = deferred(&engine);
	let (rn, is_new) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("k4")]).unwrap().remove(0);
	assert!(is_new);
	assert_eq!(rn.0, 4, "a fresh key after a restart continues the sequence, never reusing 1..=3");
}

#[test]
fn the_counter_is_shared_across_a_nodes_groups() {
	// A consumer tracks a row by number across every group, so per-group sequences would collide.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	let (a, _) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("shared")]).unwrap().remove(0);
	let (b, _) = txn.get_or_create_row_numbers(NODE, NEIGHBOUR, &[key("shared")]).unwrap().remove(0);

	assert_ne!(a, b, "the same key in two groups must not collide on one row number");
	assert_eq!(a.0, 1);
	assert_eq!(b.0, 2, "the second group's mint continues the operator's sequence");

	let (a_again, is_new) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("shared")]).unwrap().remove(0);
	assert_eq!(a_again, a, "each group's mapping is stable and independent");
	assert!(!is_new);
}

#[test]
fn get_row_number_returns_none_for_unknown_and_never_mints() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	assert_eq!(txn.get_row_numbers(NODE, GROUP, &[key("ghost")]).unwrap().remove(0), None);
	let (rn, is_new) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("real")]).unwrap().remove(0);
	assert_eq!(rn.0, 1, "a failed lookup must not advance the counter");
	assert!(is_new);
}

#[test]
fn get_row_number_returns_an_existing_mapping_without_minting() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let (minted, _) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("here")]).unwrap().remove(0);
	assert_eq!(txn.get_row_numbers(NODE, GROUP, &[key("here")]).unwrap().remove(0), Some(minted));
}

#[test]
fn get_row_numbers_reports_a_hole_for_every_unmapped_key() {
	// The batch form must stay positional, or a caller zips numbers onto the wrong rows.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let (mapped, _) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("mapped")]).unwrap().remove(0);

	let results = txn.get_row_numbers(NODE, GROUP, &[key("ghost"), key("mapped"), key("other")]).unwrap();

	assert_eq!(results, vec![None, Some(mapped), None]);
}

#[test]
fn dropping_a_mapping_removes_it_and_a_re_lookup_mints_a_fresh_number() {
	let engine = TestEngine::new();

	let mut first = deferred(&engine);
	let (minted, _) = first.get_or_create_row_numbers(NODE, GROUP, &[key("victim")]).unwrap().remove(0);
	first.remove_row_number(NODE, GROUP, &key("victim")).unwrap();
	assert_eq!(
		first.get_row_numbers(NODE, GROUP, &[key("victim")]).unwrap().remove(0),
		None,
		"the dropped mapping is gone"
	);
	commit_pending(&engine, &mut first);

	let mut second = deferred(&engine);
	let (reminted, is_new) = second.get_or_create_row_numbers(NODE, GROUP, &[key("victim")]).unwrap().remove(0);
	assert!(is_new, "a dropped key mints fresh on re-lookup");
	assert_ne!(reminted, minted, "a dropped row number is never reused");
}

#[test]
fn dropping_an_absent_mapping_is_idempotent() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	txn.remove_row_number(NODE, GROUP, &key("nope")).unwrap();
	assert_eq!(
		txn.get_row_numbers(NODE, GROUP, &[key("nope")]).unwrap().remove(0),
		None,
		"dropping an absent key must neither error nor conjure a mapping"
	);
}

#[test]
fn dropping_a_batch_removes_the_present_keys_and_leaves_the_absent_ones_alone() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let (kept, _) = txn.get_or_create_row_numbers(NODE, GROUP, &[key("kept")]).unwrap().remove(0);
	txn.get_or_create_row_numbers(NODE, GROUP, &[key("doomed")]).unwrap();

	txn.remove_row_numbers(NODE, GROUP, &[key("doomed"), key("never_mapped")]).unwrap();

	assert_eq!(
		txn.get_row_numbers(NODE, GROUP, &[key("doomed"), key("never_mapped"), key("kept")]).unwrap(),
		vec![None, None, Some(kept)],
		"a batched drop must take exactly the keys it names and spare every other mapping"
	);
}

#[test]
fn nodes_are_isolated() {
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);
	let (a, _) = txn.get_or_create_row_numbers(OperatorId(1), GROUP, &[key("shared")]).unwrap().remove(0);
	let (b, _) = txn.get_or_create_row_numbers(OperatorId(2), GROUP, &[key("shared")]).unwrap().remove(0);
	assert_eq!(a.0, 1, "each operator mints from its own sequence");
	assert_eq!(b.0, 1, "the same key under a different operator is an independent mapping");
}

#[test]
fn remove_by_prefix_reclaims_every_mapping_under_the_prefix() {
	// Prefix removal must take the whole subtree and nothing that merely sorts beside it.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	let doomed = EncodedKey::builder().u64(1u64).u32(7u32).build();
	let kept = EncodedKey::builder().u64(2u64).u32(7u32).build();
	txn.get_or_create_row_numbers(NODE, GROUP, &[doomed.clone()]).unwrap().remove(0);
	let (kept_rn, _) = txn.get_or_create_row_numbers(NODE, GROUP, &[kept.clone()]).unwrap().remove(0);

	let prefix = EncodedKey::builder().u64(1u64).build();
	txn.remove_row_numbers_by_prefix(NODE, GROUP, prefix.as_slice()).unwrap();

	assert_eq!(
		txn.get_row_numbers(NODE, GROUP, &[doomed]).unwrap().remove(0),
		None,
		"the prefixed mapping is reclaimed"
	);
	assert_eq!(
		txn.get_row_numbers(NODE, GROUP, &[kept]).unwrap().remove(0),
		Some(kept_rn),
		"a sibling prefix survives"
	);
}

#[test]
fn the_row_number_counter_never_collides_with_the_interners_group_counter() {
	// Both counters live in the root group's NODE_COUNTER keyspace and must not alias.
	let group_counter = OperatorStateKey::inner_encoded(GroupId::ROOT, KeyspaceId::NODE_COUNTER, vec![]);
	assert_ne!(counter_key(), group_counter, "the row-number counter must not alias the group-id counter");
	assert_ne!(mapping_key(GROUP, &key("x")), counter_key(), "a mapping key must never equal the counter key");
}

#[test]
fn remove_by_prefix_reclaims_a_subtree_that_spans_more_than_one_scan_page() {
	// Same paging contract for prefix removal, and the page boundary must not become a second
	// stopping condition that spares part of the subtree while a sibling prefix stays untouched.
	let engine = TestEngine::new();
	let mut txn = deferred(&engine);

	let doomed: Vec<EncodedKey> =
		(0..1027u32).map(|slot| EncodedKey::builder().u64(1u64).u32(slot).build()).collect();
	let kept = EncodedKey::builder().u64(2u64).u32(7u32).build();
	txn.get_or_create_row_numbers(NODE, GROUP, &doomed).unwrap();
	let (kept_rn, _) = txn.get_or_create_row_numbers(NODE, GROUP, &[kept.clone()]).unwrap().remove(0);

	let prefix = EncodedKey::builder().u64(1u64).build();
	txn.remove_row_numbers_by_prefix(NODE, GROUP, prefix.as_slice()).unwrap();

	let gone = txn.get_row_numbers(NODE, GROUP, &doomed).unwrap();
	assert!(gone.iter().all(Option::is_none), "the whole prefixed subtree is reclaimed, not just one page of it");
	assert_eq!(
		txn.get_row_numbers(NODE, GROUP, &[kept]).unwrap().remove(0),
		Some(kept_rn),
		"a sibling prefix survives"
	);
}
