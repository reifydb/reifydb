// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Transparency of the operator range tier. Rows live in partitions, one per group and keyspace, but what a
//! partition may answer is decided by coverage: a set of intervals, each one a claim that the tier holds every
//! key the persistent tier holds strictly within it. Inside a claim the tier answers a range outright and
//! answers a missed point read as a definitive absence; outside one it declines. Anything that can leave a
//! claim short of a key sqlite holds must shrink that claim, since a short answer reads like a correct one.

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::{
		keyspace::KEYSPACES,
		state::{
			GroupId, GroupStateKey, KeyspaceId, group_data_inner_range, group_inner_range,
			keyspace_inner_range,
		},
	},
	metrics::scan::ScanCounters,
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	persistent::{PersistentTier, sqlite::SqlitePersistent},
	range::{OperatorRangeConfig, tiers::RangeTiers},
	store::OperatorStore,
	types::{LayeredPre, OperatorBatch, OperatorWrite, StagedWrite},
};
use reifydb_testing::keyspace::state_key;
use reifydb_value::{byte_size::ByteSize, util::hash::Hash128};

const CACHED_BELOW_UNCACHED: KeyspaceId = KeyspaceId::PARTITIONED_RINGBUFFER_META;

const RANGE_ONLY: KeyspaceId = KeyspaceId::JOIN_PIN;

const RANGE_ONLY_ABOVE: KeyspaceId = KeyspaceId::GUEST_RUNNING;

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);

fn group() -> GroupId {
	GroupId::hashed(Hash128(7))
}

fn cached_store() -> (OperatorStore, SqlitePersistent, SqliteTempPathGuard) {
	// The hour-long interval on a frozen clock means the only drain a test sees is the one it asked for.
	cached_store_with(OperatorRangeConfig::testing())
}

fn cached_store_with(range: OperatorRangeConfig) -> (OperatorStore, SqlitePersistent, SqliteTempPathGuard) {
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	let (storage, guard) = SqlitePersistent::in_memory();
	let store = OperatorStore::standard(OperatorStoreConfig {
		resident: Default::default(),
		persistent: Some(OperatorPersistentConfig::opened(PersistentTier::Sqlite(storage.clone()))),
		range: Some(range),
		spawner,
		clock,
	});
	(store, storage, guard)
}

fn key_in(keyspace: KeyspaceId, suffix: u8) -> EncodedKey {
	state_key(group(), keyspace, suffix as u64)
}

fn root_key_in(keyspace: KeyspaceId, suffix: u8) -> EncodedKey {
	// A keyspace with no group column holds every row at ROOT, so any other group names a key that cannot exist.
	state_key(GroupId::ROOT, keyspace, suffix as u64)
}

fn row(body: &str) -> EncodedPodRow {
	EncodedPodRow::new(body.as_bytes())
}

fn body(row: &EncodedPodRow) -> String {
	String::from_utf8(row.body().to_vec()).expect("test bodies are utf8")
}

fn bodies(batch: &OperatorBatch) -> Vec<String> {
	batch.items.iter().map(|(_, row)| body(row)).collect()
}

fn seeded_range() -> EncodedKeyRange {
	keyspace_inner_range(group(), RANGE_ONLY)
}

fn seed_rows(storage: &SqlitePersistent, count: u8) {
	for suffix in 1..=count {
		storage.seed_durable(&[OperatorWrite::Insert {
			operator: OP_A,
			key: GroupStateKey::bound_unchecked(key_in(RANGE_ONLY, suffix)),
			post: row(&format!("v{suffix}")),
		}]);
	}
}

fn range_tier(store: &OperatorStore) -> &RangeTiers {
	store.range().tiers().expect("the fixture configures a range tier")
}

fn range_partitions(store: &OperatorStore) -> usize {
	range_tier(store).partitions()
}

fn range_intervals(store: &OperatorStore) -> usize {
	range_tier(store).intervals()
}

fn put(store: &OperatorStore, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
	// reading the pre-image back keeps the claim truthful even when an earlier write in the same test moved the key
	let write = match store.state_get(operator, &GroupStateKey::bound_unchecked(key.clone())).unwrap() {
		Some(pre) => OperatorWrite::Replace {
			operator,
			key: GroupStateKey::bound_unchecked(key),
			pre_value_bytes: ByteSize::from_bytes(pre.bytes().len() as u64),
			post: row,
		},
		None => OperatorWrite::Insert {
			operator,
			key: GroupStateKey::bound_unchecked(key),
			post: row,
		},
	};
	store.apply_batch(&[write]);
}

#[test]
fn a_range_over_a_claimed_span_is_served_without_reaching_the_persistent_tier() {
	// A repeat scan that still fetches from sqlite charges for the rows while serving nothing.
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);

	let primed = store.range_batch(OP_A, seeded_range(), 64).unwrap();
	assert_eq!(bodies(&primed), ["v1", "v2", "v3"], "the priming scan must read every durable row");
	assert_eq!(
		range_partitions(&store),
		1,
		"a whole-keyspace scan that was not cut short must materialize the span, or nothing below is tested"
	);
	assert_eq!(range_intervals(&store), 1, "one uninterrupted scan must prove one claim, not a claim per row");

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, seeded_range(), 64).unwrap();
	let scanned = before.since();

	assert_eq!(bodies(&served), ["v1", "v2", "v3"], "a tier-served range must return the same rows sqlite would");
	assert_eq!(scanned.fetched, 0, "a claimed span that still scans sqlite saves nothing");
	let counters = range_tier(&store).metrics();
	assert_eq!(counters.hits, 1, "the second scan must be attributed as a range hit");
	assert_eq!(counters.materializes, 1, "only the first scan may materialize");
}

#[test]
fn a_range_over_a_keyspace_no_scan_proved_falls_through_and_still_answers_in_full() {
	// A key warmed by a point read proves nothing about its neighbours; a range from it drops rows.
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);

	assert!(
		store.state_get(OP_A, &GroupStateKey::bound_unchecked(key_in(RANGE_ONLY, 2))).unwrap().is_some(),
		"the point read warms one key"
	);
	assert_eq!(
		range_partitions(&store),
		0,
		"a point fill must never materialize a claim over keys it did not read"
	);

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, seeded_range(), 64).unwrap();
	let scanned = before.since();

	assert_eq!(
		bodies(&served),
		["v1", "v2", "v3"],
		"a tier with no claim must not shorten the answer to the one key a point read happened to cache"
	);
	assert!(scanned.fetched >= 3, "the answer must have come from sqlite, not from an unproven span");
	let counters = range_tier(&store).metrics();
	assert_eq!(counters.hits, 0, "an unclaimed keyspace may never be counted as a range hit");
	assert_eq!(counters.misses, 1);
}

#[test]
fn a_new_key_and_a_rewrite_together_leave_the_claim_whole_and_current() {
	// A claim short of a row sqlite holds answers "no such key", and only a scan sees the shortfall.
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);

	let primed = store.range_batch(OP_A, seeded_range(), 64).unwrap();
	assert_eq!(bodies(&primed), ["v1", "v2", "v3"]);
	assert_eq!(range_partitions(&store), 1, "the claim must start standing or the writes prove nothing");

	put(&store, OP_A, key_in(RANGE_ONLY, 4), row("v4"));
	put(&store, OP_A, key_in(RANGE_ONLY, 1), row("rewritten"));
	assert!(store.flush_pending_blocking(), "both writes must reach sqlite before the claim is put to the test");

	assert_eq!(
		range_partitions(&store),
		1,
		"neither a key the claim never held nor a rewrite of one it does may retract the claim"
	);
	assert_eq!(range_intervals(&store), 1, "a write inside a standing claim must not split it");

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, seeded_range(), 64).unwrap();
	let scanned = before.since();

	assert_eq!(
		bodies(&served),
		["rewritten", "v2", "v3", "v4"],
		"a kept claim must answer for the rewritten row and for the one it learned at flush, or it serves a \
		 short answer that reads as a correct one"
	);
	assert_eq!(scanned.fetched, 0, "the answer must have come from the claim, not from a fallback scan");
}

#[test]
fn a_range_spanning_two_keyspaces_bypasses_the_tier_and_reads_every_row_out_of_sqlite() {
	// A tier is one keyspace, so a range whose ends sit in different keyspaces names no tier to ask and
	// falls through to sqlite whole. The rows must still be exactly right and in order: a fallback that
	// answers correctly is a cost, but one that drops or reorders rows is a wrong answer. The single
	// keyspace scans above the fallback are the control, and they must still be cached, or this test
	// would also pass with the tier switched off entirely.
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);
	for suffix in 1..=2u8 {
		storage.seed_durable(&[OperatorWrite::Insert {
			operator: OP_A,
			key: GroupStateKey::bound_unchecked(key_in(RANGE_ONLY_ABOVE, suffix)),
			post: row(&format!("c{suffix}")),
		}]);
	}

	assert_eq!(bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()), ["v1", "v2", "v3"]);
	assert_eq!(
		bodies(&store.range_batch(OP_A, keyspace_inner_range(group(), RANGE_ONLY_ABOVE), 64).unwrap()),
		["c1", "c2"]
	);
	assert_eq!(range_partitions(&store), 2, "both keyspaces must be claimed or the span proves nothing");

	let spanning = EncodedKeyRange::new(
		keyspace_inner_range(group(), RANGE_ONLY_ABOVE).start,
		keyspace_inner_range(group(), RANGE_ONLY).end,
	);
	let counters = range_tier(&store).metrics();
	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, spanning, 64).unwrap();
	let scanned = before.since();

	assert_eq!(
		bodies(&served),
		["c1", "c2", "v1", "v2", "v3"],
		"the fallback must answer for both keyspaces in full and in key order"
	);
	assert_eq!(scanned.fetched, 5, "the fallback reads every row out of sqlite, claims or no claims");
	let after = range_tier(&store).metrics();
	assert_eq!(
		(after.hits, after.misses),
		(counters.hits, counters.misses),
		"a range that names no tier must not be charged to one either, or the counters attribute a read \
         to a keyspace that never saw it"
	);
	assert_eq!(
		range_partitions(&store),
		2,
		"and the standing claims must survive untouched, so the next single keyspace scan still hits them"
	);
}

#[test]
fn a_write_of_a_key_the_claim_never_held_keeps_the_claim_and_still_serves_it() {
	// A claim that serves a range without a key just written to it is a silent short answer.
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()), ["v1", "v2", "v3"]);
	assert_eq!(range_partitions(&store), 1, "the claim must start standing or the write proves nothing");

	put(&store, OP_A, key_in(RANGE_ONLY, 4), row("v4"));

	assert_eq!(
		range_partitions(&store),
		1,
		"a write of a key the claim never held leaves sqlite unchanged, so the claim is still true"
	);
	assert_eq!(range_intervals(&store), 1, "a key written inside a claim joins it rather than splitting it");
	assert_eq!(
		bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()),
		["v1", "v2", "v3", "v4"],
		"the shadowed write must still reach the answer through the resident state merge"
	);

	assert!(store.flush_pending_blocking(), "the write must reach sqlite before the claim is put to the test");
	assert_eq!(
		range_partitions(&store),
		1,
		"the flush finds the key already resident, so the claim survives it untouched"
	);

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, seeded_range(), 64).unwrap();
	let scanned = before.since();

	assert_eq!(
		bodies(&served),
		["v1", "v2", "v3", "v4"],
		"a claim kept across a write must still answer for the row the flush made durable, or it serves a \
		 short answer that reads as a correct one"
	);
	assert_eq!(scanned.fetched, 0, "the answer must have come from the claim, not from a fallback scan");
}

#[test]
fn a_write_of_a_key_the_claim_holds_updates_it_in_place() {
	// Retracting the whole claim would spend every other key in it to absorb one write.
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()), ["v1", "v2", "v3"]);
	assert_eq!(range_partitions(&store), 1, "the claim must start standing or the write proves nothing");

	put(&store, OP_A, key_in(RANGE_ONLY, 2), row("rewritten"));

	assert_eq!(
		range_partitions(&store),
		1,
		"a replace of a key the claim holds must move that one entry, not retract the whole claim"
	);
	assert_eq!(range_intervals(&store), 1, "and it must leave the claim in one piece");
	assert_eq!(
		bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()),
		["v1", "rewritten", "v3"],
		"and the answer must stay right whichever tier serves it"
	);

	assert!(store.flush_pending_blocking(), "the write must reach sqlite before the kept claim is put to the test");

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, seeded_range(), 64).unwrap();
	let scanned = before.since();

	assert_eq!(
		bodies(&served),
		["v1", "rewritten", "v3"],
		"the carried claim must still answer for the row the flush made durable"
	);
	assert_eq!(scanned.fetched, 0, "the answer must have come from the claim the replace left standing");
}

#[test]
fn a_removal_of_a_key_the_claim_holds_hides_that_key_and_keeps_the_claim() {
	// Erasing the key would let a scan rematerialize the row sqlite still holds until the flush.
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()), ["v1", "v2", "v3"]);
	assert_eq!(range_partitions(&store), 1, "the claim must start standing or the removal proves nothing");

	store.apply_batch(&[OperatorWrite::Remove {
		operator: OP_A,
		key: GroupStateKey::bound_unchecked(key_in(RANGE_ONLY, 2)),
		pre: LayeredPre::Present(ByteSize::from_bytes(row("v2").bytes().len() as u64)),
	}]);

	assert_eq!(
		range_partitions(&store),
		1,
		"a removal inside a claim must hide the one key, not retract the claim around it"
	);
	assert_eq!(range_intervals(&store), 1, "and it must not punch the claim into two");

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, seeded_range(), 64).unwrap();
	let scanned = before.since();

	assert_eq!(bodies(&served), ["v1", "v3"], "the removed row must not come back from either tier");
	assert_eq!(
		scanned.fetched, 0,
		"the surviving keys must still come from the claim, or a removal costs as much as a retraction"
	);
}

#[test]
fn a_flushed_removal_demotes_its_row_to_a_proven_absence_and_leaves_the_rest_standing() {
	// Only the flush may mark the key absent; erasing it or leaving a row both serve wrong data.
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()), ["v1", "v2", "v3"]);

	store.apply_batch(&[OperatorWrite::Remove {
		operator: OP_A,
		key: GroupStateKey::bound_unchecked(key_in(RANGE_ONLY, 2)),
		pre: LayeredPre::Present(ByteSize::from_bytes(row("v2").bytes().len() as u64)),
	}]);
	assert!(store.flush_pending_blocking(), "the tombstone must reach sqlite before the claim is put to the test");

	assert_eq!(range_partitions(&store), 1, "a flushed removal must take one entry, not the whole claim");
	assert_eq!(range_intervals(&store), 1, "and it must leave the claim in one piece");

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, seeded_range(), 64).unwrap();
	let scanned = before.since();

	assert_eq!(bodies(&served), ["v1", "v3"], "the kept claim must not serve the row the flush erased");
	assert_eq!(scanned.fetched, 0, "the answer must have come from the claim the flushed removal left standing");
	assert_eq!(
		store.state_get(OP_A, &GroupStateKey::bound_unchecked(key_in(RANGE_ONLY, 2))).unwrap(),
		None,
		"and the demoted key must answer a point read as a proven absence, not fall through to sqlite"
	);
}

#[test]
fn a_removal_of_a_key_the_claim_never_held_keeps_the_claim() {
	// Retracting on every erase of a key no claim held grinds the whole feature down to nothing.
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()), ["v1", "v2", "v3"]);
	assert_eq!(range_partitions(&store), 1, "the claim must start standing or the removal proves nothing");

	store.apply_batch(&[OperatorWrite::Remove {
		operator: OP_A,
		key: GroupStateKey::bound_unchecked(key_in(RANGE_ONLY, 9)),
		pre: LayeredPre::Absent,
	}]);
	assert!(store.flush_pending_blocking(), "the tombstone must reach sqlite through the same flush path");

	assert_eq!(
		range_partitions(&store),
		1,
		"erasing a key the bucket never held cannot make the bucket short of anything"
	);

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, seeded_range(), 64).unwrap();
	let scanned = before.since();

	assert_eq!(bodies(&served), ["v1", "v2", "v3"], "the surviving rows must still all be there");
	assert_eq!(scanned.fetched, 0, "the answer must have come from the bucket that kept its claim");
}

#[test]
fn a_written_row_too_big_for_the_range_budget_takes_the_whole_claim_with_it() {
	// A claim that cannot hold the key just written to it must be retracted, never left short.
	let (store, storage, _guard) = cached_store_with(OperatorRangeConfig {
		tier_bytes: Some(ByteSize::from_bytes(4096)),
		..OperatorRangeConfig::testing()
	});
	seed_rows(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()), ["v1", "v2", "v3"]);
	assert_eq!(range_partitions(&store), 1, "the small materialize must fit its budget or nothing below is tested");

	let huge = "x".repeat(8192);
	put(&store, OP_A, key_in(RANGE_ONLY, 4), row(&huge));
	assert_eq!(
		range_partitions(&store),
		0,
		"a row the range budget cannot hold must cost the claim, not be dropped from a claim that goes on \
		 answering for the whole span"
	);
	assert_eq!(range_intervals(&store), 0, "and no interval may outlive the rows it claimed");

	assert!(store.flush_pending_blocking(), "the write must reach sqlite through the flush path");

	assert_eq!(range_partitions(&store), 0, "the flush must not resurrect a claim over a row it cannot hold");

	let served = store.range_batch(OP_A, seeded_range(), 64).unwrap();
	assert_eq!(
		bodies(&served),
		["v1", "v2", "v3", huge.as_str()],
		"and the answer must carry the durable row the tier could not keep"
	);
}

#[test]
fn a_removal_the_flush_has_not_carried_survives_the_range_tier_dropping_its_tombstone() {
	// The range tier pins a removal only while nothing beneath it records one. The resident tier keeps an
	// unflushed removal until settle_flushing marks it clean, and every read consults the resident tier
	// first, so the range tier may drop its tombstone under pressure. Pinning it instead fills the tier
	// with entries no eviction can take and refuses every materialize until the next flush lands.
	let (store, storage, _guard) = cached_store_with(OperatorRangeConfig {
		tier_bytes: Some(ByteSize::from_bytes(4096)),
		..OperatorRangeConfig::testing()
	});
	seed_rows(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()), ["v1", "v2", "v3"]);

	store.apply_batch(&[OperatorWrite::Remove {
		operator: OP_A,
		key: GroupStateKey::bound_unchecked(key_in(RANGE_ONLY, 2)),
		pre: LayeredPre::Present(ByteSize::from_bytes(row("v2").bytes().len() as u64)),
	}]);

	let huge = "x".repeat(8192);
	put(&store, OP_A, key_in(RANGE_ONLY, 4), row(&huge));

	assert_eq!(
		range_partitions(&store),
		0,
		"the eviction must be able to take the unflushed removal with the rest, or the tier keeps a \
		 partition it can never free"
	);
	assert_eq!(
		store.state_get(OP_A, &GroupStateKey::bound_unchecked(key_in(RANGE_ONLY, 2))).unwrap(),
		None,
		"the resident tier still holds the unflushed removal and must answer the point read"
	);
	assert_eq!(
		bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()),
		["v1", "v3", huge.as_str()],
		"and it must shadow the row sqlite still holds out of the scan"
	);
}

#[test]
fn dropping_one_operators_state_forgets_every_claim_and_row_it_cached() {
	// A drop that clears only the point entries lets the range tier resurrect every row it erased.
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);
	for suffix in 1..=3u8 {
		storage.seed_durable(&[OperatorWrite::Insert {
			operator: OP_B,
			key: GroupStateKey::bound_unchecked(key_in(RANGE_ONLY, suffix)),
			post: row(&format!("b{suffix}")),
		}]);
	}

	assert_eq!(bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()), ["v1", "v2", "v3"]);
	assert_eq!(bodies(&store.range_batch(OP_B, seeded_range(), 64).unwrap()), ["b1", "b2", "b3"]);
	assert_eq!(range_partitions(&store), 2, "both operators must hold a claim or the scoping below proves nothing");

	store.drop_operator(OP_A).unwrap();

	assert_eq!(
		range_partitions(&store),
		1,
		"the drop must take the dropped operator's claims, not only its point entries"
	);

	let before = ScanCounters::sample();
	assert_eq!(bodies(&store.range_batch(OP_B, seeded_range(), 64).unwrap()), ["b1", "b2", "b3"]);
	assert_eq!(
		before.since().fetched,
		0,
		"dropping by operator must be scoped, or every teardown throws away every other operator's claims"
	);

	assert!(store.flush_pending_blocking(), "the drop must reach sqlite before the mask stops hiding the tiers");
	assert_eq!(
		store.state_get(OP_A, &GroupStateKey::bound_unchecked(key_in(RANGE_ONLY, 1))).unwrap(),
		None,
		"a claim that outlived the drop answers the point read and resurrects a row sqlite no longer holds"
	);
	assert!(
		bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()).is_empty(),
		"and the range answer must be empty once the drop marker is drained"
	);
	assert_eq!(
		bodies(&store.range_batch(OP_B, seeded_range(), 64).unwrap()),
		["b1", "b2", "b3"],
		"the neighbour operator keeps every row across the flushed drop"
	);
}

#[test]
fn a_scan_that_steps_over_a_keyspace_the_tier_never_caches_reads_both_keyspaces_out_of_sqlite() {
	// Keycode inverts, so a scan walks keyspaces downward and this span starts in a keyspace the tier
	// never holds and ends in one it does. Spanning two keyspaces names no tier, so the whole span falls
	// through to sqlite and neither keyspace is claimed. The rows must still be exactly right: a
	// fallback is a cost, a short or reordered answer is a wrong result. The control at the end scans
	// the cached keyspace on its own and must be cached, or this test would pass with the tier off.
	let (store, storage, _guard) = cached_store();
	let uncached = KeyspaceId::CUSTOM_NOT_CACHED;
	let cached = CACHED_BELOW_UNCACHED;
	assert!(
		!uncached.caches_ranges(),
		"the fixture needs a keyspace the range tier keeps out and the scan crosses first"
	);
	assert!(
		cached.caches_ranges(),
		"the fixture needs a keyspace the range tier admits and the scan reaches second"
	);
	assert!(
		!KEYSPACES.iter().any(|spec| spec.id.0 > cached.0 && spec.id.0 < uncached.0),
		"no keyspace may sit between the two, or the scan never orders them next to each other"
	);

	// The cached half carries no group column, so the whole span must sit at ROOT or its rows are dropped unread.
	for suffix in 1..=3u8 {
		storage.seed_durable(&[
			OperatorWrite::Insert {
				operator: OP_A,
				key: GroupStateKey::bound_unchecked(root_key_in(uncached, suffix)),
				post: row(&format!("pin{suffix}")),
			},
			OperatorWrite::Insert {
				operator: OP_A,
				key: GroupStateKey::bound_unchecked(root_key_in(cached, suffix)),
				post: row(&format!("pub{suffix}")),
			},
		]);
	}

	let span = EncodedKeyRange::new(
		keyspace_inner_range(GroupId::ROOT, uncached).start,
		keyspace_inner_range(GroupId::ROOT, cached).end,
	);

	let first = store.range_batch(OP_A, span.clone(), 64).unwrap();
	assert_eq!(
		bodies(&first),
		["pin1", "pin2", "pin3", "pub1", "pub2", "pub3"],
		"the scan must answer both keyspaces in full, or it never reached the cached one"
	);
	assert_eq!(
		range_partitions(&store),
		0,
		"a span naming no single keyspace must claim nothing, not even the cached half of it"
	);

	let before = ScanCounters::sample();
	let second = store.range_batch(OP_A, span, 64).unwrap();
	let scanned = before.since();

	assert_eq!(bodies(&second), ["pin1", "pin2", "pin3", "pub1", "pub2", "pub3"]);
	assert_eq!(
		scanned.fetched, 6,
		"with nothing claimed both keyspaces reach sqlite again, the uncacheable one because it must \
         and the cached one because the span never asked its tier"
	);

	let alone = keyspace_inner_range(GroupId::ROOT, cached);
	assert_eq!(bodies(&store.range_batch(OP_A, alone.clone(), 64).unwrap()), ["pub1", "pub2", "pub3"]);
	assert_eq!(range_partitions(&store), 1, "the control: scanned on its own, the cached keyspace claims");
	let before = ScanCounters::sample();
	assert_eq!(bodies(&store.range_batch(OP_A, alone, 64).unwrap()), ["pub1", "pub2", "pub3"]);
	assert_eq!(
		before.since().fetched,
		0,
		"and answers the next pass from ram, or the fallback above is measuring a dead tier"
	);
}

use reifydb_store_operator::types::FlushBatch;

trait SeedDurable {
	fn seed_durable(&self, writes: &[OperatorWrite]);
}

impl SeedDurable for SqlitePersistent {
	fn seed_durable(&self, writes: &[OperatorWrite]) {
		let mut batch = FlushBatch::default();
		for write in writes {
			let (operator, key, post) = match write {
				OperatorWrite::Insert {
					operator,
					key,
					post,
				} => (*operator, key, Some(post.clone())),
				OperatorWrite::Replace {
					operator,
					key,
					post,
					..
				} => (*operator, key, Some(post.clone())),
				OperatorWrite::Remove {
					operator,
					key,
					..
				} => (*operator, key, None),
			};
			let write = match post {
				Some(row) => StagedWrite::Set(row),
				None => StagedWrite::Remove,
			};
			batch.writes.push((operator, key.clone(), write));
		}
		self.flush_batch(&batch);
	}
}

fn remove(store: &OperatorStore, operator: OperatorId, key: &EncodedKey) {
	let pre = match store.state_get(operator, &GroupStateKey::bound_unchecked(key.clone())).unwrap() {
		Some(row) => LayeredPre::Present(ByteSize::from_bytes(row.bytes().len() as u64)),
		None => LayeredPre::Absent,
	};
	store.apply_batch(&[OperatorWrite::Remove {
		operator,
		key: GroupStateKey::bound_unchecked(key.clone()),
		pre,
	}]);
}

fn bury_under_deletions(store: &OperatorStore, count: u8) {
	// a remove on a key sqlite never held still records deletion state, so the range must cross a long run of
	// it to reach the one row that survives
	for suffix in 1..=count {
		store.apply_batch(&[OperatorWrite::Remove {
			operator: OP_A,
			key: GroupStateKey::bound_unchecked(key_in(RANGE_ONLY, suffix)),
			pre: LayeredPre::Absent,
		}]);
	}
	put(store, OP_A, key_in(RANGE_ONLY, 250), row("live"));
}

#[test]
fn a_range_dominated_by_deletions_answers_in_one_call_because_deletion_state_is_never_walked() {
	// deletion state is kept beside the rows of a partition, not among them, so a graveyard costs an ordinary
	// scan nothing: the page reaches the surviving row directly instead of spending its budget stepping over
	// entries that can never answer
	let (store, _storage, _guard) = cached_store();
	bury_under_deletions(&store, 200);

	let batch = store.range_batch(OP_A, seeded_range(), 1).unwrap();

	assert_eq!(bodies(&batch), ["live"], "a scan that walks deletion state cannot reach the row behind it");
	assert!(!batch.has_more, "the one live row inside the range is the whole answer");
	assert!(batch.resume.is_none(), "a scan that never spent its budget has nothing to resume from");
}

#[test]
fn a_deleted_durable_row_stays_gone_from_a_forward_range() {
	// with deletion state out of the page there is no masking entry for the merge to meet, so the row sqlite
	// still holds is suppressed only if the scan asks the resident tier about it
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);
	remove(&store, OP_A, &key_in(RANGE_ONLY, 2));

	let batch = store.range_batch(OP_A, seeded_range(), 64).unwrap();

	assert_eq!(bodies(&batch), ["v1", "v3"], "a row the operator removed must not be served from sqlite");
}

#[test]
fn a_deleted_durable_row_stays_gone_from_a_backward_scan() {
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);
	remove(&store, OP_A, &key_in(RANGE_ONLY, 2));

	let seen: Vec<String> =
		store.state_last_iter(OP_A, seeded_range()).map(|entry| body(&entry.unwrap().1)).collect();

	assert_eq!(seen, ["v3", "v1"], "the backward scan must honour the same deletions the forward scan does");
}

#[test]
fn a_deleted_durable_row_stays_gone_from_a_group_page() {
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);
	remove(&store, OP_A, &key_in(RANGE_ONLY, 2));

	let batch = store.group_page(OP_A, &[group()], 64).unwrap();

	assert_eq!(bodies(&batch), ["v1", "v3"], "a group page must honour the same deletions a range does");
}

#[test]
fn a_remove_of_a_key_this_store_flushed_is_not_collapsed_away() {
	// a removal may be dropped instead of recorded only when no lower tier can hold the key; a key that went
	// out in a flush slice is durable, so collapsing its removal serves the row again on the next read
	let (store, storage, _guard) = cached_store();
	let key = key_in(RANGE_ONLY, 1);
	put(&store, OP_A, key.clone(), row("staged"));
	assert!(store.flush_pending_blocking(), "the row must reach sqlite before its removal is put to the test");
	assert!(storage.get(OP_A, &key).is_some(), "the fixture must leave a durable row behind to resurrect");

	remove(&store, OP_A, &key);

	assert!(
		store.state_get(OP_A, &GroupStateKey::bound_unchecked(key.clone())).unwrap().is_none(),
		"the removed row must not read back as a point get"
	);
	assert!(
		bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()).is_empty(),
		"nor may it come back through a range"
	);
}

#[test]
fn a_key_removed_and_written_again_reads_back_as_the_later_write() {
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);
	let key = key_in(RANGE_ONLY, 2);
	remove(&store, OP_A, &key);
	put(&store, OP_A, key, row("again"));

	assert_eq!(
		bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()),
		["v1", "again", "v3"],
		"a key moved out of the deleted keys and back must answer as a row again"
	);
}

#[test]
fn a_durable_row_removed_written_and_removed_again_does_not_resurrect() {
	// moving a key between the rows and the deleted keys must carry its staging history across; a key that
	// forgets it was ever durable becomes eligible to have its removal collapsed and comes back from sqlite
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);
	let key = key_in(RANGE_ONLY, 2);
	remove(&store, OP_A, &key);
	put(&store, OP_A, key.clone(), row("again"));
	remove(&store, OP_A, &key);

	assert_eq!(
		bodies(&store.range_batch(OP_A, seeded_range(), 64).unwrap()),
		["v1", "v3"],
		"the durable row must stay masked through the whole cycle"
	);
}

#[test]
fn a_scan_that_stops_on_its_budget_still_returns_every_row_when_it_resumes() {
	// an empty page that ends the scan drops rows sitting past a long tombstone run, and a short answer is
	// indistinguishable from a correct one at the caller
	let (store, _storage, _guard) = cached_store();
	bury_under_deletions(&store, 200);

	let mut range = seeded_range();
	let mut seen: Vec<String> = Vec::new();
	let mut rounds = 0;
	loop {
		rounds += 1;
		assert!(rounds < 1000, "a resume point that does not advance past what it consumed loops forever");
		let batch = store.range_batch(OP_A, range.clone(), 1).unwrap();
		seen.extend(bodies(&batch));
		if !batch.has_more {
			break;
		}
		let next = match batch.resume {
			Some(key) => key.into_encoded(),
			None => batch
				.items
				.last()
				.expect("a batch with more to give carries a row or a resume point")
				.0
				.as_encoded()
				.clone(),
		};
		range = EncodedKeyRange::new(Bound::Excluded(next), range.end.clone());
	}

	assert_eq!(seen, vec!["live".to_string()], "the row past the graveyard must survive the whole scan");
}

#[test]
fn a_range_that_fits_inside_the_scan_budget_names_no_resume_point() {
	// the budget is a ceiling, not a page size; an ordinary range must answer in one call as it always did
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);

	let batch = store.range_batch(OP_A, seeded_range(), 64).unwrap();

	assert_eq!(bodies(&batch), ["v1", "v2", "v3"]);
	assert!(!batch.has_more, "the whole range fit in one page");
	assert!(batch.resume.is_none(), "a scan that ran to the end of its range has nothing to resume from");
}

#[test]
fn a_group_range_is_served_from_the_claims_its_keyspaces_hold() {
	// A group sweep is one range across every keyspace of a group. Answered as one flat scan it ignores the
	// per keyspace claims, so a group the tier already holds whole still pays sqlite on every sweep, and the
	// reaper sweeps the same groups again and again.
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);
	for suffix in 1..=2u8 {
		storage.seed_durable(&[OperatorWrite::Insert {
			operator: OP_A,
			key: GroupStateKey::bound_unchecked(key_in(RANGE_ONLY_ABOVE, suffix)),
			post: row(&format!("c{suffix}")),
		}]);
	}

	let primed = store.range_batch(OP_A, group_inner_range(group()), 64).unwrap();
	assert_eq!(
		bodies(&primed),
		["c1", "c2", "v1", "v2", "v3"],
		"a group range must answer for every keyspace the group holds, in key order"
	);

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, group_inner_range(group()), 64).unwrap();
	let scanned = before.since();

	assert_eq!(bodies(&served), ["c1", "c2", "v1", "v2", "v3"], "the second sweep must answer with the same rows");
	assert_eq!(scanned.fetched, 0, "a group whose every keyspace is claimed must not reach sqlite a second time");
}

#[test]
fn a_data_only_group_range_answers_for_every_data_keyspace_of_the_group() {
	// The reaper frees a group through the data only range; a keyspace filter that misses one leaves rows
	// behind under a group id whose identity is about to be reclaimed.
	let (store, storage, _guard) = cached_store();
	seed_rows(&storage, 3);
	for suffix in 1..=2u8 {
		storage.seed_durable(&[OperatorWrite::Insert {
			operator: OP_A,
			key: GroupStateKey::bound_unchecked(key_in(RANGE_ONLY_ABOVE, suffix)),
			post: row(&format!("c{suffix}")),
		}]);
	}

	let swept = store.range_batch(OP_A, group_data_inner_range(group()), 64).unwrap();

	assert_eq!(
		bodies(&swept),
		bodies(&store.range_batch(OP_A, group_inner_range(group()), 64).unwrap()),
		"both keyspaces hold data rows, so the data only sweep must answer with the whole group"
	);
}
