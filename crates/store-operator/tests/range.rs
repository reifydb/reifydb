// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Transparency of the operator range tier. Rows live in partitions, one per group and keyspace, but what a
//! partition may answer is decided by coverage: a set of intervals, each one a claim that the tier holds every
//! key the persistent tier holds strictly within it. Inside a claim the tier answers a range outright and
//! answers a missed point read as a definitive absence; outside one it declines. Anything that can leave a
//! claim short of a key sqlite holds must shrink that claim, since a short answer reads like a correct one.

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, KeyspaceId, OperatorStateKey, keyspace_inner_range},
	metrics::scan::ScanCounters,
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	store::OperatorStore,
	tier::{
		persistent::{OperatorPersistentTier, sqlite::SqliteOperatorStorage},
		point::{OperatorPointConfig, OperatorPointTier},
		range::{OperatorRangeConfig, OperatorRangeTier},
	},
	types::{DurablePre, OperatorBatch, OperatorWrite},
};
use reifydb_value::byte_size::ByteSize;

const CACHED_BELOW_UNCACHED: KeyspaceId = KeyspaceId(KeyspaceId::CUSTOM_NOT_CACHED.0 - 1);

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const GROUP: GroupId = GroupId(7);

fn cached_store() -> (OperatorStore, SqliteOperatorStorage, SqliteTempPathGuard) {
	// The hour-long interval on a frozen clock means the only drain a test sees is the one it asked for.
	cached_store_with(OperatorPointConfig::testing(), OperatorRangeConfig::testing())
}

fn cached_store_with(
	point: OperatorPointConfig,
	range: OperatorRangeConfig,
) -> (OperatorStore, SqliteOperatorStorage, SqliteTempPathGuard) {
	// The two budgets are sized separately so a test can starve one of them without starving the other.
	let clock = Clock::testing();
	let actor_system = ActorSystem::testing(clock.clone());
	let spawner = actor_system.spawner();
	let (storage, guard) = SqliteOperatorStorage::in_memory();
	let store = OperatorStore::standard(OperatorStoreConfig {
		resident: Default::default(),
		persistent: Some(OperatorPersistentConfig::opened(OperatorPersistentTier::Sqlite(storage.clone()))),
		point: Some(point),
		range: Some(range),
		spawner,
		clock,
	});
	(store, storage, guard)
}

fn key_in(keyspace: KeyspaceId, suffix: u8) -> EncodedKey {
	OperatorStateKey::inner_encoded(GROUP, keyspace, [suffix]).as_encoded().clone()
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

fn accumulator_range() -> EncodedKeyRange {
	keyspace_inner_range(GROUP, KeyspaceId::ACCUMULATOR)
}

fn seed_accumulator(storage: &SqliteOperatorStorage, count: u8) {
	for suffix in 1..=count {
		storage.apply_batch(&[OperatorWrite::Insert {
			operator: OP_A,
			key: key_in(KeyspaceId::ACCUMULATOR, suffix),
			post: row(&format!("v{suffix}")),
		}]);
	}
}

fn point_tier(store: &OperatorStore) -> &OperatorPointTier {
	store.point().expect("the fixture configures a point tier")
}

fn range_tier(store: &OperatorStore) -> &OperatorRangeTier {
	store.range().expect("the fixture configures a range tier")
}

fn point_entries(store: &OperatorStore) -> usize {
	point_tier(store).entries()
}

fn range_partitions(store: &OperatorStore) -> usize {
	range_tier(store).partitions()
}

fn range_intervals(store: &OperatorStore) -> usize {
	range_tier(store).intervals()
}

fn put(store: &OperatorStore, operator: OperatorId, key: EncodedKey, row: EncodedPodRow) {
	// reading the pre-image back keeps the claim truthful even when an earlier write in the same test moved the key
	let write = match store.get(operator, &key) {
		Some(pre) => OperatorWrite::Replace {
			operator,
			key,
			pre_value_bytes: ByteSize::from_bytes(pre.bytes().len() as u64),
			post: row,
		},
		None => OperatorWrite::Insert {
			operator,
			key,
			post: row,
		},
	};
	store.apply_batch(&[write]);
}

#[test]
fn a_range_over_a_claimed_span_is_served_without_reaching_the_persistent_tier() {
	// A repeat scan that still fetches from sqlite charges for the rows while serving nothing.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);

	let primed = store.range_batch(OP_A, accumulator_range(), 64);
	assert_eq!(bodies(&primed), ["v1", "v2", "v3"], "the priming scan must read every durable row");
	assert_eq!(
		range_partitions(&store),
		1,
		"a whole-keyspace scan that was not cut short must materialize the span, or nothing below is tested"
	);
	assert_eq!(range_intervals(&store), 1, "one uninterrupted scan must prove one claim, not a claim per row");

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, accumulator_range(), 64);
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
	seed_accumulator(&storage, 3);

	assert!(store.get(OP_A, &key_in(KeyspaceId::ACCUMULATOR, 2)).is_some(), "the point read warms one key");
	assert_eq!(
		range_partitions(&store),
		0,
		"a point fill must never materialize a claim over keys it did not read"
	);

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, accumulator_range(), 64);
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
	seed_accumulator(&storage, 3);

	let primed = store.range_batch(OP_A, accumulator_range(), 64);
	assert_eq!(bodies(&primed), ["v1", "v2", "v3"]);
	assert_eq!(range_partitions(&store), 1, "the claim must start standing or the writes prove nothing");

	put(&store, OP_A, key_in(KeyspaceId::ACCUMULATOR, 4), row("v4"));
	put(&store, OP_A, key_in(KeyspaceId::ACCUMULATOR, 1), row("rewritten"));
	assert!(store.flush_pending_blocking(), "both writes must reach sqlite before the claim is put to the test");

	assert_eq!(
		range_partitions(&store),
		1,
		"neither a key the claim never held nor a rewrite of one it does may retract the claim"
	);
	assert_eq!(range_intervals(&store), 1, "a write inside a standing claim must not split it");

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, accumulator_range(), 64);
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
fn a_range_materialize_that_does_not_fit_its_own_budget_evicts_no_point_entry() {
	// A shared budget would let one range scan flush the point entries that serve their keyspaces.
	let (store, storage, _guard) = cached_store_with(
		OperatorPointConfig {
			shard_bytes: Some(ByteSize::from_bytes(1024)),
			shards: 1,
		},
		OperatorRangeConfig {
			shard_bytes: Some(ByteSize::from_bytes(256)),
			shards: 1,
			..OperatorRangeConfig::testing()
		},
	);
	seed_accumulator(&storage, 8);
	storage.apply_batch(&[OperatorWrite::Insert {
		operator: OP_A,
		key: key_in(KeyspaceId::COUNT, 1),
		post: row("pinned"),
	}]);

	assert!(store.get(OP_A, &key_in(KeyspaceId::COUNT, 1)).is_some(), "the point read warms an entry of its own");
	let point_used = point_tier(&store).resident_bytes();
	let point_held = point_entries(&store);
	assert!(point_used.as_bytes() > 0, "the point budget must be carrying something or eviction is unobservable");

	let served = store.range_batch(OP_A, accumulator_range(), 64);

	assert_eq!(
		bodies(&served),
		["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"],
		"a refused materialize must leave the answer exactly as the persistent tier gave it"
	);
	let counters = range_tier(&store).metrics();
	assert_eq!(counters.materializes_refused, 1, "the materialize must be refused, not silently admitted");
	assert_eq!(counters.materializes, 0);
	assert_eq!(range_partitions(&store), 0, "a refused materialize must leave no partition behind");
	assert_eq!(range_intervals(&store), 0, "and it must leave no claim behind over rows it did not keep");
	assert_eq!(
		point_tier(&store).resident_bytes(),
		point_used,
		"a range materialize must never be charged to the point budget"
	);
	assert_eq!(point_entries(&store), point_held, "a refused range materialize must evict no point entry");
	assert_eq!(
		point_tier(&store).metrics().evictions,
		0,
		"a refused materialize must not start an eviction cascade in the other tier"
	);
	assert_eq!(
		body(&store
			.get(OP_A, &key_in(KeyspaceId::COUNT, 1))
			.expect("the point entry survives the refused materialize")),
		"pinned"
	);
}

#[test]
fn a_range_spanning_two_claimed_keyspaces_is_served_from_the_claims_it_crosses() {
	// Every row inside a claim must come from RAM, the unscanned ground between them from sqlite.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);
	for suffix in 1..=2u8 {
		storage.apply_batch(&[OperatorWrite::Insert {
			operator: OP_A,
			key: key_in(KeyspaceId::COUNT, suffix),
			post: row(&format!("c{suffix}")),
		}]);
	}

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(bodies(&store.range_batch(OP_A, keyspace_inner_range(GROUP, KeyspaceId::COUNT), 64)), ["c1", "c2"]);
	assert_eq!(range_partitions(&store), 2, "both keyspaces must be claimed or the span proves nothing");

	let spanning = EncodedKeyRange::new(
		keyspace_inner_range(GROUP, KeyspaceId::COUNT).start,
		keyspace_inner_range(GROUP, KeyspaceId::ACCUMULATOR).end,
	);
	let counters = range_tier(&store).metrics();
	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, spanning, 64);
	let scanned = before.since();

	assert_eq!(
		bodies(&served),
		["c1", "c2", "v1", "v2", "v3"],
		"a span must answer for every claim it crosses, and for the ground between them"
	);
	assert_eq!(scanned.fetched, 0, "no row inside a standing claim may be re-read out of sqlite");
	let after = range_tier(&store).metrics();
	assert!(after.hits >= counters.hits + 2, "each claim the span crossed must be attributed as its own hit");
}

#[test]
fn a_write_of_a_key_the_claim_never_held_keeps_the_claim_and_still_serves_it() {
	// A claim that serves a range without a key just written to it is a silent short answer.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(range_partitions(&store), 1, "the claim must start standing or the write proves nothing");

	put(&store, OP_A, key_in(KeyspaceId::ACCUMULATOR, 4), row("v4"));

	assert_eq!(
		range_partitions(&store),
		1,
		"a write of a key the claim never held leaves sqlite unchanged, so the claim is still true"
	);
	assert_eq!(range_intervals(&store), 1, "a key written inside a claim joins it rather than splitting it");
	assert_eq!(
		bodies(&store.range_batch(OP_A, accumulator_range(), 64)),
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
	let served = store.range_batch(OP_A, accumulator_range(), 64);
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
	seed_accumulator(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(range_partitions(&store), 1, "the claim must start standing or the write proves nothing");

	put(&store, OP_A, key_in(KeyspaceId::ACCUMULATOR, 2), row("rewritten"));

	assert_eq!(
		range_partitions(&store),
		1,
		"a replace of a key the claim holds must move that one entry, not retract the whole claim"
	);
	assert_eq!(range_intervals(&store), 1, "and it must leave the claim in one piece");
	assert_eq!(
		bodies(&store.range_batch(OP_A, accumulator_range(), 64)),
		["v1", "rewritten", "v3"],
		"and the answer must stay right whichever tier serves it"
	);

	assert!(store.flush_pending_blocking(), "the write must reach sqlite before the kept claim is put to the test");

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, accumulator_range(), 64);
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
	seed_accumulator(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(range_partitions(&store), 1, "the claim must start standing or the removal proves nothing");

	store.apply_batch(&[OperatorWrite::Remove {
		operator: OP_A,
		key: key_in(KeyspaceId::ACCUMULATOR, 2),
		pre: DurablePre::Present(ByteSize::from_bytes(row("v2").bytes().len() as u64)),
	}]);

	assert_eq!(
		range_partitions(&store),
		1,
		"a removal inside a claim must hide the one key, not retract the claim around it"
	);
	assert_eq!(range_intervals(&store), 1, "and it must not punch the claim into two");

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, accumulator_range(), 64);
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
	seed_accumulator(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);

	store.apply_batch(&[OperatorWrite::Remove {
		operator: OP_A,
		key: key_in(KeyspaceId::ACCUMULATOR, 2),
		pre: DurablePre::Present(ByteSize::from_bytes(row("v2").bytes().len() as u64)),
	}]);
	assert!(store.flush_pending_blocking(), "the tombstone must reach sqlite before the claim is put to the test");

	assert_eq!(range_partitions(&store), 1, "a flushed removal must take one entry, not the whole claim");
	assert_eq!(range_intervals(&store), 1, "and it must leave the claim in one piece");

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, accumulator_range(), 64);
	let scanned = before.since();

	assert_eq!(bodies(&served), ["v1", "v3"], "the kept claim must not serve the row the flush erased");
	assert_eq!(scanned.fetched, 0, "the answer must have come from the claim the flushed removal left standing");
	assert_eq!(
		store.get(OP_A, &key_in(KeyspaceId::ACCUMULATOR, 2)),
		None,
		"and the demoted key must answer a point read as a proven absence, not fall through to sqlite"
	);
}

#[test]
fn a_removal_of_a_key_the_claim_never_held_keeps_the_claim() {
	// Retracting on every erase of a key no claim held grinds the whole feature down to nothing.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(range_partitions(&store), 1, "the claim must start standing or the removal proves nothing");

	store.apply_batch(&[OperatorWrite::Remove {
		operator: OP_A,
		key: key_in(KeyspaceId::ACCUMULATOR, 9),
		pre: DurablePre::Absent,
	}]);
	assert!(store.flush_pending_blocking(), "the tombstone must reach sqlite through the same flush path");

	assert_eq!(
		range_partitions(&store),
		1,
		"erasing a key the bucket never held cannot make the bucket short of anything"
	);

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, accumulator_range(), 64);
	let scanned = before.since();

	assert_eq!(bodies(&served), ["v1", "v2", "v3"], "the surviving rows must still all be there");
	assert_eq!(scanned.fetched, 0, "the answer must have come from the bucket that kept its claim");
}

#[test]
fn a_written_row_too_big_for_the_range_budget_takes_the_whole_claim_with_it() {
	// A claim that cannot hold the key just written to it must be retracted, never left short.
	let (store, storage, _guard) = cached_store_with(
		OperatorPointConfig {
			shard_bytes: Some(ByteSize::from_mib(1)),
			shards: 1,
		},
		OperatorRangeConfig {
			shard_bytes: Some(ByteSize::from_bytes(4096)),
			shards: 1,
			..OperatorRangeConfig::testing()
		},
	);
	seed_accumulator(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(range_partitions(&store), 1, "the small materialize must fit its budget or nothing below is tested");

	let huge = "x".repeat(8192);
	put(&store, OP_A, key_in(KeyspaceId::ACCUMULATOR, 4), row(&huge));
	assert_eq!(
		range_partitions(&store),
		0,
		"a row the range budget cannot hold must cost the claim, not be dropped from a claim that goes on \
		 answering for the whole span"
	);
	assert_eq!(range_intervals(&store), 0, "and no interval may outlive the rows it claimed");

	assert!(store.flush_pending_blocking(), "the write must reach sqlite through the flush path");

	assert_eq!(range_partitions(&store), 0, "the flush must not resurrect a claim over a row it cannot hold");

	let served = store.range_batch(OP_A, accumulator_range(), 64);
	assert_eq!(
		bodies(&served),
		["v1", "v2", "v3", huge.as_str()],
		"and the answer must carry the durable row the tier could not keep"
	);
}

#[test]
fn dropping_one_operators_state_forgets_every_claim_and_row_it_cached() {
	// A drop that clears only the point entries lets the range tier resurrect every row it erased.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);
	for suffix in 1..=3u8 {
		storage.apply_batch(&[OperatorWrite::Insert {
			operator: OP_B,
			key: key_in(KeyspaceId::ACCUMULATOR, suffix),
			post: row(&format!("b{suffix}")),
		}]);
	}

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(bodies(&store.range_batch(OP_B, accumulator_range(), 64)), ["b1", "b2", "b3"]);
	assert_eq!(range_partitions(&store), 2, "both operators must hold a claim or the scoping below proves nothing");

	store.drop_operator_state(OP_A);

	assert_eq!(
		range_partitions(&store),
		1,
		"the drop must take the dropped operator's claims, not only its point entries"
	);

	let before = ScanCounters::sample();
	assert_eq!(bodies(&store.range_batch(OP_B, accumulator_range(), 64)), ["b1", "b2", "b3"]);
	assert_eq!(
		before.since().fetched,
		0,
		"dropping by operator must be scoped, or every teardown throws away every other operator's claims"
	);

	assert!(store.flush_pending_blocking(), "the drop must reach sqlite before the mask stops hiding the tiers");
	assert_eq!(
		store.get(OP_A, &key_in(KeyspaceId::ACCUMULATOR, 1)),
		None,
		"a claim that outlived the drop answers the point read and resurrects a row sqlite no longer holds"
	);
	assert!(
		bodies(&store.range_batch(OP_A, accumulator_range(), 64)).is_empty(),
		"and the range answer must be empty once the drop marker is drained"
	);
	assert_eq!(
		bodies(&store.range_batch(OP_B, accumulator_range(), 64)),
		["b1", "b2", "b3"],
		"the neighbour operator keeps every row across the flushed drop"
	);
}

#[test]
fn a_scan_that_steps_over_a_keyspace_the_tier_never_caches_still_materializes_the_ones_after_it() {
	// Keycode inverts, so a scan walks keyspaces downward and every cached keyspace sitting one slot
	// below an uncached one is reached only after the tier has already declined a span. Reading that
	// decline as "the tier is full" starves the rest of the scan, and the starved keyspaces re-read
	// sqlite on every pass forever.
	let (store, storage, _guard) = cached_store();
	let uncached = KeyspaceId::CUSTOM_NOT_CACHED;
	let cached = CACHED_BELOW_UNCACHED;
	assert!(
		!uncached.cache_tiers().caches_ranges(),
		"the fixture needs a keyspace the range tier keeps out and the scan crosses first"
	);
	assert!(
		cached.cache_tiers().caches_ranges(),
		"the fixture needs a keyspace the range tier admits and the scan reaches second"
	);
	assert_eq!(cached.0 + 1, uncached.0, "the two keyspaces must be adjacent, or the scan never orders them");

	for suffix in 1..=3u8 {
		storage.apply_batch(&[
			OperatorWrite::Insert {
				operator: OP_A,
				key: key_in(uncached, suffix),
				post: row(&format!("pin{suffix}")),
			},
			OperatorWrite::Insert {
				operator: OP_A,
				key: key_in(cached, suffix),
				post: row(&format!("pub{suffix}")),
			},
		]);
	}

	let span = EncodedKeyRange::new(
		keyspace_inner_range(GROUP, uncached).start,
		keyspace_inner_range(GROUP, cached).end,
	);

	let first = store.range_batch(OP_A, span.clone(), 64);
	assert_eq!(
		bodies(&first),
		["pin1", "pin2", "pin3", "pub1", "pub2", "pub3"],
		"the scan must answer both keyspaces in full, or it never reached the cached one"
	);
	assert_eq!(
		range_partitions(&store),
		1,
		"crossing an uncached keyspace must not stop the scan from materializing the cached one behind it"
	);

	let before = ScanCounters::sample();
	let second = store.range_batch(OP_A, span, 64);
	let scanned = before.since();

	assert_eq!(bodies(&second), ["pin1", "pin2", "pin3", "pub1", "pub2", "pub3"]);
	assert_eq!(
		scanned.fetched, 3,
		"only the three rows of the uncacheable keyspace may reach sqlite twice; the cached keyspace \
         must be answered from ram"
	);
}
