// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Transparency of the operator range tier. A bucket is the set of keys sharing one group and keyspace, and
//! only a scan that covered that whole key range end to end may install one, so residency is itself the claim
//! that the bucket holds every key the persistent tier holds in that range. The claim answers a later range
//! outright and answers a point read the flat tier missed as a definitive absence, so anything that can leave a
//! resident bucket short of a key sqlite holds has to take the whole bucket with it: there is no weaker state
//! to demote it to, and a short answer reads exactly like a correct one.

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{GroupId, Keyspace, OperatorStateKey, keyspace_inner_range},
	metrics::scan::ScanCounters,
};
use reifydb_runtime::{actor::system::ActorSystem, context::clock::Clock};
use reifydb_sqlite::SqliteTempPathGuard;
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	sqlite::SqliteOperatorStorage,
	store::OperatorStore,
	tier::{
		persistent::OperatorPersistentTier,
		point::{OperatorPointConfig, OperatorPointTier},
		range::{OperatorRangeConfig, OperatorRangeTier},
	},
	types::OperatorBatch,
};
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

const OP_A: OperatorId = OperatorId(1);
const OP_B: OperatorId = OperatorId(2);
const GROUP: GroupId = GroupId(7);

fn cached_store() -> (OperatorStore, SqliteOperatorStorage, SqliteTempPathGuard) {
	// The hour-long interval on a frozen clock means the only drain a test sees is the one it asked for.
	cached_store_with(OperatorPointConfig::default(), OperatorRangeConfig::default())
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
		commit: Default::default(),
		persistent: Some(OperatorPersistentConfig::opened(OperatorPersistentTier::Sqlite(storage.clone()))
			.flush_interval(Duration::from_hours_const(1))),
		point: Some(point),
		range: Some(range),
		spawner,
		clock,
	});
	(store, storage, guard)
}

fn key_in(keyspace: Keyspace, suffix: u8) -> EncodedKey {
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
	keyspace_inner_range(GROUP, Keyspace::ACCUMULATOR)
}

fn seed_accumulator(storage: &SqliteOperatorStorage, count: u8) {
	for suffix in 1..=count {
		storage.set(OP_A, key_in(Keyspace::ACCUMULATOR, suffix), row(&format!("v{suffix}")));
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

fn range_buckets(store: &OperatorStore) -> usize {
	range_tier(store).buckets()
}

#[test]
fn a_range_over_a_complete_bucket_is_served_without_reaching_the_persistent_tier() {
	// A scan that ran to the end has paid for every row in the bucket's key range, so a repeat that still fetches
	// from sqlite holds the rows and charges for them while serving nothing.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);

	let primed = store.range_batch(OP_A, accumulator_range(), 64);
	assert_eq!(bodies(&primed), ["v1", "v2", "v3"], "the priming scan must read every durable row");
	assert_eq!(
		range_buckets(&store),
		1,
		"a whole-bucket scan that was not cut short must install the bucket, or nothing below is tested"
	);

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, accumulator_range(), 64);
	let scanned = before.since();

	assert_eq!(bodies(&served), ["v1", "v2", "v3"], "a tier-served range must return the same rows sqlite would");
	assert_eq!(scanned.fetched, 0, "a resident bucket that still scans sqlite saves nothing");
	let counters = range_tier(&store).metrics();
	assert_eq!(counters.hits, 1, "the second scan must be attributed as a range hit");
	assert_eq!(counters.fills, 1, "only the first scan may fill");
}

#[test]
fn a_range_over_an_incomplete_bucket_falls_through_and_still_answers_in_full() {
	// A key warmed by a point read is never provably the whole bucket, so answering a range from it would silently
	// drop every row the point reads never touched.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);

	assert!(store.get(OP_A, &key_in(Keyspace::ACCUMULATOR, 2)).is_some(), "the point read warms one key");
	assert_eq!(range_buckets(&store), 0, "a point fill must never install a bucket claiming to hold every key");

	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, accumulator_range(), 64);
	let scanned = before.since();

	assert_eq!(
		bodies(&served),
		["v1", "v2", "v3"],
		"a tier with no bucket must not shorten the answer to the one key a point read happened to cache"
	);
	assert!(scanned.fetched >= 3, "the answer must have come from sqlite, not from a partial bucket");
	let counters = range_tier(&store).metrics();
	assert_eq!(counters.hits, 0, "an absent bucket may never be counted as a range hit");
	assert_eq!(counters.misses, 1);
}

#[test]
fn a_write_into_a_complete_bucket_stops_it_answering_the_next_range() {
	// A bucket that keeps its claim after losing an entry answers "that key does not exist" for a row sqlite still
	// holds, and only a scan can see the shortfall.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);

	let primed = store.range_batch(OP_A, accumulator_range(), 64);
	assert_eq!(bodies(&primed), ["v1", "v2", "v3"]);
	assert_eq!(range_buckets(&store), 1, "the bucket must start resident or the write proves nothing");

	storage.set(OP_A, key_in(Keyspace::ACCUMULATOR, 4), row("v4"));
	store.set(OP_A, key_in(Keyspace::ACCUMULATOR, 1), row("rewritten"));
	assert!(store.flush_pending_blocking(), "the write must reach sqlite before the staleness is observable");

	assert_eq!(
		range_buckets(&store),
		0,
		"a write of a key the bucket holds must drop the whole bucket, not merely evict the one entry"
	);

	let served = store.range_batch(OP_A, accumulator_range(), 64);
	assert_eq!(
		bodies(&served),
		["rewritten", "v2", "v3", "v4"],
		"a bucket that kept its claim serves the rows it remembers and silently loses the durable row it never saw"
	);
}

#[test]
fn a_range_fill_that_does_not_fit_its_own_budget_evicts_no_point_entry() {
	// A shared budget would let one big bucket scan flush the point entries that serve 99% of their keyspaces,
	// trading a measured win for a measured loss.
	let (store, storage, _guard) = cached_store_with(
		OperatorPointConfig {
			resident_bytes: Some(ByteSize::from_bytes(1024)),
			shards: 1,
		},
		OperatorRangeConfig {
			resident_bytes: Some(ByteSize::from_bytes(256)),
			shards: 1,
		},
	);
	seed_accumulator(&storage, 8);
	storage.set(OP_A, key_in(Keyspace::COUNT, 1), row("pinned"));

	assert!(store.get(OP_A, &key_in(Keyspace::COUNT, 1)).is_some(), "the point read warms an entry of its own");
	let point_used = point_tier(&store).resident_bytes();
	let point_held = point_entries(&store);
	assert!(point_used.as_bytes() > 0, "the point budget must be carrying something or eviction is unobservable");

	let served = store.range_batch(OP_A, accumulator_range(), 64);

	assert_eq!(
		bodies(&served),
		["v1", "v2", "v3", "v4", "v5", "v6", "v7", "v8"],
		"a declined fill must leave the answer exactly as the persistent tier gave it"
	);
	let counters = range_tier(&store).metrics();
	assert_eq!(counters.fills_declined, 1, "the fill must be declined, not silently admitted");
	assert_eq!(counters.fills, 0);
	assert_eq!(range_buckets(&store), 0, "a declined fill must leave no bucket behind claiming to be whole");
	assert_eq!(
		point_tier(&store).resident_bytes(),
		point_used,
		"a range fill must never be charged to the point budget"
	);
	assert_eq!(point_entries(&store), point_held, "a declined range fill must evict no point entry");
	assert_eq!(
		point_tier(&store).metrics().evictions,
		0,
		"a declined fill must not start an eviction cascade in the other tier"
	);
	assert_eq!(
		body(&store
			.get(OP_A, &key_in(Keyspace::COUNT, 1))
			.expect("the point entry survives the declined fill")),
		"pinned"
	);
}

#[test]
fn a_range_spanning_two_complete_buckets_is_never_served_from_the_tier() {
	// The tier cannot tell "that keyspace holds nothing" from "that keyspace was never cached", so serving a span
	// from the first bucket it crosses returns a short answer that reads as a correct one.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);
	for suffix in 1..=2u8 {
		storage.set(OP_A, key_in(Keyspace::COUNT, suffix), row(&format!("c{suffix}")));
	}

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(bodies(&store.range_batch(OP_A, keyspace_inner_range(GROUP, Keyspace::COUNT), 64)), ["c1", "c2"]);
	assert_eq!(range_buckets(&store), 2, "both buckets must be resident or the span proves nothing");

	let spanning = EncodedKeyRange::new(
		keyspace_inner_range(GROUP, Keyspace::COUNT).start,
		keyspace_inner_range(GROUP, Keyspace::ACCUMULATOR).end,
	);
	let counters = range_tier(&store).metrics();
	let before = ScanCounters::sample();
	let served = store.range_batch(OP_A, spanning, 64);
	let scanned = before.since();

	assert_eq!(
		bodies(&served),
		["c1", "c2", "v1", "v2", "v3"],
		"a span must answer for every bucket it crosses; a tier answer stops at the first one"
	);
	assert!(scanned.fetched >= 5, "the span must have been answered by sqlite");
	let after = range_tier(&store).metrics();
	assert_eq!(after.hits, counters.hits, "a span must not be attributed to any single bucket");
	assert_eq!(after.fills, counters.fills, "a span must never fill a bucket it only partly covers");
}

#[test]
fn a_write_of_a_key_the_bucket_never_held_keeps_the_claim_and_the_flush_still_serves_it() {
	// A write the bucket never held leaves sqlite unchanged so the claim is still true, but a surviving claim that
	// then serves a range without the key the flush made durable is a silent short answer, which is worse than no
	// claim at all.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(range_buckets(&store), 1, "the bucket must start resident or the write proves nothing");

	store.set(OP_A, key_in(Keyspace::ACCUMULATOR, 4), row("v4"));

	assert_eq!(
		range_buckets(&store),
		1,
		"a write of a key the bucket never held leaves sqlite unchanged, so the claim is still true"
	);
	assert_eq!(
		bodies(&store.range_batch(OP_A, accumulator_range(), 64)),
		["v1", "v2", "v3", "v4"],
		"the shadowed write must still reach the answer through the commit buffer merge"
	);

	assert!(store.flush_pending_blocking(), "the write must reach sqlite before the claim is put to the test");
	assert_eq!(
		range_buckets(&store),
		1,
		"the flush write-through adds the key to the bucket at the moment sqlite gains it, so the claim survives"
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
	assert_eq!(scanned.fetched, 0, "the answer must have come from the bucket, not from a fallback scan");
}

#[test]
fn a_write_of_a_key_the_bucket_holds_drops_the_claim() {
	// The write shadows the cached row, so a bucket that kept the claim would answer absent for a row sqlite still
	// has.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(range_buckets(&store), 1, "the bucket must start resident or the write proves nothing");

	store.set(OP_A, key_in(Keyspace::ACCUMULATOR, 2), row("rewritten"));

	assert_eq!(
		range_buckets(&store),
		0,
		"a write of a key the bucket holds must drop it; the bucket is now short exactly that key"
	);
	assert_eq!(
		bodies(&store.range_batch(OP_A, accumulator_range(), 64)),
		["v1", "rewritten", "v3"],
		"and the answer must stay right whichever tier serves it"
	);
}

#[test]
fn a_removal_of_a_key_the_bucket_holds_drops_the_claim() {
	// Sqlite keeps the row until the flush drains the tombstone, so in that window the bucket is short a key sqlite
	// has and the claim cannot stand.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(range_buckets(&store), 1, "the bucket must start resident or the removal proves nothing");

	store.remove(OP_A, &key_in(Keyspace::ACCUMULATOR, 2));

	assert_eq!(range_buckets(&store), 0, "a removal of a key the bucket holds must drop it just as a write does");
	assert_eq!(
		bodies(&store.range_batch(OP_A, accumulator_range(), 64)),
		["v1", "v3"],
		"and the removed row must not come back from either tier"
	);
}

#[test]
fn a_removal_of_a_key_the_bucket_never_held_keeps_the_claim() {
	// Retention erases keys by the million against buckets that never held them, so dropping the claim on each one
	// is how the whole feature gets ground down to nothing.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(range_buckets(&store), 1, "the bucket must start resident or the removal proves nothing");

	store.remove(OP_A, &key_in(Keyspace::ACCUMULATOR, 9));
	assert!(store.flush_pending_blocking(), "the tombstone must reach sqlite through the same flush path");

	assert_eq!(
		range_buckets(&store),
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
fn a_flushed_row_too_big_for_the_range_budget_takes_the_whole_bucket_with_it() {
	// The claim survives a write only because the write-through lands the key as sqlite gains it, so a row that
	// cannot fit must take the whole bucket rather than leave one standing that is short the key sqlite just
	// gained.
	let (store, storage, _guard) = cached_store_with(
		OperatorPointConfig {
			resident_bytes: Some(ByteSize::from_mib(1)),
			shards: 1,
		},
		OperatorRangeConfig {
			resident_bytes: Some(ByteSize::from_bytes(4096)),
			shards: 1,
		},
	);
	seed_accumulator(&storage, 3);

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(range_buckets(&store), 1, "the small fill must fit its budget or nothing below is tested");

	let huge = "x".repeat(8192);
	store.set(OP_A, key_in(Keyspace::ACCUMULATOR, 4), row(&huge));
	assert_eq!(
		range_buckets(&store),
		1,
		"the write alone leaves sqlite unchanged, so the eviction below is what has to clear the claim"
	);

	assert!(store.flush_pending_blocking(), "the write must reach sqlite through the flush path");

	assert_eq!(
		range_buckets(&store),
		0,
		"a row the range budget cannot hold must cost the bucket its claim, not be silently dropped from a \
		 bucket that goes on claiming it holds everything"
	);

	let served = store.range_batch(OP_A, accumulator_range(), 64);
	assert_eq!(
		bodies(&served),
		["v1", "v2", "v3", huge.as_str()],
		"and the answer must carry the durable row the bucket could not keep"
	);
}

#[test]
fn dropping_one_operators_state_forgets_the_whole_buckets_it_cached() {
	// A resident bucket answers a point read outright, so a drop that clears only the point entries lets the range
	// tier resurrect every row the drop erased.
	let (store, storage, _guard) = cached_store();
	seed_accumulator(&storage, 3);
	for suffix in 1..=3u8 {
		storage.set(OP_B, key_in(Keyspace::ACCUMULATOR, suffix), row(&format!("b{suffix}")));
	}

	assert_eq!(bodies(&store.range_batch(OP_A, accumulator_range(), 64)), ["v1", "v2", "v3"]);
	assert_eq!(bodies(&store.range_batch(OP_B, accumulator_range(), 64)), ["b1", "b2", "b3"]);
	assert_eq!(range_buckets(&store), 2, "both operators must hold a bucket or the scoping below proves nothing");

	store.drop_operator_state(OP_A);

	assert_eq!(
		range_buckets(&store),
		1,
		"the drop must take the dropped operator's buckets, not only its point entries"
	);

	let before = ScanCounters::sample();
	assert_eq!(bodies(&store.range_batch(OP_B, accumulator_range(), 64)), ["b1", "b2", "b3"]);
	assert_eq!(
		before.since().fetched,
		0,
		"dropping by operator must be scoped, or every teardown throws away every other operator's buckets"
	);

	assert!(store.flush_pending_blocking(), "the drop must reach sqlite before the mask stops hiding the tiers");
	assert_eq!(
		store.get(OP_A, &key_in(Keyspace::ACCUMULATOR, 1)),
		None,
		"a bucket that outlived the drop answers the point read and resurrects a row sqlite no longer holds"
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
