// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Flush+evict (RAM-bounding) behaviour exercised through the full StandardMultiStore.
//!
//! The eviction sweep is the fix that bounds the commit tier's RAM: on a watermark W it persists the
//! latest-<=W value per key of every PERSISTENT shape to the persistent tier, then drops ALL <=W versions from
//! the commit tier (persistent or not), keeping only versions > W resident. These tests pin that the store still
//! returns correct values after eviction (served from persistent for evicted versions, from the commit tier for
//! resident ones), that `persistent:false` shapes are evicted WITHOUT being persisted, and that the post-eviction
//! MVCC view matches an identical never-evicted store.
//!
//! The sweep itself fires on a background timer inside the FlushActor and is not directly callable from an
//! integration test. `sweep_through_store` below replicates the sweep's composition through the public store API
//! (`collect_evictable_below` -> persist -> drop) and reproduces its read-tier effect: ephemeral keys are
//! invalidated, persistent keys are left resident in the read tier (the actor seeds them on eviction; here a
//! post-drop read-through warms the same entry) so the store-level read-through can be asserted deterministically.
//! Two tests drive the genuine FlushActor timer: `real_flush_actor_sweep_bounds_ram_end_to_end` proves the wiring
//! fires, and `real_flush_actor_seeds_read_tier_on_eviction` proves the sweep seeds the read tier with the
//! persisted value so a post-eviction read skips the persistent tier.

use std::{collections::HashMap, sync::Arc, time::Instant};

use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	encoded::{key::EncodedKey, row::EncodedRow},
	event::EventBus,
	interface::{
		catalog::{id::TableId, shape::ShapeId},
		store::{EntryKind, MultiVersionCommit, MultiVersionGet, classify_key},
	},
	key::row::RowKey,
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_multi::{
	MultiVersionScope,
	config::{CommitBufferConfig, MultiStoreConfig, PersistentConfig},
	flush::ShapePersistence,
	gc::EvictionWatermark,
	store::StandardMultiStore,
	tier::{TierStorage, VersionedGetResult, commit::buffer::MultiCommitBufferTier},
};
use reifydb_value::{cow_vec, util::cowvec::CowVec, value::duration::Duration};

const SHAPE: ShapeId = ShapeId::Table(TableId(1));

fn store_with_persistent() -> (StandardMultiStore, impl Drop) {
	StandardMultiStore::testing_memory_with_persistent_sqlite()
}

/// Build a store whose flush actor ticks quickly, so a set eviction watermark drives the real sweep promptly.
fn store_with_fast_flush() -> (StandardMultiStore, impl Drop) {
	let pools = Pools::new(PoolConfig::default());
	let clock = Clock::Real;
	let actor_system = ActorSystem::new(pools, clock.clone());
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	let event_bus = EventBus::new(&spawner);
	let (persistent, guard) = PersistentConfig::sqlite_in_memory();
	let store = StandardMultiStore::new(MultiStoreConfig {
		commit: Some(CommitBufferConfig {
			storage: MultiCommitBufferTier::memory(),
		}),
		persistent: Some(persistent.flush_interval(Duration::from_milliseconds(25).unwrap())),
		retention: Default::default(),
		merge_config: Default::default(),
		event_bus,
		spawner,
		clock,
	})
	.unwrap();
	(store, guard)
}

fn row_key(row: u64) -> EncodedKey {
	RowKey::encoded(SHAPE, row)
}

fn commit(store: &StandardMultiStore, k: &EncodedKey, version: u64, value: &str) {
	MultiVersionCommit::commit(
		store,
		cow_vec![Delta::Set {
			key: k.clone(),
			row: EncodedRow(CowVec::new(value.as_bytes().to_vec())),
		}],
		CommitVersion(version),
	)
	.unwrap();
}

fn get(store: &StandardMultiStore, k: &EncodedKey, version: u64) -> Option<Vec<u8>> {
	store.get(k, CommitVersion(version)).unwrap().map(|r| r.row.to_vec())
}

fn scan_keys(store: &StandardMultiStore, version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
	store.range(
		RowKey::full_scan(SHAPE),
		MultiVersionScope::AsOf {
			read: CommitVersion(version),
		},
		1024,
	)
	.collect::<Result<Vec<_>, _>>()
	.unwrap()
	.into_iter()
	.map(|r| (r.key.to_vec(), r.row.to_vec()))
	.collect()
}

/// Deterministic stand-in for FlushActor::sweep: collects evictable-below-W per entry kind, persists the
/// latest-<=W value of persistent shapes, then drops all <=W versions from the commit tier. It reproduces the
/// actor's post-sweep read-tier state: ephemeral (`persistent:false`) keys are invalidated, while persistent keys
/// are left resident in the read tier (the actor seeds them on eviction; here a post-drop read-through warms the
/// same entry). `persistent` decides whether the (single) shape is treated as persistent, mirroring the actor's
/// `is_persistent_shape` gate.
fn sweep_through_store(store: &StandardMultiStore, cutoff: CommitVersion, persistent_shape: bool) {
	let commit = store.commit().expect("commit tier configured");
	let kinds = commit.list_all_entry_kinds().unwrap();
	for kind in kinds {
		let (to_persist, to_drop) = match commit {
			MultiCommitBufferTier::Memory(s) => s.collect_evictable_below(kind, cutoff),
		};
		if to_drop.is_empty() {
			continue;
		}

		if persistent_shape && !to_persist.is_empty() {
			let persistent = store.persistent().expect("persistent tier configured");
			let mut by_version: HashMap<
				CommitVersion,
				HashMap<EntryKind, Vec<(EncodedKey, Option<CowVec<u8>>)>>,
			> = HashMap::new();
			for (key, version, value) in &to_persist {
				by_version
					.entry(*version)
					.or_default()
					.entry(kind)
					.or_default()
					.push((key.clone(), value.clone()));
			}
			for (version, batch) in by_version {
				persistent.set(version, batch).unwrap();
			}
		}

		if !persistent_shape {
			for (key, _) in &to_drop {
				store.invalidate_read_key(key);
			}
		}

		commit.drop(HashMap::from([(kind, to_drop)])).unwrap();

		if persistent_shape {
			for (key, version, _) in &to_persist {
				store.get(key, *version).unwrap();
			}
		}
	}
}

#[test]
fn eviction_persists_latest_below_w_and_drops_them_from_commit_tier() {
	// Test 1 (a,b,c,d): commit v1<v2<v3 for one persistent key, evict <= 2. The latest-<=2 value (v2) must
	// be persisted; the commit tier must no longer hold v1/v2 (counts drop) while v3 stays resident; and both
	// point reads and range scans must still return the correct values across the tier boundary.
	let (store, _guard) = store_with_persistent();
	let kind = EntryKind::Source(SHAPE);
	let k = row_key(1);

	commit(&store, &k, 1, "v1");
	commit(&store, &k, 2, "v2");
	commit(&store, &k, 3, "v3");

	let commit_tier = store.commit().unwrap();
	let current_before = commit_tier.count_current(kind).unwrap();
	let historical_before = commit_tier.count_historical(kind).unwrap();
	assert_eq!(current_before, 1, "v3 is the current version");
	assert_eq!(historical_before, 2, "v1 and v2 are historical");

	sweep_through_store(&store, CommitVersion(2), true);

	// (a) the latest-<=W value (v2) is now in the persistent tier.
	let persistent = store.persistent().unwrap();
	assert!(
		matches!(persistent.get(kind, k.as_ref(), CommitVersion(2)).unwrap(), VersionedGetResult::Value { .. }),
		"v2 must be persisted"
	);

	// (b) the commit tier no longer holds <= W versions: only v3 remains, nothing historical.
	assert_eq!(commit_tier.count_current(kind).unwrap(), 1, "v3 still current");
	assert_eq!(commit_tier.count_historical(kind).unwrap(), 0, "v1/v2 dropped from the commit tier's history");
	assert!(
		matches!(commit_tier.get(kind, k.as_ref(), CommitVersion(2)).unwrap(), VersionedGetResult::NotFound),
		"the commit tier must not answer for an evicted version"
	);

	// (c) versions > W remain resident in the commit tier.
	assert_eq!(
		commit_tier.get(kind, k.as_ref(), CommitVersion(3)).unwrap().value().as_deref(),
		Some(b"v3".as_slice()),
		"v3 (> W) stays in the commit tier"
	);

	// (d) point reads still correct across the boundary: v3 from commit, v2 from persistent.
	assert_eq!(get(&store, &k, 3).as_deref(), Some(b"v3".as_slice()));
	assert_eq!(get(&store, &k, 2).as_deref(), Some(b"v2".as_slice()), "served from persistent after eviction");
	// Range scan at the latest snapshot still surfaces the live row.
	let scanned = scan_keys(&store, 3);
	assert!(scanned.iter().any(|(kk, vv)| kk == k.as_ref() && vv == b"v3"), "scan must still see the live row");
}

#[test]
fn persistent_false_shape_is_dropped_without_persisting() {
	// Test 2: a persistent:false shape must still be EVICTED below W (the RAM-bounding fix), but its value must
	// NOT be written to the persistent tier. A read after eviction returns NotFound - it was RAM-only ephemeral.
	// This guards the bug where persistent:false shapes were never evicted (unbounded RAM) or were wrongly
	// flushed to disk.
	let (store, _guard) = store_with_persistent();
	let kind = EntryKind::Source(SHAPE);
	let k = row_key(1);

	commit(&store, &k, 1, "v1");
	commit(&store, &k, 2, "v2");

	sweep_through_store(&store, CommitVersion(2), false);

	let commit_tier = store.commit().unwrap();
	assert!(
		matches!(commit_tier.get(kind, k.as_ref(), CommitVersion(2)).unwrap(), VersionedGetResult::NotFound),
		"a persistent:false shape must still be evicted from the commit tier below W"
	);

	let persistent = store.persistent().unwrap();
	assert!(
		matches!(persistent.get(kind, k.as_ref(), CommitVersion(2)).unwrap(), VersionedGetResult::NotFound),
		"a persistent:false shape must NOT be written to the persistent tier"
	);

	// A read after eviction returns nothing: the value was ephemeral and is gone everywhere.
	assert_eq!(get(&store, &k, 2), None, "an evicted persistent:false value must read as NotFound");
}

#[test]
fn mvcc_view_after_eviction_matches_a_never_evicted_store() {
	// Test 3: versions v1<v2<v3; evict <= v2 in one store, leave an identical store untouched. Every snapshot
	// read (including a between-version snapshot) must resolve identically. This is the parity check that the
	// tier boundary introduced by eviction is invisible to MVCC semantics.
	let (evicted, _evicted_guard) = store_with_persistent();
	let (intact, _intact_guard) = store_with_persistent();
	let k = row_key(1);

	for store in [&evicted, &intact] {
		commit(store, &k, 1, "v1");
		commit(store, &k, 2, "v2");
		commit(store, &k, 3, "v3");
	}

	sweep_through_store(&evicted, CommitVersion(2), true);

	// The sweep persists only the LATEST-<=W value per key (v2) and drops every <=W version (v1 and v2) from
	// the commit tier. So at and above the persisted floor (W=2) the evicted store must resolve identically to
	// the intact store; below the floor (snapshot 1) the evicted store correctly returns NotFound, because v1
	// was discarded - it is no longer reachable at any snapshot. This is the documented RAM-bounding trade:
	// snapshots older than the eviction watermark are not preserved.
	for snapshot in [2u64, 3, 4] {
		assert_eq!(
			get(&evicted, &k, snapshot),
			get(&intact, &k, snapshot),
			"snapshot {snapshot} (>= W) must resolve identically in the evicted and intact stores"
		);
	}

	// Spell out the absolute expectations so the parity check cannot pass vacuously (both stores broken alike).
	assert_eq!(get(&evicted, &k, 3).as_deref(), Some(b"v3".as_slice()), "v3 from the commit tier");
	assert_eq!(get(&evicted, &k, 2).as_deref(), Some(b"v2".as_slice()), "v2 from the persistent tier");
	assert_eq!(
		get(&intact, &k, 1).as_deref(),
		Some(b"v1".as_slice()),
		"the never-evicted store still holds v1 in its commit-tier history"
	);
	assert_eq!(
		get(&evicted, &k, 1),
		None,
		"v1 (below W) was discarded by the sweep: only the latest-<=W value is preserved"
	);
}

#[test]
fn versions_above_w_are_left_entirely_resident() {
	// Guards the converse of eviction: with W below all committed versions, nothing is persisted and nothing is
	// dropped. A regression that evicted too eagerly would surface here.
	let (store, _guard) = store_with_persistent();
	let kind = EntryKind::Source(SHAPE);
	let k = row_key(1);
	commit(&store, &k, 5, "v5");

	sweep_through_store(&store, CommitVersion(3), true);

	let commit_tier = store.commit().unwrap();
	assert_eq!(
		commit_tier.get(kind, k.as_ref(), CommitVersion(5)).unwrap().value().as_deref(),
		Some(b"v5".as_slice()),
		"v5 (> W) must stay resident"
	);
	let persistent = store.persistent().unwrap();
	assert!(
		matches!(persistent.get(kind, k.as_ref(), CommitVersion(5)).unwrap(), VersionedGetResult::NotFound),
		"nothing below W => nothing persisted"
	);
}

struct StaticWatermark(CommitVersion);
impl EvictionWatermark for StaticWatermark {
	fn watermark(&self) -> CommitVersion {
		self.0
	}
}

struct AllPersistent;
impl ShapePersistence for AllPersistent {
	fn is_persistent(&self, _shape: ShapeId) -> bool {
		true
	}
}

#[test]
fn real_flush_actor_sweep_bounds_ram_end_to_end() {
	// End-to-end: drive the GENUINE FlushActor sweep via a set eviction watermark and a fast flush timer. This
	// proves the production wiring (watermark -> tick -> sweep -> persist + drop) actually fires and bounds the
	// commit tier's RAM, not just the hand-rolled stand-in above. Polls for the observable effect with a
	// bounded timeout so a never-firing sweep fails loudly rather than hanging.
	//
	// This is the exact gap the "flush == evict-safe" redesign closed. Flush is now SOLELY the watermark sweep;
	// there is no eager drain racing it. The persistent tier is CURRENT-ONLY with a version-guarded upsert
	// (WHERE excluded.version >= stored.version), so the sweep must persist the latest-<=W value (v2) - not the
	// current v3. Under the old eager-drain model the drain wrote v3 to persistent out-of-band, and the sweep's
	// correct v2 was then rejected as a lower version, leaving a read at the W snapshot returning NotFound. With
	// the drain gone, persistent holds v2: a read at W (v2) resolves to v2 from persistent, a read at v3 resolves
	// to v3 from the commit tier, and the <= W history is gone from the commit tier (RAM bounded).
	let (store, _guard) = store_with_fast_flush();
	let kind = EntryKind::Source(SHAPE);
	let k = row_key(1);

	store.set_row_settings_provider(Arc::new(AllPersistent));
	store.set_eviction_watermark(Arc::new(StaticWatermark(CommitVersion(2))));

	commit(&store, &k, 1, "v1");
	commit(&store, &k, 2, "v2");
	commit(&store, &k, 3, "v3");
	store.flush_pending_blocking();

	let commit_tier = store.commit().unwrap();
	let deadline = Instant::now() + Duration::from_seconds(10).unwrap().to_std();
	loop {
		// The sweep drops the <= W history from the commit tier: historical falls to 0 and a commit-tier read
		// at the watermark snapshot no longer resolves (only the current v3 > W remains resident).
		let historical = commit_tier.count_historical(kind).unwrap();
		let evicted_gone = matches!(
			commit_tier.get(kind, k.as_ref(), CommitVersion(2)).unwrap(),
			VersionedGetResult::NotFound
		);
		if historical == 0 && evicted_gone {
			break;
		}
		if Instant::now() >= deadline {
			panic!(
				"flush actor sweep did not evict <= W within the timeout (historical={historical}, evicted_gone={evicted_gone})"
			);
		}
		std::thread::yield_now();
	}

	// v3 (> W) is still resident in the commit tier; the live snapshot reads correctly.
	assert_eq!(
		commit_tier.get(kind, k.as_ref(), CommitVersion(3)).unwrap().value().as_deref(),
		Some(b"v3".as_slice()),
		"v3 (> W) stays resident in the commit tier after the sweep"
	);
	assert_eq!(get(&store, &k, 3).as_deref(), Some(b"v3".as_slice()), "the live snapshot reads correctly");

	// The sweep persists the latest-<=W value (v2), not the current v3. The version guard would have rejected a
	// stale v3-over-v2 write, but there is no eager drain to write v3 out-of-band any more.
	let persistent = store.persistent().unwrap();
	assert_eq!(
		persistent.get(kind, k.as_ref(), CommitVersion(2)).unwrap().value().as_deref(),
		Some(b"v2".as_slice()),
		"the latest-<=W value (v2) is durable in the persistent tier"
	);

	// The regression: a read at the eviction-watermark snapshot (W = v2) must return v2 from persistent, NOT
	// NotFound. This is exactly what failed when the eager drain overwrote persistent with v3.
	assert_eq!(
		get(&store, &k, 2).as_deref(),
		Some(b"v2".as_slice()),
		"a read at the eviction watermark must resolve to the latest-<=W value from persistent, not NotFound"
	);
}

#[test]
fn real_flush_actor_seeds_read_tier_on_eviction() {
	// Seed-on-evict: when the genuine sweep evicts a persistent key, it must SEED the read tier with the value it
	// just persisted, not invalidate it. Proven deterministically by deleting the key from the persistent tier
	// AFTER eviction - a read can then only still return the value if it is resident in the read tier. Under the
	// old invalidate-on-evict behaviour this read returned NotFound (cache punched, SQLite row gone).
	let (store, _guard) = store_with_fast_flush();
	let kind = EntryKind::Source(SHAPE);
	let k = row_key(1);

	store.set_row_settings_provider(Arc::new(AllPersistent));
	store.set_eviction_watermark(Arc::new(StaticWatermark(CommitVersion(2))));

	commit(&store, &k, 1, "v1");
	commit(&store, &k, 2, "v2");
	store.flush_pending_blocking();

	let commit_tier = store.commit().unwrap();
	let deadline = Instant::now() + Duration::from_seconds(10).unwrap().to_std();
	loop {
		let evicted = matches!(
			commit_tier.get(kind, k.as_ref(), CommitVersion(2)).unwrap(),
			VersionedGetResult::NotFound
		);
		if evicted {
			break;
		}
		if Instant::now() >= deadline {
			panic!("flush actor sweep did not evict v2 from the commit tier within the timeout");
		}
		std::thread::yield_now();
	}

	// SQLite no longer has the key; the read tier is the only place its value can survive.
	let persistent = store.persistent().unwrap();
	let deleted = persistent.delete_keys(kind, std::slice::from_ref(&k)).unwrap();
	assert_eq!(deleted, 1, "the evicted key must have been durable in the persistent tier before the delete");

	assert_eq!(
		get(&store, &k, 2).as_deref(),
		Some(b"v2".as_slice()),
		"after eviction the read tier must serve the seeded value even though the persistent row is gone; \
		 invalidate-on-evict would return NotFound here"
	);
}

#[test]
fn seeded_read_tier_entry_loses_to_a_newer_resident_commit_version() {
	// A seeded (older) read-tier entry must never shadow a newer version still resident in the commit tier. The
	// sweep seeds v2 (<= W) while v5 (> W) stays in the commit tier. Deleting the persistent row isolates the read
	// tier as the only source of v2, so the version resolution is exercised purely against the seed.
	let (store, _guard) = store_with_fast_flush();
	let kind = EntryKind::Source(SHAPE);
	let k = row_key(1);

	store.set_row_settings_provider(Arc::new(AllPersistent));
	store.set_eviction_watermark(Arc::new(StaticWatermark(CommitVersion(2))));

	commit(&store, &k, 1, "v1");
	commit(&store, &k, 2, "v2");
	commit(&store, &k, 5, "v5");
	store.flush_pending_blocking();

	let commit_tier = store.commit().unwrap();
	let deadline = Instant::now() + Duration::from_seconds(10).unwrap().to_std();
	loop {
		let evicted = matches!(
			commit_tier.get(kind, k.as_ref(), CommitVersion(2)).unwrap(),
			VersionedGetResult::NotFound
		);
		if evicted {
			break;
		}
		if Instant::now() >= deadline {
			panic!("flush actor sweep did not evict <= W (v2) from the commit tier within the timeout");
		}
		std::thread::yield_now();
	}

	let persistent = store.persistent().unwrap();
	persistent.delete_keys(kind, std::slice::from_ref(&k)).unwrap();

	// The newer resident version wins (served from the commit tier).
	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()), "a reader at v5 must see the resident v5");
	// An older snapshot is served the seeded v2 from the read tier (persistent row deleted).
	assert_eq!(
		get(&store, &k, 2).as_deref(),
		Some(b"v2".as_slice()),
		"an older snapshot must be served the seeded v2 from the read tier"
	);
	// A between snapshot resolves to the latest <= it, which is the seeded v2 (v5 is not yet visible).
	assert_eq!(get(&store, &k, 4).as_deref(), Some(b"v2".as_slice()), "v4 resolves to the latest <= 4 (seeded v2)");
}

/// Build a row value. TTL eviction now keys off the per-key commit version, not any header
/// timestamp, so the value is just the payload bytes.
fn versioned_row(payload: &[u8]) -> CowVec<u8> {
	CowVec::new(payload.to_vec())
}

#[test]
fn row_ttl_deletes_from_persistent_and_invalidated_read_tier_does_not_serve_it() {
	// Test 6: an expired row deleted from the persistent tier (the row-TTL GC's persistent path) must not be
	// returned by a subsequent read, AND a stale read-tier entry for that key must not resurrect it. The row
	// GC actor invalidates the read tier (clear_read / invalidate_read_key) precisely so this can't happen;
	// this pins that the invalidation is load-bearing for correctness, not just a cache-freshness nicety.
	let (store, _guard) = store_with_persistent();
	let kind = EntryKind::Source(SHAPE);
	let k = row_key(1);

	// Land an expired row (created long ago) directly in the persistent tier.
	let persistent = store.persistent().unwrap();
	let table = classify_key(&k);
	persistent
		.set(CommitVersion(1), HashMap::from([(table, vec![(k.clone(), Some(versioned_row(b"old")))])]))
		.unwrap();

	// A point read populates the read tier with the (soon-to-be-stale) value.
	let expected = versioned_row(b"old").to_vec();
	assert_eq!(
		get(&store, &k, 1),
		Some(expected),
		"the persistent row is readable before TTL deletion (and now cached)"
	);

	// Row-TTL GC's persistent step: delete everything whose commit version is at or below the cutoff.
	let deleted = persistent.delete_below_version(kind, CommitVersion(1), None).unwrap();
	assert_eq!(deleted.len(), 1, "the expired row must be physically deleted from the persistent tier");

	// Without invalidation the read tier would still serve "old" - prove the cache is indeed stale right now.
	// (We do NOT assert it here to avoid pinning an implementation detail; instead we run the GC's invalidation
	// step and assert the post-invalidation read is correct.)
	store.invalidate_read_key(&k);

	assert_eq!(
		get(&store, &k, 1),
		None,
		"after TTL deletion + read-tier invalidation, the expired row must read as NotFound"
	);

	// clear_read (the broader invalidation the actor uses when any persistent rows were deleted) must also
	// leave the deleted row unreadable.
	store.clear_read();
	assert_eq!(get(&store, &k, 1), None, "clear_read must not resurrect the deleted row either");
}
