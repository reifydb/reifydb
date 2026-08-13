// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Flush+evict (RAM-bounding) behaviour through the full StandardMultiStore: at watermark W the sweep
//! persists the latest-<=W value of persistent objects and drops all <=W versions from the commit tier,
//! so snapshots older than W are deliberately not preserved.

use std::{collections::HashMap, sync::Arc, time::Instant};

use reifydb_codec::{key::encoded::EncodedKey, row::bytes::EncodedBytes};
use reifydb_core::{
	common::CommitVersion,
	delta::Delta,
	event::EventBus,
	interface::{
		catalog::{id::TableId, storage::StorageId},
		store::{EntryKind, MultiVersionCommit, MultiVersionGet, classify_key},
	},
	key::row::RowKey,
	lifecycle::watermark::EvictionWatermark,
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_multi::{
	MultiVersionScope,
	config::{CommitBufferConfig, MultiStoreConfig, PersistentConfig},
	flush::ObjectPersistence,
	store::StandardMultiStore,
	tier::{TierStorage, VersionedGetResult, commit::buffer::MultiCommitBufferTier, read::ReadBufferConfig},
};
use reifydb_value::{cow_vec, util::cowvec::CowVec, value::duration::Duration};

const STORAGE: StorageId = StorageId::Table(TableId(1));

fn store_with_persistent() -> (StandardMultiStore, impl Drop) {
	StandardMultiStore::testing_memory_with_persistent_sqlite()
}

fn store_with_fast_flush() -> (StandardMultiStore, impl Drop) {
	// The flush engine is wired so a set watermark plus flush_pending_blocking drives the genuine sweep.
	let pools = Pools::new(PoolConfig::default());
	let clock = Clock::Real;
	let actor_system = ActorSystem::new(pools, clock.clone());
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	let event_bus = EventBus::new(&spawner);
	let (persistent, guard) = PersistentConfig::sqlite_in_memory();
	let store = StandardMultiStore::new(MultiStoreConfig {
		commit: CommitBufferConfig {
			storage: MultiCommitBufferTier::memory(),
		},
		read: Some(ReadBufferConfig::default()),
		persistent: Some(persistent),
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
	RowKey::encoded(STORAGE, row)
}

fn commit(store: &StandardMultiStore, k: &EncodedKey, version: u64, value: &str) {
	MultiVersionCommit::commit(
		store,
		cow_vec![Delta::Set {
			key: k.clone(),
			bytes: EncodedBytes(CowVec::new(value.as_bytes().to_vec())),
		}],
		CommitVersion(version),
	)
	.unwrap();
}

fn get(store: &StandardMultiStore, k: &EncodedKey, version: u64) -> Option<Vec<u8>> {
	store.get(k, CommitVersion(version)).unwrap().map(|r| r.bytes.to_vec())
}

fn scan_keys(store: &StandardMultiStore, version: u64) -> Vec<(Vec<u8>, Vec<u8>)> {
	store.range(
		RowKey::full_scan(STORAGE),
		MultiVersionScope::AsOf {
			read: CommitVersion(version),
		},
		1024,
	)
	.collect::<Result<Vec<_>, _>>()
	.unwrap()
	.into_iter()
	.map(|r| (r.key.to_vec(), r.bytes.to_vec()))
	.collect()
}

fn sweep_through_store(store: &StandardMultiStore, cutoff: CommitVersion, persistent_object: bool) {
	// Deterministic stand-in for the flush sweep. It reproduces the sweep's read-tier effect (ephemeral
	// keys invalidated, persistent keys left resident) so store-level read-through can be asserted
	// without racing the actor.
	let commit = store.commit();
	let kinds = commit.list_all_entry_kinds().unwrap();
	for kind in kinds {
		let (to_persist, to_compact, _) = match commit {
			MultiCommitBufferTier::Memory(s) => s.collect_evictable_below(kind, cutoff, usize::MAX),
		};
		if to_compact.is_empty() {
			continue;
		}

		if persistent_object && !to_persist.is_empty() {
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

		if !persistent_object {
			for evicted in &to_compact {
				store.invalidate_read_key(&evicted.key);
			}
		}

		commit.compact(HashMap::from([(kind, to_compact.into_iter().map(|e| (e.key, e.version)).collect())]))
			.unwrap();

		if persistent_object {
			for (key, version, _) in &to_persist {
				store.get(key, *version).unwrap();
			}
		}
	}
}

#[test]
fn eviction_persists_latest_below_w_and_drops_them_from_commit_tier() {
	// Reads must stay correct across the tier boundary the sweep introduces.
	let (store, _guard) = store_with_persistent();
	let kind = EntryKind::Source(STORAGE.into());
	let k = row_key(1);

	commit(&store, &k, 1, "v1");
	commit(&store, &k, 2, "v2");
	commit(&store, &k, 3, "v3");

	let commit_tier = store.commit();
	let current_before = commit_tier.count_current(kind).unwrap();
	let historical_before = commit_tier.count_historical(kind).unwrap();
	assert_eq!(current_before, 1, "v3 is the current version");
	assert_eq!(historical_before, 2, "v1 and v2 are historical");

	sweep_through_store(&store, CommitVersion(2), true);

	let persistent = store.persistent().unwrap();
	assert!(
		matches!(persistent.get(kind, k.as_ref(), CommitVersion(2)).unwrap(), VersionedGetResult::Value { .. }),
		"v2 must be persisted"
	);

	assert_eq!(commit_tier.count_current(kind).unwrap(), 1, "v3 still current");
	assert_eq!(commit_tier.count_historical(kind).unwrap(), 0, "v1/v2 dropped from the commit tier's history");
	assert!(
		matches!(commit_tier.get(kind, k.as_ref(), CommitVersion(2)).unwrap(), VersionedGetResult::NotFound),
		"the commit tier must not answer for an evicted version"
	);

	assert_eq!(
		commit_tier.get(kind, k.as_ref(), CommitVersion(3)).unwrap().value().as_deref(),
		Some(b"v3".as_slice()),
		"v3 (> W) stays in the commit tier"
	);

	assert_eq!(get(&store, &k, 3).as_deref(), Some(b"v3".as_slice()));
	assert_eq!(get(&store, &k, 2).as_deref(), Some(b"v2".as_slice()), "served from persistent after eviction");
	let scanned = scan_keys(&store, 3);
	assert!(scanned.iter().any(|(kk, vv)| kk == k.as_ref() && vv == b"v3"), "scan must still see the live row");
}

#[test]
fn persistent_false_object_is_dropped_without_persisting() {
	// A persistent:false object is RAM-only: skipping its eviction would leave RAM unbounded, and
	// persisting it would write data that was never meant to be durable.
	let (store, _guard) = store_with_persistent();
	let kind = EntryKind::Source(STORAGE.into());
	let k = row_key(1);

	commit(&store, &k, 1, "v1");
	commit(&store, &k, 2, "v2");

	sweep_through_store(&store, CommitVersion(2), false);

	let commit_tier = store.commit();
	assert!(
		matches!(commit_tier.get(kind, k.as_ref(), CommitVersion(2)).unwrap(), VersionedGetResult::NotFound),
		"a persistent:false object must still be evicted from the commit tier below W"
	);

	let persistent = store.persistent().unwrap();
	assert!(
		matches!(persistent.get(kind, k.as_ref(), CommitVersion(2)).unwrap(), VersionedGetResult::NotFound),
		"a persistent:false object must NOT be written to the persistent tier"
	);

	assert_eq!(get(&store, &k, 2), None, "an evicted persistent:false value must read as NotFound");
}

#[test]
fn mvcc_view_after_eviction_matches_a_never_evicted_store() {
	// The tier boundary eviction introduces must be invisible to MVCC semantics at and above W.
	let (evicted, _evicted_guard) = store_with_persistent();
	let (intact, _intact_guard) = store_with_persistent();
	let k = row_key(1);

	for store in [&evicted, &intact] {
		commit(store, &k, 1, "v1");
		commit(store, &k, 2, "v2");
		commit(store, &k, 3, "v3");
	}

	sweep_through_store(&evicted, CommitVersion(2), true);

	// Only the latest-<=W value survives, so parity holds at and above W; snapshot 1 is unreachable by
	// design - that is the RAM-bounding trade.
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
	// With W below every committed version, an over-eager sweep would surface here.
	let (store, _guard) = store_with_persistent();
	let kind = EntryKind::Source(STORAGE.into());
	let k = row_key(1);
	commit(&store, &k, 5, "v5");

	sweep_through_store(&store, CommitVersion(3), true);

	let commit_tier = store.commit();
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
impl ObjectPersistence for AllPersistent {
	fn is_persistent(&self, _storage: StorageId) -> bool {
		true
	}
}

#[test]
fn real_flush_actor_sweep_bounds_ram_end_to_end() {
	// Drives the genuine engine sweep rather than the stand-in. The persistent tier is current-only with a
	// version-guarded upsert, so the sweep must persist the latest-<=W value (v2); anything writing the
	// current v3 out-of-band makes a read at the W snapshot return NotFound.
	let (store, _guard) = store_with_fast_flush();
	let kind = EntryKind::Source(STORAGE.into());
	let k = row_key(1);

	store.set_row_settings_provider(Arc::new(AllPersistent));
	store.set_eviction_watermark(Arc::new(StaticWatermark(CommitVersion(2))));

	commit(&store, &k, 1, "v1");
	commit(&store, &k, 2, "v2");
	commit(&store, &k, 3, "v3");
	store.flush_pending_blocking();

	let commit_tier = store.commit();
	let deadline = Instant::now() + Duration::from_seconds(10).unwrap().to_std();
	loop {
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

	assert_eq!(
		commit_tier.get(kind, k.as_ref(), CommitVersion(3)).unwrap().value().as_deref(),
		Some(b"v3".as_slice()),
		"v3 (> W) stays resident in the commit tier after the sweep"
	);
	assert_eq!(get(&store, &k, 3).as_deref(), Some(b"v3".as_slice()), "the live snapshot reads correctly");

	let persistent = store.persistent().unwrap();
	assert_eq!(
		persistent.get(kind, k.as_ref(), CommitVersion(2)).unwrap().value().as_deref(),
		Some(b"v2".as_slice()),
		"the latest-<=W value (v2) is durable in the persistent tier"
	);

	assert_eq!(
		get(&store, &k, 2).as_deref(),
		Some(b"v2".as_slice()),
		"a read at the eviction watermark must resolve to the latest-<=W value from persistent, not NotFound"
	);
}

#[test]
fn real_flush_actor_seeds_read_tier_on_eviction() {
	// Deleting the persistent row after eviction isolates the read tier as the only possible source, so a
	// successful read proves the sweep seeded rather than invalidated.
	let (store, _guard) = store_with_fast_flush();
	let kind = EntryKind::Source(STORAGE.into());
	let k = row_key(1);

	store.set_row_settings_provider(Arc::new(AllPersistent));
	store.set_eviction_watermark(Arc::new(StaticWatermark(CommitVersion(2))));

	commit(&store, &k, 1, "v1");
	commit(&store, &k, 2, "v2");
	store.flush_pending_blocking();

	let commit_tier = store.commit();
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
	// A seeded (older) read-tier entry must never shadow a newer version still resident in the commit tier;
	// deleting the persistent row isolates the seed as the only source of v2.
	let (store, _guard) = store_with_fast_flush();
	let kind = EntryKind::Source(STORAGE.into());
	let k = row_key(1);

	store.set_row_settings_provider(Arc::new(AllPersistent));
	store.set_eviction_watermark(Arc::new(StaticWatermark(CommitVersion(2))));

	commit(&store, &k, 1, "v1");
	commit(&store, &k, 2, "v2");
	commit(&store, &k, 5, "v5");
	store.flush_pending_blocking();

	let commit_tier = store.commit();
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

	assert_eq!(get(&store, &k, 5).as_deref(), Some(b"v5".as_slice()), "a reader at v5 must see the resident v5");
	assert_eq!(
		get(&store, &k, 2).as_deref(),
		Some(b"v2".as_slice()),
		"an older snapshot must be served the seeded v2 from the read tier"
	);
	assert_eq!(get(&store, &k, 4).as_deref(), Some(b"v2".as_slice()), "v4 resolves to the latest <= 4 (seeded v2)");
}

fn versioned_row(payload: &[u8]) -> CowVec<u8> {
	// TTL eviction keys off the per-key commit version, not a header timestamp, so no header is needed.
	CowVec::new(payload.to_vec())
}

#[test]
fn row_ttl_deletes_from_persistent_and_invalidated_read_tier_does_not_serve_it() {
	// Read-tier invalidation after a persistent TTL delete is load-bearing for correctness, not a
	// cache-freshness nicety: without it a stale entry resurrects the deleted row.
	let (store, _guard) = store_with_persistent();
	let kind = EntryKind::Source(STORAGE.into());
	let k = row_key(1);

	let persistent = store.persistent().unwrap();
	let table = classify_key(&k);
	persistent
		.set(CommitVersion(1), HashMap::from([(table, vec![(k.clone(), Some(versioned_row(b"old")))])]))
		.unwrap();

	// This read warms the read tier with the soon-to-be-stale value.
	let expected = versioned_row(b"old").to_vec();
	assert_eq!(
		get(&store, &k, 1),
		Some(expected),
		"the persistent row is readable before TTL deletion (and now cached)"
	);

	let deleted = persistent.delete_below_version(kind, CommitVersion(1), None, None, usize::MAX).unwrap().0;
	assert_eq!(deleted.len(), 1, "the expired row must be physically deleted from the persistent tier");

	// The staleness itself is deliberately not asserted; only the post-invalidation read is pinned.
	store.invalidate_read_key(&k);

	assert_eq!(
		get(&store, &k, 1),
		None,
		"after TTL deletion + read-tier invalidation, the expired row must read as NotFound"
	);

	store.clear_read();
	assert_eq!(get(&store, &k, 1), None, "clear_read must not resurrect the deleted row either");
}
