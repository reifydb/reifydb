// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Store-configuration builders and the deterministic flush stand-in.

use std::collections::HashMap;

use reifydb_codec::{
	encoded::row::{EncodedRow, SHAPE_HEADER_SIZE},
	key::encoded::EncodedKey,
};
use reifydb_core::{common::CommitVersion, event::EventBus, interface::store::EntryKind};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_multi::{
	config::{MultiStoreConfig, PersistentConfig},
	store::StandardMultiStore,
	tier::{TierStorage, commit::buffer::MultiCommitBufferTier},
};
use reifydb_value::{util::cowvec::CowVec, value::duration::Duration};

/// commit buffer + SQLite persistent + read cache, built with sync_only pools so the timer-driven
/// flush/compaction actors never fire on their own (the large flush_interval is extra insurance on top). The
/// SQLite temp-path guard is returned as `impl Drop` so the test never has to name the guard type.
pub fn sync_persistent_store() -> (StandardMultiStore, impl Drop) {
	let pools = Pools::new(PoolConfig::sync_only());
	let clock = Clock::testing();
	let actor_system = ActorSystem::new(pools, clock.clone());
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	let event_bus = EventBus::new(&spawner);
	let (persistent, guard) = PersistentConfig::sqlite_in_memory();
	let persistent = persistent.flush_interval(Duration::from_seconds(86_400).unwrap());
	let store = StandardMultiStore::new(MultiStoreConfig::sqlite(persistent, spawner, clock, event_bus)).unwrap();
	(store, guard)
}

/// Deterministic stand-in for the FlushActor sweep, mirroring its persist -> refresh-read -> evict
/// ordering: move the latest-<=cutoff value of every key into the persistent tier, insert those
/// flushed `(key, version, value)` entries into the read cache exactly like
/// `FlushActor::refresh_read_tier` does (the flush echo that clears two-version previous slots),
/// then evict the flushed versions from the commit buffer.
pub fn flush(store: &StandardMultiStore, cutoff: CommitVersion) {
	let commit = store.commit();
	for kind in commit.list_all_entry_kinds().unwrap() {
		// Unbounded budget: this stand-in must move every key below the cutoff in one pass, so the
		// oracle can assume a complete flush. A budgeted call would leave a tail behind and the
		// differential check would see a partially-flushed store rather than the state it models.
		let (to_persist, to_compact, more) = match commit {
			MultiCommitBufferTier::Memory(s) => s.collect_evictable_below(kind, cutoff, usize::MAX),
		};
		assert!(!more, "an unbounded collect must never report a remaining tail");
		if to_compact.is_empty() {
			continue;
		}
		if !to_persist.is_empty() {
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
			for (key, version, value) in to_persist {
				store.insert_read_key(key, version, value);
			}
		}
		let to_compact: Vec<(EncodedKey, CommitVersion)> =
			to_compact.into_iter().map(|evicted| (evicted.key, evicted.version)).collect();
		commit.compact(HashMap::from([(kind, to_compact)])).unwrap();
	}
}

/// Build a row carrying `payload`. TTL eviction is version-anchored now (it keys off each row's commit
/// version, read from the store, not any header timestamp), so the test controls a key's age purely by
/// the version it commits at - the row body is opaque to eviction.
pub fn build_row(payload: &[u8]) -> EncodedRow {
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
	EncodedRow(CowVec::new(buf))
}
