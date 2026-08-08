// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Store-configuration builders and the deterministic flush stand-in.

use std::collections::HashMap;

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::bytes::{EncodedBytes, SHAPE_HEADER_SIZE},
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

/// Commit buffer + SQLite persistent + read cache on sync_only pools, so the timer-driven flush and
/// compaction actors never fire on their own and the run stays a pure function of the seed.
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

/// Deterministic stand-in for the flush sweep, in its persist -> refresh-read -> evict order. The
/// read-cache insert is the flush echo that clears two-version previous slots, so omitting it would
/// leave the stand-in behaving differently from the actor it models.
pub fn flush(store: &StandardMultiStore, cutoff: CommitVersion) {
	let commit = store.commit();
	for kind in commit.list_all_entry_kinds().unwrap() {
		// The oracle assumes a complete flush, so a budgeted call would leave a tail and the
		// differential check would compare against a state the model never reaches.
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

/// TTL eviction is version-anchored, so a key's age is controlled purely by the version it commits at
/// and the row body is opaque to eviction.
pub fn build_bytes(payload: &[u8]) -> EncodedBytes {
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
	EncodedBytes(CowVec::new(buf))
}
