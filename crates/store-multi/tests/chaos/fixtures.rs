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
/// flush/drop actors never fire on their own (the large flush_interval is extra insurance on top). The
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

/// Deterministic stand-in for the FlushActor sweep, mirroring its persist -> refresh-read -> drop
/// ordering: move the latest-<=cutoff value of every key into the persistent tier, insert those
/// flushed `(key, version, value)` entries into the read cache exactly like
/// `FlushActor::refresh_read_tier` does (the flush echo that clears two-version previous slots),
/// then drop the evicted versions from the commit buffer.
pub fn flush(store: &StandardMultiStore, cutoff: CommitVersion) {
	let commit = store.commit().expect("commit tier configured");
	for kind in commit.list_all_entry_kinds().unwrap() {
		let (to_persist, to_drop) = match commit {
			MultiCommitBufferTier::Memory(s) => s.collect_evictable_below(kind, cutoff),
		};
		if to_drop.is_empty() {
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
		commit.drop(HashMap::from([(kind, to_drop)])).unwrap();
	}
}

/// Deterministic stand-in for the drop actor's purge cadence: settle every pending drop by deleting
/// the masked persisted rows (bounded by each drop's commit version), cleaning the read cache, and
/// clearing the overlay. Chaos runs use sync_only pools, so without this pump the purge never
/// happens and the PendingDrops mask must carry correctness alone.
pub fn pump_pending_drops(store: &StandardMultiStore) {
	store.purge_pending_drops();
}

/// A store over an explicit SQLite directory that survives store teardown, so a chaos workload
/// can simulate a process restart by shutting the store down and rebuilding over the same file.
/// The directory is removed when the returned guard drops.
pub struct RestartDir {
	pub path: std::path::PathBuf,
}

impl Drop for RestartDir {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.path);
	}
}

pub fn restart_dir(seed: u64) -> RestartDir {
	let path = std::env::temp_dir().join(format!("reifydb-restart-chaos-{}-{}", std::process::id(), seed));
	std::fs::create_dir_all(&path).unwrap();
	RestartDir {
		path,
	}
}

pub fn persistent_store_at(dir: &RestartDir) -> StandardMultiStore {
	let pools = Pools::new(PoolConfig::sync_only());
	let clock = Clock::testing();
	let actor_system = ActorSystem::new(pools, clock.clone());
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	let event_bus = EventBus::new(&spawner);
	let sqlite = reifydb_sqlite::SqliteConfig::new(&dir.path);
	let persistent = PersistentConfig::sqlite(sqlite).flush_interval(Duration::from_seconds(86_400).unwrap());
	StandardMultiStore::new(MultiStoreConfig::sqlite(persistent, spawner, clock, event_bus)).unwrap()
}

/// Build a row carrying `payload`. TTL eviction is version-anchored now (it keys off each row's commit
/// version, read from the store, not any header timestamp), so the test controls a key's age purely by
/// the version it commits at - the row body is opaque to eviction.
pub fn build_row(payload: &[u8]) -> EncodedRow {
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
	EncodedRow(CowVec::new(buf))
}
