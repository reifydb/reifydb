// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

//! Store-configuration builders and the deterministic flush stand-in.

use std::{collections::HashMap, time::Duration};

use reifydb_core::{
	common::CommitVersion,
	encoded::{
		key::EncodedKey,
		row::{EncodedRow, SHAPE_HEADER_SIZE},
	},
	event::EventBus,
	interface::store::EntryKind,
};
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
use reifydb_value::util::cowvec::CowVec;

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
	let persistent = persistent.flush_interval(Duration::from_secs(86_400));
	let store = StandardMultiStore::new(MultiStoreConfig::sqlite(persistent, spawner, clock, event_bus)).unwrap();
	(store, guard)
}

/// Deterministic stand-in for the FlushActor sweep (same persist -> invalidate-read -> drop ordering as
/// `tests/range_cache.rs`): move the latest-<=cutoff value of every key into the persistent tier, clear
/// those keys from the read cache, then drop them from the commit buffer.
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
			for (key, version, value) in to_persist {
				by_version.entry(version).or_default().entry(kind).or_default().push((key, value));
			}
			for (version, batch) in by_version {
				persistent.set(version, batch).unwrap();
			}
		}
		for (key, _) in &to_drop {
			store.invalidate_read_key(key);
		}
		commit.drop(HashMap::from([(kind, to_drop)])).unwrap();
	}
}

/// Build a row whose shape header carries the TTL anchors (created/updated nanos at offsets 8..16 / 16..24),
/// matching `tests/gc_ttl.rs::build_row`. The TTL scanner and the persistent tier read these to decide
/// expiry, so the test controls a key's age by what it writes here.
pub fn build_row(payload: &[u8], created_nanos: u64, updated_nanos: u64) -> EncodedRow {
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[8..16].copy_from_slice(&created_nanos.to_le_bytes());
	buf[16..24].copy_from_slice(&updated_nanos.to_le_bytes());
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
	EncodedRow(CowVec::new(buf))
}
