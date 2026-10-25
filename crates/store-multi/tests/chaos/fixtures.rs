// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! Store-configuration builders and the deterministic flush stand-in.

use std::sync::Arc;

use reifydb_codec::row::bytes::{EncodedBytes, SHAPE_HEADER_SIZE};
use reifydb_core::{common::CommitVersion, event::EventBus, lifecycle::watermark::EvictionWatermark};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_store_multi::{
	config::{MultiStoreConfig, PersistentConfig},
	store::StandardMultiStore,
	tier::{point::MultiPointConfig, range::MultiRangeConfig},
};
use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};

/// Commit buffer + SQLite persistent + read tiers on sync_only pools, so the timer-driven flush and
/// compaction actors never fire on their own and the run stays a pure function of the seed.
pub fn sync_persistent_store() -> (StandardMultiStore, impl Drop) {
	sync_persistent_store_with_tiers(MultiPointConfig::testing(), MultiRangeConfig::testing())
}

pub fn sync_persistent_store_with_tiers(
	point: MultiPointConfig,
	range: MultiRangeConfig,
) -> (StandardMultiStore, impl Drop) {
	let pools = Pools::new(PoolConfig::sync_only());
	let clock = Clock::testing();
	let actor_system = ActorSystem::new(pools, clock.clone());
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	let event_bus = EventBus::new(&spawner);
	let (persistent, guard) = PersistentConfig::sqlite_in_memory();
	let mut config = MultiStoreConfig::sqlite(persistent, spawner, clock, event_bus);
	config.point = Some(point);
	config.range = Some(range);
	let store = StandardMultiStore::new(config).unwrap();
	(store, guard)
}

/// One shard, so the budget is not split and a few kib really does evict; a roomy cache would make the
/// differential run agree for the wrong reason.
pub fn tiny_tiers(kib: u64) -> (MultiPointConfig, MultiRangeConfig) {
	(
		MultiPointConfig {
			shard_bytes: Some(ByteSize::from_kib(kib)),
			shards: 1,
		},
		MultiRangeConfig {
			shard_bytes: Some(ByteSize::from_kib(kib)),
			shards: 1,
			..MultiRangeConfig::testing()
		},
	)
}

struct FixedWatermark(CommitVersion);

impl EvictionWatermark for FixedWatermark {
	fn watermark(&self) -> CommitVersion {
		self.0
	}
}

pub fn flush(store: &StandardMultiStore, cutoff: CommitVersion) {
	store.set_eviction_watermark(Arc::new(FixedWatermark(cutoff)));
	store.flush_pending_blocking();
}

/// TTL eviction is version-anchored, so a key's age is controlled purely by the version it commits at
/// and the row body is opaque to eviction.
pub fn build_bytes(payload: &[u8]) -> EncodedBytes {
	let mut buf = vec![0u8; SHAPE_HEADER_SIZE + payload.len()];
	buf[SHAPE_HEADER_SIZE..].copy_from_slice(payload);
	EncodedBytes(CowVec::new(buf))
}
