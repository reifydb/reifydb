// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! The resident key filter is authoritative for absence: once it is armed, a read that it rejects never
//! reaches sqlite. That makes every one of its answers a correctness claim rather than a hint, so any key
//! the store made durable has to be a key the filter still admits, for as long as that key is durable. A
//! false negative here is not a slow read, it is a durable row that reads as absent, and the layers above
//! have no way to tell the two apart.

use std::{
	collections::HashMap,
	path::Path,
	thread,
	time::{Duration, Instant},
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{GroupId, GroupStateKey, KeyspaceId},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::SqliteConfig;
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig, ResidentConfig},
	range::OperatorRangeConfig,
	resident::{Resident, ResidentLimits},
	store::OperatorStore,
	types::{BufferedState, OperatorWrite},
};
use reifydb_testing::{keyspace::state_key, tempdir::temp_dir};
use reifydb_value::util::hash::Hash128;

const OP: OperatorId = OperatorId(1);

const KEYS: u64 = 64;

fn group() -> GroupId {
	GroupId::hashed(Hash128(1))
}

/// A store whose resident tier holds almost nothing, so a flushed row is evicted rather than kept in memory
/// where the buffered read would answer for it and the filter would never be consulted.
fn evicting_store_at(path: &Path) -> OperatorStore {
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	OperatorStore::standard(OperatorStoreConfig {
		resident: ResidentConfig {
			storage: Resident::with_limits(ResidentLimits {
				entries: 2,
				..ResidentLimits::default()
			}),
			..Default::default()
		},
		persistent: Some(OperatorPersistentConfig::sqlite(SqliteConfig::new(path))),
		range: Some(OperatorRangeConfig::testing()),
		spawner,
		clock: Clock::Real,
	})
}

fn key(seed: u64) -> EncodedKey {
	state_key(group(), KeyspaceId::JOIN_LEFT, seed)
}

fn row(seed: u64) -> EncodedPodRow {
	EncodedPodRow::new(format!("v{seed}").as_bytes())
}

fn put(store: &OperatorStore, key: EncodedKey, row: EncodedPodRow) {
	store.apply_batch(&[OperatorWrite::Insert {
		operator: OP,
		key: GroupStateKey::bound_unchecked(key),
		post: row,
	}]);
}

/// Writes every key, makes it durable, then drops what it can from the resident tier and reports the keys
/// that actually left it. Those are the only keys whose reads have to consult the filter, so they are the
/// only keys a test may make a claim about.
fn seed_durable_and_evict(store: &OperatorStore, seeds: impl Iterator<Item = u64>) -> Vec<u64> {
	let seeds: Vec<u64> = seeds.collect();
	for seed in &seeds {
		put(store, key(*seed), row(*seed));
	}
	assert!(store.flush_pending_blocking(), "the keys have to be durable before eviction may drop them");
	store.resident().evict_to_capacity();

	let evicted: Vec<u64> = seeds
		.into_iter()
		.filter(|seed| matches!(store.resident().lookup_state(OP, &key(*seed)), BufferedState::Absent))
		.collect();
	assert!(
		!evicted.is_empty(),
		"nothing left the resident tier, so every read below would be answered from the buffer and the \
		 filter would never be asked; the test would pass without exercising anything"
	);
	evicted
}

fn wait_until_armed(store: &OperatorStore) {
	let deadline = Instant::now() + Duration::from_secs(30);
	while Instant::now() < deadline && !store.filter_metrics().enabled {
		thread::sleep(Duration::from_millis(10));
	}
	assert!(store.filter_metrics().enabled, "the filter must arm, otherwise this test proves nothing about it");
}

#[test]
fn a_key_flushed_after_the_filter_arms_is_still_readable_once_it_leaves_the_resident_tier() {
	// A fresh store arms its filter over empty sqlite, which is only sound while every later flush also
	// records its keys in that filter. If a flush writes to sqlite without telling the filter, the key stays
	// readable exactly as long as it stays resident, and turns into a silent None the moment it is evicted.
	temp_dir(|dir| {
		let store = evicting_store_at(dir);
		let evicted = seed_durable_and_evict(&store, 0..KEYS);

		for seed in evicted {
			assert_eq!(
				store.state_get(OP, &GroupStateKey::bound_unchecked(key(seed)))
					.unwrap()
					.as_ref()
					.map(|found| found.body().to_vec()),
				Some(row(seed).body().to_vec()),
				"key {seed} is in sqlite and is no longer resident, so the read has to reach sqlite; \
				 an armed filter that never learned about the flush rejects it instead and the row \
				 reads as absent while it is sitting on disk"
			);
		}
		Ok(())
	})
	.unwrap();
}

#[test]
fn every_read_entry_point_agrees_about_a_durable_key_that_left_the_resident_tier() {
	// get, get_many, contains and state_sizes each short circuit on the filter separately. They answer the
	// same question, so one of them healing while the others still reject is worse than all four failing:
	// a caller that probes with contains and then reads with get would see the key exist and come back empty.
	temp_dir(|dir| {
		let store = evicting_store_at(dir);
		let evicted = seed_durable_and_evict(&store, 0..KEYS);
		let keys: Vec<GroupStateKey> =
			evicted.iter().map(|seed| GroupStateKey::bound_unchecked(key(*seed))).collect();

		let mut batched: HashMap<GroupStateKey, EncodedPodRow> = HashMap::new();
		store.state_get_many(OP, &keys, &mut |key, row| {
			batched.insert(key, row);
			Ok(())
		})
		.unwrap();
		let probes: Vec<(OperatorId, GroupStateKey)> = keys.iter().map(|key| (OP, key.clone())).collect();
		let sizes = store.state_sizes(&probes).unwrap();

		for (index, seed) in evicted.iter().enumerate() {
			assert!(
				batched.contains_key(&keys[index]),
				"get_many dropped durable key {seed}; the batched read skips sqlite on the same \
				 filter answer the point read does"
			);
			assert!(
				store.contains(OP, &keys[index]).unwrap(),
				"contains denied durable key {seed}; a caller that gates a write on this decides \
				 to insert a row that is already there"
			);
			assert!(
				sizes[index].is_some(),
				"state_sizes reported no size for durable key {seed}; the eviction accounting sizes \
				 state through this path, so a zero here lets the tier believe it freed nothing"
			);
		}
		Ok(())
	})
	.unwrap();
}

#[test]
fn a_key_written_after_the_filter_was_rebuilt_from_sqlite_is_still_readable_once_it_is_evicted() {
	// Booting over populated state rebuilds the filter from what sqlite held at the time of the scan. That
	// snapshot goes stale the instant the store accepts another write, so a key flushed after the rebuild is
	// covered only if the flush itself feeds the filter. Without that, every restart is followed by a growing
	// set of durable keys the filter denies.
	temp_dir(|dir| {
		let seeded = evicting_store_at(dir);
		seed_durable_and_evict(&seeded, 0..KEYS);
		drop(seeded);

		let booted = evicting_store_at(dir);
		wait_until_armed(&booted);

		let evicted = seed_durable_and_evict(&booted, KEYS..(KEYS * 2));
		for seed in evicted {
			assert_eq!(
				booted.state_get(OP, &GroupStateKey::bound_unchecked(key(seed)))
					.unwrap()
					.as_ref()
					.map(|found| found.body().to_vec()),
				Some(row(seed).body().to_vec()),
				"key {seed} was flushed after the rebuild scan and then evicted; a filter that only \
				 ever learns keys from a rebuild cannot know about it, so the row is durable and \
				 unreadable until the next rebuild happens to sweep it up"
			);
		}
		Ok(())
	})
	.unwrap();
}
