// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A store with the point and range caches on must answer every read exactly like a store
//! without them; the caches are read-through and never authoritative, so any divergence is
//! a cache serving stale or fabricated state.

use std::ops::Bound;

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator_state::{
		GroupId, Keyspace, OperatorStateKey, group_data_inner_range, keyspace_inner_range,
		keyspace_inner_range_upto,
	},
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	store::OperatorStore,
	tier::{point::OperatorPointConfig, range::OperatorRangeConfig},
};
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

const SEED: u64 = 0x9E3779B97F4A7C15;

const STEPS: u64 = 4000;

const OPERATORS: u64 = 2;

const GROUPS: u64 = 2;

const SUFFIXES: u64 = 160;

const KEYSPACES: [Keyspace; 5] = [
	Keyspace::ACCUMULATOR,
	Keyspace::JOIN_PUBLISHED,
	Keyspace::EXPIRY,
	Keyspace::CUSTOM_NOT_CACHED,
	Keyspace::TIMER_WHEEL,
];

const BATCHES: [u64; 4] = [2, 7, 64, 1024];

struct Rng(u64);

impl Rng {
	// xorshift keeps the workload deterministic without pulling in a rand dependency
	fn next(&mut self) -> u64 {
		let mut x = self.0;
		x ^= x << 13;
		x ^= x >> 7;
		x ^= x << 17;
		self.0 = x;
		x
	}

	fn below(&mut self, n: u64) -> u64 {
		self.next() % n
	}
}

fn store(cached: bool) -> (OperatorStore, SqliteTempPathGuard) {
	// a one-hour flush interval means rows reach sqlite only when the test flushes, keeping both stores in lockstep
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	let (config, guard) = SqliteConfig::in_memory();
	let store = OperatorStore::standard(OperatorStoreConfig {
		commit: Default::default(),
		persistent: Some(OperatorPersistentConfig::sqlite(config).flush_interval(Duration::from_hours_const(1))),
		// small shard budgets force evictions so the sampled-LRU and abort paths run, not just fills
		point: cached.then(|| OperatorPointConfig {
			resident_bytes: Some(ByteSize::from_bytes(128 * 1024)),
			shards: 4,
		}),
		range: cached.then(|| OperatorRangeConfig {
			resident_bytes: Some(ByteSize::from_bytes(128 * 1024)),
			shards: 4,
		}),
		spawner,
		clock: Clock::Real,
	});
	(store, guard)
}

fn key(rng: &mut Rng) -> (OperatorId, EncodedKey) {
	let operator = OperatorId(1 + rng.below(OPERATORS));
	let group = GroupId(1 + rng.below(GROUPS));
	let keyspace = KEYSPACES[rng.below(KEYSPACES.len() as u64) as usize];
	let suffix = rng.below(SUFFIXES);
	(operator, OperatorStateKey::inner_encoded(group, keyspace, suffix.to_be_bytes()).as_encoded().clone())
}

fn drain(
	store: &OperatorStore,
	operator: OperatorId,
	range: &EncodedKeyRange,
	batch: u64,
) -> Vec<(Vec<(EncodedKey, EncodedPodRow)>, bool)> {
	// pages continue from an excluded cursor exactly like the flow-side iterator does
	let mut pages = Vec::new();
	let mut current = range.clone();
	loop {
		let page = store.range_batch(operator, current.clone(), batch);
		let has_more = page.has_more;
		let last = page.items.last().map(|(key, _)| key.clone());
		pages.push((page.items, has_more));
		match (has_more, last) {
			(true, Some(last)) => current = EncodedKeyRange::new(Bound::Excluded(last), current.end),
			_ => return pages,
		}
	}
}

fn assert_same_range(
	cached: &OperatorStore,
	oracle: &OperatorStore,
	operator: OperatorId,
	range: &EncodedKeyRange,
	batch: u64,
	step: u64,
) {
	let lhs = drain(cached, operator, range, batch);
	let rhs = drain(oracle, operator, range, batch);
	assert_eq!(lhs, rhs, "range pages diverged at step {step} operator {} batch {batch}", operator.0);
}

fn random_range(rng: &mut Rng) -> EncodedKeyRange {
	let group = GroupId(1 + rng.below(GROUPS));
	let keyspace = KEYSPACES[rng.below(KEYSPACES.len() as u64) as usize];
	match rng.below(3) {
		0 => keyspace_inner_range(group, keyspace),
		1 => group_data_inner_range(group),
		_ => keyspace_inner_range_upto(group, keyspace, &rng.below(SUFFIXES).to_be_bytes()),
	}
}

fn sweep(cached: &OperatorStore, oracle: &OperatorStore, step: u64) {
	for operator in 1..=OPERATORS {
		let operator = OperatorId(operator);
		for group in 1..=GROUPS {
			let group = GroupId(group);
			for keyspace in KEYSPACES {
				for batch in [3, 1024] {
					let range = keyspace_inner_range(group, keyspace);
					assert_same_range(cached, oracle, operator, &range, batch, step);
				}
			}
			assert_same_range(cached, oracle, operator, &group_data_inner_range(group), 1024, step);
		}
	}
}

#[test]
fn cached_reads_equal_uncached_oracle_across_randomized_workload() {
	// interleaving reads with writes is the point: fills, invalidations and write-through all race the workload
	let (cached, _cached_guard) = store(true);
	let (oracle, _oracle_guard) = store(false);
	let mut rng = Rng(SEED);
	for step in 0..STEPS {
		match rng.below(100) {
			0..40 => {
				let (operator, key) = key(&mut rng);
				let row = EncodedPodRow::new(format!("{step}").as_bytes());
				cached.set(operator, key.clone(), row.clone());
				oracle.set(operator, key, row);
			}
			40..55 => {
				let (operator, key) = key(&mut rng);
				cached.remove(operator, &key);
				oracle.remove(operator, &key);
			}
			55..65 => {
				let (operator, key) = key(&mut rng);
				assert_eq!(
					cached.get(operator, &key),
					oracle.get(operator, &key),
					"point get diverged at step {step}"
				);
			}
			65..70 => {
				let (operator, key) = key(&mut rng);
				assert_eq!(
					cached.contains(operator, &key),
					oracle.contains(operator, &key),
					"contains diverged at step {step}"
				);
			}
			70..95 => {
				let operator = OperatorId(1 + rng.below(OPERATORS));
				let range = random_range(&mut rng);
				let batch = BATCHES[rng.below(BATCHES.len() as u64) as usize];
				assert_same_range(&cached, &oracle, operator, &range, batch, step);
			}
			_ => {
				assert_eq!(
					cached.flush_pending_blocking(),
					oracle.flush_pending_blocking(),
					"flush outcomes diverged at step {step}"
				);
			}
		}
		if step % 1000 == 999 {
			sweep(&cached, &oracle, step);
		}
	}
	assert!(cached.flush_pending_blocking(), "the workload must leave something to flush");
	assert!(oracle.flush_pending_blocking(), "the workload must leave something to flush");
	sweep(&cached, &oracle, STEPS);
}
