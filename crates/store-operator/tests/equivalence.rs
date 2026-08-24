// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A store with the point and range caches on must answer every read exactly like a store
//! without them; the caches are read-through and never authoritative, so any divergence is
//! a cache serving stale or fabricated state.

use std::{collections::HashMap, ops::Bound};

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
	metrics::scan::ScanCounters,
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store::coverage::DEFAULT_GAP_GUARD;
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	store::OperatorStore,
	tier::{point::OperatorPointConfig, range::OperatorRangeConfig},
	types::{DurablePre, OperatorWrite},
};
use reifydb_value::{byte_size::ByteSize, value::duration::Duration};

const SEED: u64 = 0x9E3779B97F4A7C15;

const STEPS: u64 = 4000;

const OPERATORS: u64 = 2;

const GROUPS: u64 = 2;

const SUFFIXES: u64 = 160;

/// The subset the range tier is allowed to cache, which is what the read-cost gate may measure.
const CACHED_KEYSPACES: [Keyspace; 2] = [Keyspace::ACCUMULATOR, Keyspace::JOIN_PUBLISHED];

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
	store_with_range_budget(cached, 128 * 1024)
}

fn store_with_range_budget(cached: bool, range_bytes: u64) -> (OperatorStore, SqliteTempPathGuard) {
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
			resident_bytes: Some(ByteSize::from_bytes(range_bytes)),
			shards: 4,
			gap_guard: DEFAULT_GAP_GUARD,
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

/// Drains every cacheable keyspace of one store, ignoring the answers, and reports nothing.
///
/// Equality is asserted elsewhere; this exists purely so the persistent reads it costs can be
/// bracketed by a counter.
fn drain_cacheable(store: &OperatorStore) {
	for operator in 1..=OPERATORS {
		for group in 1..=GROUPS {
			for keyspace in CACHED_KEYSPACES {
				drain(store, OperatorId(operator), &keyspace_inner_range(GroupId(group), keyspace), 64);
			}
		}
	}
}

#[test]
fn a_warm_cache_reads_far_less_than_the_oracle_for_the_same_answers() {
	// A tier that answers correctly while still reaching sqlite is worthless, so reads are measured.
	let (cached, _cached_guard) = store_with_range_budget(true, 4 * 1024 * 1024);
	let (oracle, _oracle_guard) = store_with_range_budget(false, 4 * 1024 * 1024);
	let mut rng = Rng(SEED);
	let mut live: HashMap<(OperatorId, EncodedKey), ByteSize> = HashMap::new();

	for step in 0..STEPS {
		let (operator, key) = key(&mut rng);
		let row = EncodedPodRow::new(format!("{step}").as_bytes());
		let post_bytes = ByteSize::from_bytes(row.bytes().len() as u64);
		let write = match live.insert((operator, key.clone()), post_bytes) {
			Some(pre_value_bytes) => OperatorWrite::Replace {
				operator,
				key,
				pre_value_bytes,
				post: row,
			},
			None => OperatorWrite::Insert {
				operator,
				key,
				post: row,
			},
		};
		cached.apply_batch(&[write.clone()]);
		oracle.apply_batch(&[write]);
	}
	assert!(cached.flush_pending_blocking(), "the rows must reach sqlite, or nothing is read back from it");
	assert!(oracle.flush_pending_blocking(), "the rows must reach sqlite, or nothing is read back from it");

	drain_cacheable(&cached);

	let before_warm = ScanCounters::sample();
	drain_cacheable(&cached);
	let warm = before_warm.since();

	let before_cold = ScanCounters::sample();
	drain_cacheable(&oracle);
	let cold = before_cold.since();

	let tier = cached.range().expect("the cached fixture configures a range tier");
	let counters = tier.metrics();
	assert!(counters.installs > 0, "the workload must install at least one span, or there is no cache to measure");
	assert!(counters.hits > 0, "a warmed tier that never reports a hit answered nothing from RAM");
	assert_eq!(
		counters.installs_refused, 0,
		"the fixture budget must hold the working set, or this measures budget refusals and not coverage"
	);
	assert!(cold.fetched > 0, "the oracle must actually read sqlite, or the comparison below has no denominator");
	assert!(
		warm.fetched * 4 < cold.fetched,
		"a warmed range tier answered {} rows out of sqlite where the uncached oracle answered {}; a tier \
		 that serves the right answer while still reaching the store on every read costs memory and saves \
		 nothing, and equivalence alone can never fail it",
		warm.fetched,
		cold.fetched
	);
}

#[test]
fn cached_reads_equal_uncached_oracle_across_randomized_workload() {
	// interleaving reads with writes is the point: fills, invalidations and write-through all race the workload
	let (cached, _cached_guard) = store(true);
	let (oracle, _oracle_guard) = store(false);
	let mut rng = Rng(SEED);
	// A wrong pre-image claim drifts the census forever, so live value sizes are tracked here.
	let mut live: HashMap<(OperatorId, EncodedKey), ByteSize> = HashMap::new();
	for step in 0..STEPS {
		match rng.below(100) {
			0..40 => {
				let (operator, key) = key(&mut rng);
				let row = EncodedPodRow::new(format!("{step}").as_bytes());
				let post_bytes = ByteSize::from_bytes(row.bytes().len() as u64);
				let write = match live.insert((operator, key.clone()), post_bytes) {
					Some(pre_value_bytes) => OperatorWrite::Replace {
						operator,
						key,
						pre_value_bytes,
						post: row,
					},
					None => OperatorWrite::Insert {
						operator,
						key,
						post: row,
					},
				};
				cached.apply_batch(&[write.clone()]);
				oracle.apply_batch(&[write]);
			}
			40..55 => {
				let (operator, key) = key(&mut rng);
				let pre = match live.remove(&(operator, key.clone())) {
					Some(pre_value_bytes) => DurablePre::Present(pre_value_bytes),
					None => DurablePre::Absent,
				};
				let write = OperatorWrite::Remove {
					operator,
					key,
					pre,
				};
				cached.apply_batch(&[write.clone()]);
				oracle.apply_batch(&[write]);
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
