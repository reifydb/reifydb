// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

//! A store with the point and range caches on must answer every read exactly like a store
//! without them; the caches are read-through and never authoritative, so any divergence is
//! a cache serving stale or fabricated state.

use std::{
	collections::{HashMap, HashSet},
	ops::Bound,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, EncodedKeyRange},
	row::pod::EncodedPodRow,
};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::operator::state::{
		GroupId, GroupStateKey, KeyspaceId, KeyspaceMask, OperatorStateKey, group_data_inner_range,
		keyspace_inner_range, keyspace_inner_range_upto,
	},
	metrics::scan::ScanCounters,
	state::timer::StateStore,
};
use reifydb_runtime::{
	actor::system::ActorSystem,
	context::clock::Clock,
	pool::{PoolConfig, Pools},
};
use reifydb_sqlite::{SqliteConfig, SqliteTempPathGuard};
use reifydb_store::coverage::plan::DEFAULT_GAP_GUARD;
use reifydb_store_operator::{
	config::{OperatorPersistentConfig, OperatorStoreConfig},
	store::OperatorStore,
	tier::{point::OperatorPointConfig, range::OperatorRangeConfig},
	types::{DurablePre, OperatorWrite},
};
use reifydb_testing::keyspace::state_key;
use reifydb_value::{
	Result,
	byte_size::ByteSize,
	value::{datetime::DateTime, row_number::RowNumber},
};

const CACHED: KeyspaceId = KeyspaceId::JOIN_LEFT;

const SEED: u64 = 0x9E3779B97F4A7C15;

const STEPS: u64 = 4000;

const OPERATORS: u64 = 2;

const GROUPS: u64 = 2;

const SUFFIXES: u64 = 160;

/// The subset the range tier is allowed to cache, which is what the read-cost gate may measure.
const CACHED_KEYSPACES: [KeyspaceId; 2] = [CACHED, KeyspaceId::JOIN_PUBLISHED];

const KEYSPACES: [KeyspaceId; 5] = [
	CACHED,
	KeyspaceId::JOIN_PUBLISHED,
	KeyspaceId::GUEST_ACCUMULATOR,
	KeyspaceId::CUSTOM_NOT_CACHED,
	KeyspaceId::EMIT,
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

const TIGHT_RANGE_BUDGET: u64 = 128 * 1024;

const BUDGET_ABOVE_WORKING_SET: u64 = 4 * 1024 * 1024;

fn store(cached: bool) -> (OperatorStore, SqliteTempPathGuard) {
	store_with_range_budget(cached, TIGHT_RANGE_BUDGET)
}

fn store_with_range_budget(cached: bool, range_bytes: u64) -> (OperatorStore, SqliteTempPathGuard) {
	// a one-hour flush interval means rows reach sqlite only when the test flushes, keeping both stores in lockstep
	let pools = Pools::new(PoolConfig::default());
	let actor_system = ActorSystem::new(pools, Clock::Real);
	let spawner = actor_system.spawner();
	std::mem::forget(actor_system);
	let (config, guard) = SqliteConfig::in_memory();
	let store = OperatorStore::standard(OperatorStoreConfig {
		resident: Default::default(),
		persistent: Some(OperatorPersistentConfig::sqlite(config)),
		// small tier budgets force evictions so the sampled-LRU and abort paths run, not just fills
		point: cached.then(|| OperatorPointConfig {
			tier_bytes: Some(ByteSize::from_bytes(128 * 1024)),
		}),
		range: cached.then(|| OperatorRangeConfig {
			tier_bytes: Some(ByteSize::from_bytes(range_bytes)),
			gap_guard: DEFAULT_GAP_GUARD,
		}),
		spawner,
		clock: Clock::Real,
	});
	(store, guard)
}

fn key(rng: &mut Rng) -> (OperatorId, EncodedKey) {
	let operator = OperatorId(1 + rng.below(OPERATORS));
	let group = GroupId((1 + rng.below(GROUPS)) as u128);
	let keyspace = KEYSPACES[rng.below(KEYSPACES.len() as u64) as usize];
	let suffix = rng.below(SUFFIXES);
	(operator, state_key(group, keyspace, suffix as u64))
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
	let group = GroupId((1 + rng.below(GROUPS)) as u128);
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
			let group = GroupId(group as u128);
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
				drain(
					store,
					OperatorId(operator),
					&keyspace_inner_range(GroupId(group as u128), keyspace),
					64,
				);
			}
		}
	}
}

#[test]
fn a_warm_cache_reads_far_less_than_the_oracle_for_the_same_answers() {
	// A tier that answers correctly while still reaching sqlite is worthless, so reads are measured.
	let (cached, _cached_guard) = store_with_range_budget(true, BUDGET_ABOVE_WORKING_SET);
	let (oracle, _oracle_guard) = store_with_range_budget(false, BUDGET_ABOVE_WORKING_SET);
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
	assert!(
		counters.materializes > 0,
		"the workload must materialize at least one span, or there is no cache to measure"
	);
	assert!(counters.hits > 0, "a warmed tier that never reports a hit answered nothing from RAM");
	assert_eq!(
		counters.materializes_refused, 0,
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

#[test]
fn get_many_answers_exactly_as_repeated_get_across_randomized_workload() {
	// get_many resolves the commit, point and range tiers itself and batches only the sqlite tail, so it
	// duplicates the whole tier walk that get performs; a divergence means the batched path invented a
	// row, missed a tombstone, or mismatched a result to the wrong slot.
	let (store, _guard) = store(true);
	let mut rng = Rng(SEED);
	let mut live: HashMap<(OperatorId, EncodedKey), ByteSize> = HashMap::new();
	let mut compared = 0u64;
	let mut seen_present = false;
	let mut seen_absent = false;

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
				store.apply_batch(&[write]);
			}
			40..55 => {
				let (operator, key) = key(&mut rng);
				let pre = match live.remove(&(operator, key.clone())) {
					Some(pre_value_bytes) => DurablePre::Present(pre_value_bytes),
					None => DurablePre::Absent,
				};
				store.apply_batch(&[OperatorWrite::Remove {
					operator,
					key,
					pre,
				}]);
			}
			55..95 => {
				let operator = OperatorId(1 + rng.below(OPERATORS));
				// duplicates in one batch must each resolve, so the same key is allowed to repeat
				let batch: Vec<EncodedKey> = (0..1 + rng.below(12)).map(|_| key(&mut rng).1).collect();
				let batched = store.get_many(operator, &batch);
				assert_eq!(
					batched.len(),
					batch.len(),
					"get_many must answer every slot at step {step}"
				);
				for (slot, key) in batch.iter().enumerate() {
					let single = store.get(operator, key);
					assert_eq!(
						batched[slot], single,
						"get_many slot {slot} diverged from get at step {step}"
					);
					// get runs second and would echo a tier get_many corrupted, so live must
					// witness it
					assert_eq!(
						batched[slot].is_some(),
						live.contains_key(&(operator, key.clone())),
						"get_many slot {slot} disagreed with the tracked live set at step {step}"
					);
					match single {
						Some(_) => seen_present = true,
						None => seen_absent = true,
					}
				}
				compared += batch.len() as u64;
			}
			_ => {
				store.flush_pending_blocking();
			}
		}
	}

	assert!(compared > 0, "the workload must actually compare batched reads against single reads");
	assert!(seen_present, "a run that never resolves a live row would pass with get_many always returning none");
	assert!(seen_absent, "a run that never resolves a missing row would not exercise tombstones or filter misses");
}

const SWEPT: OperatorId = OperatorId(1);

const SCOPED: GroupId = GroupId(9);

const NEIGHBOUR: GroupId = GroupId(11);

/// Group-scoped data keyspaces whose suffix is a single direction-wrapped column.
const BARE_SUFFIX_DATA: [KeyspaceId; 2] = [KeyspaceId::GUEST_ACCUMULATOR, KeyspaceId::EMIT];

/// Group-scoped data keyspaces whose suffix is a struct of its own, decoded column by column.
const NAMED_SUFFIX_DATA: [KeyspaceId; 2] = [KeyspaceId::WINDOW_META, KeyspaceId::TUMBLING_EXPIRY];

/// Identity keyspaces that hold root-scoped rows in every live store.
const ROOT_LAYOUT_IDENTITY: [KeyspaceId; 2] = [KeyspaceId::TIMER_WHEEL, KeyspaceId::NODE_COUNTER];

const SWEEP_PAGE: usize = 1024;

struct StoreState {
	store: OperatorStore,
	operator: OperatorId,
	live: HashMap<EncodedKey, ByteSize>,
}

impl StoreState {
	fn new(store: OperatorStore, operator: OperatorId) -> Self {
		Self {
			store,
			operator,
			live: HashMap::new(),
		}
	}

	fn seed(&mut self, group: GroupId, keyspace: KeyspaceId, seed: u64) {
		let key = GroupStateKey::bound_unchecked(state_key(group, keyspace, seed));
		self.state_set(&key, EncodedPodRow::new(format!("{}:{seed}", group.0).as_bytes()))
			.expect("a seeded write must reach the store");
	}

	fn flush(&mut self) {
		self.store.flush_pending_blocking();
	}
}

impl StateStore for StoreState {
	fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
		Ok(self.store.get(self.operator, key.as_encoded()))
	}

	fn state_get_many_visit(
		&mut self,
		keys: &[GroupStateKey],
		visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
	) -> Result<()> {
		for key in keys {
			if let Some(row) = self.store.get(self.operator, key.as_encoded()) {
				visit(key.clone(), row)?;
			}
		}
		Ok(())
	}

	fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
		let post_bytes = ByteSize::from_bytes(payload.bytes().len() as u64);
		let encoded = key.as_encoded().clone();
		let write = match self.live.insert(encoded.clone(), post_bytes) {
			Some(pre_value_bytes) => OperatorWrite::Replace {
				operator: self.operator,
				key: encoded,
				pre_value_bytes,
				post: payload,
			},
			None => OperatorWrite::Insert {
				operator: self.operator,
				key: encoded,
				post: payload,
			},
		};
		self.store.apply_batch(&[write]);
		Ok(())
	}

	fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
		let encoded = key.as_encoded().clone();
		let pre = match self.live.remove(&encoded) {
			Some(pre_value_bytes) => DurablePre::Present(pre_value_bytes),
			None => DurablePre::Absent,
		};
		self.store.apply_batch(&[OperatorWrite::Remove {
			operator: self.operator,
			key: encoded,
			pre,
		}]);
		Ok(())
	}

	fn state_page_inner(
		&mut self,
		range: EncodedKeyRange,
		limit: Option<usize>,
	) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
		// a scan that stops on its budget answers with no rows at all, so a cursor taken only from the
		// last row would end the walk here and silently drop everything past the point it stopped
		let mut out = Vec::new();
		let mut current = range;
		let batch = limit.map_or(SWEEP_PAGE, |limit| limit.saturating_add(1).min(SWEEP_PAGE));
		loop {
			if limit.is_some_and(|limit| out.len() >= limit) {
				return Ok(out);
			}
			let page = self.store.range_batch(self.operator, current.clone(), batch as u64);
			let has_more = page.has_more;
			let cursor = page.resume.clone().or_else(|| page.items.last().map(|(key, _)| key.clone()));
			for (key, row) in page.items {
				if limit.is_some_and(|limit| out.len() >= limit) {
					return Ok(out);
				}
				out.push((GroupStateKey::bound_unchecked(key), row));
			}
			match (has_more, cursor) {
				(true, Some(cursor)) => {
					current = EncodedKeyRange::new(Bound::Excluded(cursor), current.end)
				}
				_ => return Ok(out),
			}
		}
	}

	fn get_or_create_row_numbers(
		&mut self,
		_group: GroupId,
		_keys: &[EncodedKey],
	) -> Result<Vec<(RowNumber, bool)>> {
		panic!("the sweep path never mints a row number; a call here means the fixture drifted off it")
	}

	fn get_or_create_row_numbers_for_groups(&mut self, _groups: &[GroupId]) -> Result<Vec<(RowNumber, bool)>> {
		panic!("the sweep path never mints a row number; a call here means the fixture drifted off it")
	}

	fn remove_row_number(&mut self, _group: GroupId, _key: &EncodedKey) -> Result<()> {
		panic!("the sweep path never drops a row number; a call here means the fixture drifted off it")
	}

	fn remove_row_number_for_group(&mut self, _group: GroupId) -> Result<()> {
		panic!("the sweep path never drops a row number; a call here means the fixture drifted off it")
	}

	fn written_at(&self) -> DateTime {
		DateTime::default()
	}
}

fn state(cached: bool) -> (StoreState, SqliteTempPathGuard) {
	let (store, guard) = store(cached);
	(StoreState::new(store, SWEPT), guard)
}

fn group_of(key: &GroupStateKey) -> GroupId {
	OperatorStateKey::decode_inner(key.as_encoded().as_slice())
		.expect("a key the store answered with must carry a decodable inner encoding")
		.0
}

fn keyspace_of(key: &GroupStateKey) -> KeyspaceId {
	OperatorStateKey::decode_inner(key.as_encoded().as_slice())
		.expect("a key the store answered with must carry a decodable inner encoding")
		.1
}

fn bytes_of(keys: &[GroupStateKey]) -> HashSet<Vec<u8>> {
	keys.iter().map(|key| key.as_encoded().as_slice().to_vec()).collect()
}

fn populate(state: &mut StoreState, group: GroupId, keyspaces: &[KeyspaceId], rows: u64) {
	for keyspace in keyspaces {
		for seed in 0..rows {
			state.seed(group, *keyspace, seed);
		}
	}
}

fn populate_reaper_shaped(state: &mut StoreState) {
	// a live store always holds root-scoped identity rows next to the group's own rows, and the two must
	// stay tellable apart or a sweep of one group answers with the other group's state
	populate(state, GroupId::ROOT, &ROOT_LAYOUT_IDENTITY, 4);
	populate(state, GroupId::ROOT, &NAMED_SUFFIX_DATA, 4);
	populate(state, SCOPED, &BARE_SUFFIX_DATA, 4);
	populate(state, SCOPED, &NAMED_SUFFIX_DATA, 4);
	state.seed(SCOPED, KeyspaceId::GROUP_ROW_MAPPING, 0);
	populate(state, NEIGHBOUR, &BARE_SUFFIX_DATA, 4);
	populate(state, NEIGHBOUR, &NAMED_SUFFIX_DATA, 4);
	state.flush();
}

#[test]
fn a_keyspace_scan_answers_only_with_keys_that_carry_the_group_it_asked_for() {
	// the range asked for is one group's slice of one keyspace, so a key answered under another group is a
	// row the caller never asked about and cannot address: removing it removes the wrong group's state.
	// a keyspace whose layout carries no group holds exactly one partition, at root, so the only truthful
	// answer to a non-root ask is nothing at all: a row here is another partition's, handed to a caller
	// that will reap it
	let (mut state, _guard) = state(false);
	populate_reaper_shaped(&mut state);

	for keyspace in NAMED_SUFFIX_DATA.iter().chain(&BARE_SUFFIX_DATA) {
		let page = state
			.state_page_inner(keyspace_inner_range(SCOPED, *keyspace), None)
			.expect("a keyspace scan must answer");
		assert!(!page.is_empty(), "{} holds seeded rows for the scoped group", keyspace.name());
		for (key, _) in &page {
			assert_eq!(
				group_of(key),
				SCOPED,
				"a scan of {} under group {} answered with a key under group {}",
				keyspace.name(),
				SCOPED.0,
				group_of(key).0
			);
		}
	}

	for keyspace in &ROOT_LAYOUT_IDENTITY {
		let page = state
			.state_page_inner(keyspace_inner_range(SCOPED, *keyspace), None)
			.expect("a keyspace scan must answer");
		assert!(
			page.is_empty(),
			"{} holds its one partition at root, so a scan of it under group {} must answer with \
			 nothing, not {} row(s)",
			keyspace.name(),
			SCOPED.0,
			page.len()
		);
	}
}

#[test]
fn a_group_sweep_answers_only_with_keys_that_carry_the_group_it_asked_for() {
	// the reaper reaps every key a sweep hands back and then declares the group gone; a key under another
	// group is reaped from a partition nobody asked to drop while the group's own rows survive unaddressed
	let (mut state, _guard) = state(false);
	populate_reaper_shaped(&mut state);

	for data_only in [false, true] {
		for limit in [None, Some(256)] {
			let keys = state
				.group_sweep_in(SCOPED, KeyspaceMask::KNOWN, data_only, limit)
				.expect("a sweep must answer");
			assert!(!keys.is_empty(), "the scoped group holds seeded rows, so no sweep of it is empty");
			for key in &keys {
				assert_eq!(
					group_of(key),
					SCOPED,
					"a sweep of group {} with data_only={data_only} limit={limit:?} answered with a \
					 key under group {} in {}",
					SCOPED.0,
					group_of(key).0,
					keyspace_of(key).name()
				);
			}
		}
	}
}

#[test]
fn a_data_only_sweep_is_a_subset_of_a_full_sweep() {
	// the two calls differ by one filter clause, so a data row the narrow sweep sees and the wide one misses
	// is a row the reaper never reaps and never counts against its budget
	let (mut state, _guard) = state(false);
	populate_reaper_shaped(&mut state);

	let full = state.group_sweep_in(SCOPED, KeyspaceMask::KNOWN, false, None).expect("a sweep must answer");
	let data = state.group_sweep_in(SCOPED, KeyspaceMask::KNOWN, true, None).expect("a sweep must answer");

	assert!(!data.is_empty(), "the scoped group holds seeded data rows, so the narrow sweep is not empty");
	let wide = bytes_of(&full);
	for key in &data {
		assert!(
			wide.contains(key.as_encoded().as_slice()),
			"a data-only sweep of group {} returned a key in {} the full sweep of the same group never \
			 returned",
			SCOPED.0,
			keyspace_of(key).name()
		);
	}
}

#[test]
fn a_full_sweep_under_its_budget_holds_every_row_a_data_only_sweep_finds() {
	// a returned length at or under the budget is the only proof the reaper has that the sweep saw the whole
	// group, and it reaps on that proof alone; a short answer inside the budget reads exactly like a complete
	// one and leaves the rows it missed behind forever
	const BUDGET: usize = 256;
	let (mut state, _guard) = state(false);
	populate_reaper_shaped(&mut state);

	let scanned = state
		.group_sweep_in(SCOPED, KeyspaceMask::KNOWN, false, Some(BUDGET + 1))
		.expect("a sweep must answer");
	assert!(scanned.len() <= BUDGET, "the fixture must stay under the budget, or this proves nothing");

	let data = state.group_sweep_in(SCOPED, KeyspaceMask::KNOWN, true, None).expect("a sweep must answer");
	assert!(!data.is_empty(), "the scoped group holds seeded data rows, so the narrow sweep is not empty");

	let complete = bytes_of(&scanned);
	for key in &data {
		assert!(
			complete.contains(key.as_encoded().as_slice()),
			"a full sweep of group {} that stopped under its budget missed a data row in {}, so the \
			 reaper reads it as complete and reaps only part of the group",
			SCOPED.0,
			keyspace_of(key).name()
		);
	}
}

#[test]
fn reaping_every_data_key_a_full_sweep_returned_empties_the_data_only_sweep() {
	// this is the drain the reaper runs: scan wide, split by is_data, reap the data half, then prove the
	// group holds no data rows before forgetting the group id that addresses them
	const BUDGET: usize = 256;
	let (mut state, _guard) = state(false);
	populate_reaper_shaped(&mut state);

	let scanned = state
		.group_sweep_in(SCOPED, KeyspaceMask::KNOWN, false, Some(BUDGET + 1))
		.expect("a sweep must answer");
	assert!(scanned.len() <= BUDGET, "the fixture must stay under the budget, or the reaper takes another path");
	let reaped: Vec<GroupStateKey> = scanned.into_iter().filter(|key| keyspace_of(key).is_data()).collect();
	assert!(!reaped.is_empty(), "a drain that reaps nothing would pass without exercising the recheck");
	for key in &reaped {
		state.state_remove(key).expect("a reap must reach the store");
	}

	let leftover = state.group_sweep_in(SCOPED, KeyspaceMask::KNOWN, true, None).expect("a sweep must answer");
	assert!(
		leftover.is_empty(),
		"group {} still holds {} data rows after every data key its own scan returned was reaped; \
		 forgetting the group now orphans them behind an id nothing can resolve again",
		SCOPED.0,
		leftover.len()
	);
}

#[test]
fn a_full_sweep_overruns_its_budget_only_when_the_group_holds_more_keys_than_the_budget() {
	// the reaper reads len() > budget as truncation and anything at or under it as a complete group, so both
	// edges of that comparison decide whether a whole group is reaped or handed to the slower path
	const BUDGET: usize = 8;
	for held in [BUDGET - 1, BUDGET, BUDGET + 1, BUDGET + 2] {
		let (mut state, _guard) = state(false);
		for seed in 0..held as u64 {
			state.seed(SCOPED, KeyspaceId::GUEST_ACCUMULATOR, seed);
		}
		state.flush();

		let keys = state
			.group_sweep_in(SCOPED, KeyspaceMask::KNOWN, false, Some(BUDGET + 1))
			.expect("a sweep must answer");

		assert_eq!(
			keys.len(),
			held.min(BUDGET + 1),
			"a sweep of a group holding {held} keys under a limit of {} answered {} of them",
			BUDGET + 1,
			keys.len()
		);
		assert_eq!(
			keys.len() > BUDGET,
			held > BUDGET,
			"a group holding {held} keys must report truncation to the reaper exactly when it holds \
			 more than the budget of {BUDGET}"
		);
	}
}

#[test]
fn a_flush_does_not_change_which_keys_a_group_sweep_answers_with() {
	// a flush moves rows from the write buffer to sqlite and changes nothing about which rows exist, so two
	// sweeps that straddle it must name the same keys; a key that changes shape across the flush is one the
	// reaper can read from one sweep and fail to remove with the other
	let (mut state, _guard) = state(false);
	populate(&mut state, SCOPED, &NAMED_SUFFIX_DATA, 4);
	populate(&mut state, SCOPED, &BARE_SUFFIX_DATA, 4);

	let buffered = state.group_sweep_in(SCOPED, KeyspaceMask::KNOWN, true, None).expect("a sweep must answer");
	state.flush();
	let stored = state.group_sweep_in(SCOPED, KeyspaceMask::KNOWN, true, None).expect("a sweep must answer");

	assert!(!buffered.is_empty(), "the group holds seeded data rows, so neither sweep is empty");
	let after = bytes_of(&stored);
	let renamed: Vec<&GroupStateKey> =
		buffered.iter().filter(|key| !after.contains(key.as_encoded().as_slice())).collect();
	assert!(
		renamed.is_empty(),
		"a flush that changed no row dropped {} of the {} keys a sweep of group {} named; the first sits in \
		 {} and the sweep now names it under group {}",
		renamed.len(),
		buffered.len(),
		SCOPED.0,
		keyspace_of(renamed[0]).name(),
		stored.iter()
			.find(|key| keyspace_of(key) == keyspace_of(renamed[0]))
			.map(|key| group_of(key).0)
			.unwrap_or_default()
	);
	assert_eq!(
		buffered.len(),
		stored.len(),
		"a flush that changed no row changed how many keys a sweep of group {} names",
		SCOPED.0
	);
}

#[test]
fn a_keyspace_scan_never_answers_with_a_row_another_group_wrote() {
	// two groups writing the same keyspace hold separate state, and a scan of one that hands back the other
	// group's rows lets a reaper count, reap and report state that belongs to a group still running
	for cached in [false, true] {
		let (mut state, _guard) = state(cached);
		for seed in 0..4u64 {
			state.seed(SCOPED, KeyspaceId::WINDOW_META, seed);
			state.seed(NEIGHBOUR, KeyspaceId::WINDOW_META, 100 + seed);
		}
		state.flush();

		let page = state
			.state_page_inner(keyspace_inner_range(SCOPED, KeyspaceId::WINDOW_META), None)
			.expect("a keyspace scan must answer");

		let owners: Vec<String> = page
			.iter()
			.map(|(_, row)| String::from_utf8(row.body().to_vec()).expect("seeded bodies are utf8"))
			.filter(|body| !body.starts_with(&format!("{}:", SCOPED.0)))
			.collect();
		assert!(
			owners.is_empty(),
			"a cached={cached} scan of group {} answered with {} row(s) another group wrote, the first \
			 being {}",
			SCOPED.0,
			owners.len(),
			owners[0]
		);
		assert_eq!(
			page.len(),
			4,
			"a cached={cached} scan of group {} must answer with its own four rows",
			SCOPED.0
		);
	}
}

#[test]
fn a_group_sweep_never_answers_with_a_root_row_no_group_ever_wrote_under_it() {
	// the reaper hands every key a sweep returns to reclaim_identity_keys and then forgets the group, so a
	// root-scoped row in that answer is reclaimed while root still needs it; nothing here writes an identity
	// keyspace anywhere but root, so the only way one reaches this answer is the sweep crossing partitions
	let (mut state, _guard) = state(false);
	populate(&mut state, GroupId::ROOT, &ROOT_LAYOUT_IDENTITY, 4);
	populate(&mut state, SCOPED, &BARE_SUFFIX_DATA, 4);
	state.flush();

	let keys = state.group_sweep_in(SCOPED, KeyspaceMask::KNOWN, false, None).expect("a sweep must answer");
	for key in &keys {
		assert_eq!(
			group_of(key),
			SCOPED,
			"a sweep of group {} answered with a group-{} key in {}, a keyspace only root ever wrote",
			SCOPED.0,
			group_of(key).0,
			keyspace_of(key).name()
		);
	}
}

#[test]
fn a_range_that_stops_at_a_real_group_never_answers_from_the_root_pile() {
	// a keyspace with no group column keeps one pile of rows, at root, and root encodes above every real
	// group; a range that stops at a real group therefore ends below that pile entirely, so a row from it
	// in this answer is one the caller never addressed and the reaper would delete out from under root
	let (mut state, _guard) = state(false);
	populate(&mut state, GroupId::ROOT, &ROOT_LAYOUT_IDENTITY, 4);
	state.flush();

	let end = state_key(SCOPED, KeyspaceId::TIMER_WHEEL, u64::MAX);
	let page = state
		.state_page_inner(EncodedKeyRange::new(Bound::Unbounded, Bound::Included(end)), None)
		.expect("a range scan must answer");

	assert!(
		page.is_empty(),
		"a range ending at group {} answered with {} row(s) from root's pile, the first in {}",
		SCOPED.0,
		page.len(),
		keyspace_of(&page[0].0).name()
	);
}
