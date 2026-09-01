// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	sync::{
		Arc, Barrier, Mutex, RwLock,
		atomic::{AtomicU64, Ordering},
	},
	thread,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::key::typed::{ExclusiveUpperEnd, MultiKey};

use crate::coverage::{interval::CoverageSet, retraction::Retractions};

type PartId = u8;

type Hull = (EncodedKey, ExclusiveUpperEnd<MultiKey>);

type Interlock = Box<dyn Fn(&ModelCache) + Send + Sync>;

fn key(part: PartId, n: u64) -> EncodedKey {
	let mut bytes = Vec::with_capacity(9);
	bytes.push(part);
	bytes.extend_from_slice(&n.to_be_bytes());
	EncodedKey::new(bytes)
}

fn part_of(key: &EncodedKey) -> PartId {
	key.as_slice()[0]
}

fn island(at: &EncodedKey) -> Hull {
	(at.clone(), ExclusiveUpperEnd::just_past(at))
}

fn widen(slot: &mut Option<Hull>, span: Hull) {
	match slot {
		None => *slot = Some(span),
		Some((start, end)) => {
			if span.0.as_slice() < start.as_slice() {
				*start = span.0;
			}
			*end = end.clone().max(span.1);
		}
	}
}

#[derive(Default)]
struct Partition {
	rows: BTreeMap<EncodedKey, u64>,
	claimed: Option<Hull>,
	fills: u64,
	tick: u64,
}

struct ModelCache {
	partitions: Mutex<HashMap<PartId, Partition>>,
	coverage: RwLock<CoverageSet<MultiKey>>,
	retractions: Retractions,
	sequence: AtomicU64,
	tick: AtomicU64,
	cap: usize,
	interlock: Option<Interlock>,
	invalidate_interlock: Option<Interlock>,
	published: AtomicU64,
	refused: AtomicU64,
	drops_refused: AtomicU64,
	evictions: AtomicU64,
}

impl ModelCache {
	fn build(cap: usize, fill: Option<Interlock>, invalidate: Option<Interlock>) -> Arc<Self> {
		Arc::new(Self {
			partitions: Mutex::new(HashMap::new()),
			coverage: RwLock::new(CoverageSet::new()),
			retractions: Retractions::new(),
			sequence: AtomicU64::new(0),
			tick: AtomicU64::new(0),
			cap,
			interlock: fill,
			invalidate_interlock: invalidate,
			published: AtomicU64::new(0),
			refused: AtomicU64::new(0),
			drops_refused: AtomicU64::new(0),
			evictions: AtomicU64::new(0),
		})
	}

	fn new(cap: usize) -> Arc<Self> {
		Self::build(cap, None, None)
	}

	fn with_interlock(cap: usize, interlock: Interlock) -> Arc<Self> {
		Self::build(cap, Some(interlock), None)
	}

	fn with_invalidate_interlock(cap: usize, interlock: Interlock) -> Arc<Self> {
		Self::build(cap, None, Some(interlock))
	}

	fn place(&self, rows: &[(EncodedKey, u64)], span: &Hull) -> Option<PartId> {
		let part = part_of(&rows.first()?.0);
		let mut partitions = self.partitions.lock().unwrap();
		let entry = partitions.entry(part).or_default();
		for (at, value) in rows {
			entry.rows.insert(at.clone(), *value);
		}
		entry.fills = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
		entry.tick = self.tick.fetch_add(1, Ordering::SeqCst);
		widen(&mut entry.claimed, span.clone());
		Some(part)
	}

	fn publish(&self, span: Hull, token: u64) -> bool {
		let mut coverage = self.coverage.write().unwrap();
		if !self.retractions.unchanged(token) {
			self.refused.fetch_add(1, Ordering::Relaxed);
			return false;
		}
		coverage.extend(span.0, span.1);
		self.published.fetch_add(1, Ordering::Relaxed);
		true
	}

	fn fill(&self, at: EncodedKey, value: u64) -> bool {
		let token = self.retractions.token();
		let span = island(&at);
		if self.place(&[(at, value)], &span).is_none() {
			return false;
		}
		if let Some(hook) = &self.interlock {
			hook(self);
		}
		self.publish(span, token)
	}

	fn materialize(&self, lo: &EncodedKey, through: &EncodedKey, rows: &[(EncodedKey, u64)]) -> bool {
		let token = self.retractions.token();
		let span = (lo.clone(), ExclusiveUpperEnd::just_past(through));
		if self.place(rows, &span).is_none() {
			return false;
		}
		if let Some(hook) = &self.interlock {
			hook(self);
		}
		self.publish(span, token)
	}

	fn withdraw_key(&self, at: &EncodedKey) {
		let mut coverage = self.coverage.write().unwrap();
		coverage.shrink_key(at);
		self.retractions.record();
	}

	fn withdraw_span(&self, span: &Hull) {
		let mut coverage = self.coverage.write().unwrap();
		coverage.shrink_range(&span.0, &span.1);
		self.retractions.record();
	}

	fn invalidate(&self, at: &EncodedKey) {
		self.withdraw_key(at);
		if let Some(hook) = &self.invalidate_interlock {
			hook(self);
		}
		let emptied = {
			let mut partitions = self.partitions.lock().unwrap();
			match partitions.get_mut(&part_of(at)) {
				Some(entry) => {
					entry.rows.remove(at);
					entry.rows.is_empty()
				}
				None => false,
			}
		};
		self.withdraw_key(at);
		if emptied {
			self.retract_partition(part_of(at));
		}
	}

	fn retract_partition(&self, part: PartId) {
		let hull = self.partitions.lock().unwrap().get(&part).and_then(|entry| entry.claimed.clone());
		if let Some(hull) = &hull {
			self.withdraw_span(hull);
		}
	}

	fn pick_victim(&self) -> Option<(PartId, u64)> {
		let partitions = self.partitions.lock().unwrap();
		if partitions.len() <= self.cap {
			return None;
		}
		partitions.iter().min_by_key(|(_, entry)| entry.tick).map(|(part, entry)| (*part, entry.fills))
	}

	fn drop_victim(&self, part: PartId, fills: u64) -> bool {
		self.retract_partition(part);
		let mut partitions = self.partitions.lock().unwrap();
		let Some(entry) = partitions.get(&part) else {
			return false;
		};
		if entry.fills != fills {
			self.drops_refused.fetch_add(1, Ordering::Relaxed);
			return false;
		}
		partitions.remove(&part);
		self.evictions.fetch_add(1, Ordering::Relaxed);
		true
	}

	fn evict_to_capacity(&self) {
		while let Some((part, fills)) = self.pick_victim() {
			if !self.drop_victim(part, fills) {
				break;
			}
		}
	}

	fn clear(&self) {
		let mut coverage = self.coverage.write().unwrap();
		coverage.clear();
		self.partitions.lock().unwrap().clear();
		self.retractions.record();
	}

	fn covers(&self, at: &EncodedKey) -> bool {
		self.coverage.read().unwrap().contains(at)
	}

	fn resident(&self, at: &EncodedKey) -> bool {
		self.partitions.lock().unwrap().get(&part_of(at)).is_some_and(|entry| entry.rows.contains_key(at))
	}

	fn hull_of(&self, part: PartId) -> Option<Hull> {
		self.partitions.lock().unwrap().get(&part).and_then(|entry| entry.claimed.clone())
	}

	fn intervals(&self) -> usize {
		self.coverage.read().unwrap().len()
	}

	fn overstated(&self, domain: &[EncodedKey]) -> Option<EncodedKey> {
		domain.iter().find(|at| self.covers(at) && !self.resident(at)).cloned()
	}
}

fn dense(part: PartId, from: u64, through: u64, value: u64) -> Vec<(EncodedKey, u64)> {
	(from..=through).map(|n| (key(part, n), value)).collect()
}

#[test]
fn a_single_key_fill_claims_that_key_and_nothing_around_it() {
	let cache = ModelCache::new(8);
	assert!(cache.fill(key(1, 10), 100));

	assert!(cache.covers(&key(1, 10)));
	assert!(!cache.covers(&key(1, 9)), "the claim reached below the key it placed");
	assert!(!cache.covers(&key(1, 11)), "the claim reached above the key it placed");
}

#[test]
fn a_fill_places_its_rows_before_it_publishes_the_claim() {
	let observed = Arc::new(AtomicU64::new(0));
	let seen_resident = Arc::new(AtomicU64::new(0));
	let cache = {
		let observed = observed.clone();
		let seen_resident = seen_resident.clone();
		ModelCache::with_interlock(
			8,
			Box::new(move |cache| {
				observed.fetch_add(1, Ordering::SeqCst);
				if cache.resident(&key(1, 10)) {
					seen_resident.fetch_add(1, Ordering::SeqCst);
				}
			}),
		)
	};

	assert!(cache.fill(key(1, 10), 100));
	assert_eq!(observed.load(Ordering::SeqCst), 1, "the interlock must run inside the fill window");
	assert_eq!(seen_resident.load(Ordering::SeqCst), 1, "a claim was published before its rows were placed");
}

#[test]
fn a_partition_records_its_hull_when_its_rows_land_not_when_the_claim_publishes() {
	let seen = Arc::new(AtomicU64::new(0));
	let cache = {
		let seen = seen.clone();
		ModelCache::with_interlock(
			8,
			Box::new(move |cache| {
				if cache.hull_of(1).is_some() {
					seen.fetch_add(1, Ordering::SeqCst);
				}
			}),
		)
	};

	cache.fill(key(1, 10), 100);

	assert_eq!(seen.load(Ordering::SeqCst), 1, "the partition had no hull while its rows were already resident");
}

#[test]
fn a_fill_that_read_its_token_before_a_shrink_publishes_nothing() {
	let cache = {
		ModelCache::with_interlock(
			8,
			Box::new(move |cache| {
				cache.withdraw_key(&key(1, 10));
			}),
		)
	};

	assert!(!cache.fill(key(1, 10), 100), "a fill must refuse to publish across a retraction");
	assert!(!cache.covers(&key(1, 10)), "a refused fill left its claim standing");
	assert_eq!(cache.refused.load(Ordering::Relaxed), 1);
}

#[test]
fn an_invalidate_that_lands_inside_a_fill_window_must_not_leave_the_key_claimed() {
	let fired = Arc::new(AtomicU64::new(0));
	let cache = {
		let fired = fired.clone();
		ModelCache::with_interlock(
			8,
			Box::new(move |cache| {
				if fired.fetch_add(1, Ordering::SeqCst) != 1 {
					return;
				}
				cache.invalidate(&key(1, 11));
			}),
		)
	};

	cache.fill(key(1, 10), 100);
	cache.fill(key(1, 11), 101);

	assert_eq!(fired.load(Ordering::SeqCst), 2, "the interlock must have run inside both fills");
	assert!(!cache.resident(&key(1, 11)), "the interlock did not remove the key it invalidated");
	assert!(!cache.covers(&key(1, 11)), "a fill republished a claim over a key the invalidate removed from RAM");
}

#[test]
fn an_invalidate_withdraws_the_claim_before_the_row_leaves_ram() {
	let cache = ModelCache::new(8);
	cache.fill(key(1, 10), 100);
	assert!(cache.covers(&key(1, 10)) && cache.resident(&key(1, 10)));

	cache.invalidate(&key(1, 10));

	assert!(!cache.covers(&key(1, 10)), "the claim outlived the row");
	assert!(!cache.resident(&key(1, 10)));
}

#[test]
fn an_invalidate_punches_out_exactly_the_key_that_left_ram() {
	let cache = ModelCache::new(8);
	for n in 10..15 {
		cache.fill(key(1, n), n);
	}

	cache.invalidate(&key(1, 12));

	assert!(!cache.covers(&key(1, 12)), "the invalidated key stayed claimed");
	for n in [10u64, 11, 13, 14] {
		assert!(cache.covers(&key(1, n)), "key {n} lost its claim to a neighbour's invalidate");
	}
}

#[test]
fn a_partition_emptied_by_an_invalidate_leaves_no_claim_behind() {
	let cache = ModelCache::new(8);
	cache.fill(key(1, 10), 100);
	cache.fill(key(1, 11), 101);
	cache.fill(key(2, 10), 200);

	cache.invalidate(&key(1, 10));
	cache.invalidate(&key(1, 11));

	assert!(!cache.covers(&key(1, 10)), "an emptied partition left a claim standing");
	assert!(!cache.covers(&key(1, 11)), "an emptied partition left a claim standing");
	assert!(cache.covers(&key(2, 10)), "emptying one partition retracted a neighbour's claim");
}

#[test]
fn a_hull_that_outlived_its_rows_withdraws_nothing_a_neighbour_still_needs() {
	let cache = ModelCache::new(8);
	cache.fill(key(1, 10), 100);
	cache.fill(key(1, 11), 101);
	cache.fill(key(2, 10), 200);

	cache.invalidate(&key(1, 10));
	cache.invalidate(&key(1, 11));
	let stale = cache.hull_of(1).expect("a hull outlives the rows it was published for");

	cache.withdraw_span(&stale);

	assert!(cache.covers(&key(2, 10)), "a stale hull reached the partition beside it");
}

#[test]
fn evicting_a_partition_withdraws_every_claim_it_published() {
	let cache = ModelCache::new(1);
	for n in 10..14 {
		cache.fill(key(1, n), n);
	}
	cache.fill(key(2, 10), 200);

	let (victim, fills) = cache.pick_victim().expect("two partitions against a cap of one");
	assert_eq!(victim, 1, "the oldest partition must be the victim");
	assert!(cache.drop_victim(victim, fills));

	for n in 10..14 {
		assert!(!cache.covers(&key(1, n)), "the evicted partition kept its claim over key {n}");
	}
	assert!(cache.covers(&key(2, 10)), "the eviction retracted a claim it did not publish");
}

#[test]
fn a_drop_refuses_a_partition_that_was_evicted_and_refilled_since_the_victim_was_chosen() {
	let cache = ModelCache::new(1);
	cache.fill(key(1, 10), 100);
	cache.fill(key(2, 10), 200);

	let (victim, stale_fills) = cache.pick_victim().expect("two partitions against a cap of one");
	assert_eq!(victim, 1);

	cache.invalidate(&key(1, 10));
	cache.fill(key(1, 11), 111);
	assert!(cache.covers(&key(1, 11)), "the refill must claim its island");

	assert!(!cache.drop_victim(victim, stale_fills), "a stale drop took a partition that was refilled");
	assert_eq!(cache.drops_refused.load(Ordering::Relaxed), 1);
	assert!(cache.resident(&key(1, 11)), "the refused drop removed the refilled row anyway");
}

#[test]
fn one_partition_hull_never_reaches_another_partitions_keys() {
	let cache = ModelCache::new(8);
	for n in 10..13 {
		cache.fill(key(1, n), n);
		cache.fill(key(2, n), n);
	}

	let hull = cache.hull_of(1).expect("a fill records a hull");
	cache.withdraw_span(&hull);

	for n in 10..13 {
		assert!(!cache.covers(&key(1, n)), "the hull did not retract its own partition at key {n}");
		assert!(cache.covers(&key(2, n)), "one partition's hull reached the partition beside it at key {n}");
	}
}

#[test]
fn a_hull_end_is_never_the_top_of_the_key_space() {
	let cache = ModelCache::new(8);
	cache.fill(key(1, 10), 100);
	cache.materialize(&key(2, 0), &key(2, 5), &dense(2, 0, 5, 200));

	for part in [1u8, 2] {
		let (_, end) = cache.hull_of(part).expect("a fill records a hull");
		assert_ne!(end, ExclusiveUpperEnd::Top, "partition {part} claimed to the top of the key space");
	}
}

#[test]
fn a_materialize_claims_the_span_it_scanned_and_stops_at_its_edge() {
	let cache = ModelCache::new(8);
	let rows = dense(1, 10, 14, 500);

	assert!(cache.materialize(&key(1, 10), &key(1, 14), &rows));

	for n in 10..=14 {
		assert!(cache.covers(&key(1, n)), "the scanned span left key {n} unclaimed");
	}
	assert!(!cache.covers(&key(1, 15)), "the claim reached past the key the scan stopped at");
	assert!(!cache.covers(&key(1, 9)), "the claim reached below the key the scan started at");
}

#[test]
fn a_materialize_that_read_its_token_before_a_shrink_publishes_nothing() {
	let cache = {
		ModelCache::with_interlock(
			8,
			Box::new(move |cache| {
				cache.withdraw_key(&key(1, 12));
			}),
		)
	};

	assert!(!cache.materialize(&key(1, 10), &key(1, 14), &dense(1, 10, 14, 500)));
	for n in 10..=14 {
		assert!(!cache.covers(&key(1, n)), "a refused materialize left key {n} claimed");
	}
}

#[test]
fn clearing_withdraws_every_claim() {
	let cache = ModelCache::new(8);
	for part in 1..4u8 {
		for n in 10..13 {
			cache.fill(key(part, n), n);
		}
	}
	assert!(cache.intervals() > 0);

	cache.clear();

	assert_eq!(cache.intervals(), 0, "clear left claims standing");
	for part in 1..4u8 {
		for n in 10..13 {
			assert!(!cache.covers(&key(part, n)));
		}
	}
}

#[test]
fn a_publish_that_lands_inside_an_invalidate_must_not_survive_the_row_removal() {
	let fired = Arc::new(AtomicU64::new(0));
	let cache = {
		let fired = fired.clone();
		ModelCache::with_invalidate_interlock(
			8,
			Box::new(move |cache| {
				if fired.fetch_add(1, Ordering::SeqCst) != 0 {
					return;
				}
				cache.materialize(&key(1, 0), &key(1, 7), &dense(1, 0, 7, 900));
			}),
		)
	};
	for n in 0..8 {
		cache.fill(key(1, n), n);
	}

	cache.invalidate(&key(1, 4));

	assert_eq!(fired.load(Ordering::SeqCst), 1, "the interlock must have run inside the invalidate window");
	assert!(!cache.resident(&key(1, 4)), "the invalidate did not remove its row");
	assert!(!cache.covers(&key(1, 4)), "a claim published inside the invalidate window outlived its row");
}

struct Lcg(u64);

impl Lcg {
	fn next(&mut self) -> u64 {
		self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
		self.0 >> 33
	}
}

const PARTS: u8 = 6;
const PER_PART: u64 = 8;

fn domain() -> Vec<EncodedKey> {
	(1..=PARTS).flat_map(|part| (0..PER_PART).map(move |n| key(part, n))).collect()
}

fn step(cache: &ModelCache, rng: &mut Lcg, domain: &[EncodedKey], version: &AtomicU64) {
	let at = domain[(rng.next() % domain.len() as u64) as usize].clone();
	let value = version.fetch_add(1, Ordering::SeqCst);
	match rng.next() % 100 {
		0..34 => {
			cache.fill(at, value);
		}
		34..50 => {
			let part = (rng.next() % PARTS as u64) as u8 + 1;
			cache.materialize(
				&key(part, 0),
				&key(part, PER_PART - 1),
				&dense(part, 0, PER_PART - 1, value),
			);
		}
		50..74 => cache.invalidate(&at),
		74..96 => cache.evict_to_capacity(),
		_ => cache.clear(),
	}
}

#[test]
fn concurrent_fills_evictions_and_invalidates_never_overstate_coverage() {
	const THREADS: usize = 8;
	const ROUNDS: usize = 200;
	const STEPS: usize = 5;
	const SEEDS: [u64; 4] = [1, 29, 307, 4517];

	let domain = domain();
	let mut published = 0u64;
	let mut refused = 0u64;
	let mut drops_refused = 0u64;
	let mut evictions = 0u64;
	let violation: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));

	for seed in SEEDS {
		let cache = ModelCache::new(2);
		let barrier = Arc::new(Barrier::new(THREADS));
		let version = Arc::new(AtomicU64::new(1));
		let mut handles = Vec::with_capacity(THREADS);
		for id in 0..THREADS {
			let cache = cache.clone();
			let barrier = barrier.clone();
			let version = version.clone();
			let violation = violation.clone();
			let domain = domain.clone();
			handles.push(thread::spawn(move || {
				let mut rng = Lcg(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(id as u64));
				for round in 0..ROUNDS {
					for _ in 0..STEPS {
						step(&cache, &mut rng, &domain, &version);
					}
					barrier.wait();
					if id == 0
						&& let Some(at) = cache.overstated(&domain)
					{
						let mut slot = violation.lock().unwrap();
						if slot.is_none() {
							*slot = Some(format!(
								"seed {seed} round {round}: {:?} covered but not resident",
								at.as_slice()
							));
						}
					}
					barrier.wait();
				}
			}));
		}
		for handle in handles {
			handle.join().expect("a stress thread must not panic");
		}
		published += cache.published.load(Ordering::Relaxed);
		refused += cache.refused.load(Ordering::Relaxed);
		drops_refused += cache.drops_refused.load(Ordering::Relaxed);
		evictions += cache.evictions.load(Ordering::Relaxed);
	}

	assert_eq!(violation.lock().unwrap().clone(), None, "coverage overstated what RAM holds");
	assert!(published > 1000, "only {published} claims published: nothing was ever claimed to overstate");
	assert!(evictions > 100, "only {evictions} evictions: the capacity cap never forced the retraction path");
	assert!(refused > 10, "only {refused} claims refused by a token: the fill-versus-shrink race never ran");
	assert!(drops_refused > 0, "no drop was refused, so the fill-count guard never ran");
}
