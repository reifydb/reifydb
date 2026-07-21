// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{collections::HashMap, mem::size_of};

use reifydb_value::{byte_size::ByteSize, count::Count, reifydb_assertions};

use crate::metrics::heap::StateMemory;

const BUCKET_SLOTS: usize = 4;
const MAX_KICKS: usize = 512;
const MIN_BUCKETS: usize = 64;
const OVERFLOW_ENTRY_BYTES: u64 = 32;
const TARGET_LOAD_PERCENT: usize = 85;
const FP_DELTA_MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;
const KICK_SEED: u64 = 0x517C_C1B7_2722_0A95;

struct CuckooFilter {
	buckets: Vec<[u16; BUCKET_SLOTS]>,
	mask: u64,
	len: u64,
	kick_state: u64,
}

fn fingerprint(hash: u64) -> u16 {
	let fp = (hash >> 48) as u16;
	if fp == 0 {
		1
	} else {
		fp
	}
}

fn filter_bytes(bucket_count: usize) -> u64 {
	(bucket_count * BUCKET_SLOTS * size_of::<u16>() + size_of::<CuckooFilter>()) as u64
}

impl CuckooFilter {
	fn with_buckets(bucket_count: usize) -> Self {
		reifydb_assertions! {
			assert!(
				bucket_count.is_power_of_two(),
				"cuckoo bucket count must be a power of two so the mask-derived home and \
				 alt buckets stay in range (got {bucket_count})"
			);
		}
		Self {
			buckets: vec![[0; BUCKET_SLOTS]; bucket_count],
			mask: (bucket_count - 1) as u64,
			len: 0,
			kick_state: KICK_SEED ^ (bucket_count as u64),
		}
	}

	fn home_bucket(&self, hash: u64) -> usize {
		(hash & self.mask) as usize
	}

	fn alt_bucket(&self, bucket: usize, fp: u16) -> usize {
		let delta = ((fp as u64).wrapping_mul(FP_DELTA_MULTIPLIER) & self.mask) | 1;
		bucket ^ (delta as usize & self.mask as usize)
	}

	fn next_kick(&mut self) -> u64 {
		let mut x = self.kick_state;
		x ^= x << 13;
		x ^= x >> 7;
		x ^= x << 17;
		self.kick_state = x;
		x
	}

	fn try_place(&mut self, bucket: usize, fp: u16) -> bool {
		for slot in self.buckets[bucket].iter_mut() {
			if *slot == 0 {
				*slot = fp;
				return true;
			}
		}
		false
	}

	fn insert(&mut self, hash: u64) -> bool {
		let fp = fingerprint(hash);
		let home = self.home_bucket(hash);
		let alt = self.alt_bucket(home, fp);
		if self.try_place(home, fp) || self.try_place(alt, fp) {
			self.len += 1;
			return true;
		}

		let mut bucket = if self.next_kick() & 1 == 0 {
			home
		} else {
			alt
		};
		let mut in_hand = fp;
		let mut trail: Vec<(usize, usize, u16)> = Vec::with_capacity(MAX_KICKS);
		for _ in 0..MAX_KICKS {
			let slot = (self.next_kick() % BUCKET_SLOTS as u64) as usize;
			let displaced = self.buckets[bucket][slot];
			self.buckets[bucket][slot] = in_hand;
			trail.push((bucket, slot, displaced));
			in_hand = displaced;
			bucket = self.alt_bucket(bucket, in_hand);
			if self.try_place(bucket, in_hand) {
				self.len += 1;
				return true;
			}
		}
		for (bucket, slot, displaced) in trail.into_iter().rev() {
			self.buckets[bucket][slot] = displaced;
		}
		false
	}

	fn contains(&self, hash: u64) -> bool {
		let fp = fingerprint(hash);
		let home = self.home_bucket(hash);
		let alt = self.alt_bucket(home, fp);
		self.buckets[home].contains(&fp) || self.buckets[alt].contains(&fp)
	}

	fn remove(&mut self, hash: u64) -> bool {
		let fp = fingerprint(hash);
		let home = self.home_bucket(hash);
		let alt = self.alt_bucket(home, fp);
		for bucket in [home, alt] {
			for slot in self.buckets[bucket].iter_mut() {
				if *slot == fp {
					*slot = 0;
					self.len -= 1;
					return true;
				}
			}
		}
		false
	}

	fn bytes(&self) -> u64 {
		filter_bytes(self.buckets.len())
	}
}

pub struct MembershipIndex {
	filters: Vec<CuckooFilter>,
	overflow: HashMap<u64, u64>,
	byte_cap: u64,
	len: u64,
}

impl MembershipIndex {
	pub fn new(byte_cap: u64) -> Self {
		Self::with_capacity(0, byte_cap)
	}

	pub fn with_capacity(expected: usize, byte_cap: u64) -> Self {
		let needed = (expected * 100).div_ceil(BUCKET_SLOTS * TARGET_LOAD_PERCENT);
		let mut buckets = needed.next_power_of_two().max(MIN_BUCKETS);
		while buckets > MIN_BUCKETS && filter_bytes(buckets) > byte_cap {
			buckets /= 2;
		}
		Self {
			filters: vec![CuckooFilter::with_buckets(buckets)],
			overflow: HashMap::new(),
			byte_cap,
			len: 0,
		}
	}

	pub fn insert(&mut self, hash: u64) -> bool {
		if let Some(count) = self.overflow.get_mut(&hash) {
			*count += 1;
			self.len += 1;
			return true;
		}
		for filter in self.filters.iter_mut().rev() {
			if filter.insert(hash) {
				self.len += 1;
				return true;
			}
		}
		if self.filters.iter().rev().any(|filter| filter.contains(hash)) {
			if self.bytes() + OVERFLOW_ENTRY_BYTES > self.byte_cap {
				return false;
			}
			self.overflow.insert(hash, 1);
			self.len += 1;
			return true;
		}
		let grown = self.filters.last().expect("index always holds at least one filter").buckets.len() * 2;
		if self.bytes() + filter_bytes(grown) > self.byte_cap {
			return false;
		}
		let mut filter = CuckooFilter::with_buckets(grown);
		if !filter.insert(hash) {
			return false;
		}
		self.filters.push(filter);
		self.len += 1;
		true
	}

	pub fn contains(&self, hash: u64) -> bool {
		self.overflow.contains_key(&hash) || self.filters.iter().rev().any(|filter| filter.contains(hash))
	}

	pub fn remove(&mut self, hash: u64) -> bool {
		if let Some(count) = self.overflow.get_mut(&hash) {
			*count -= 1;
			if *count == 0 {
				self.overflow.remove(&hash);
			}
			self.len -= 1;
			return true;
		}
		for filter in self.filters.iter_mut().rev() {
			if filter.remove(hash) {
				self.len -= 1;
				return true;
			}
		}
		false
	}

	pub fn len(&self) -> u64 {
		self.len
	}

	pub fn is_empty(&self) -> bool {
		self.len == 0
	}

	pub fn bytes(&self) -> u64 {
		self.filters.iter().map(CuckooFilter::bytes).sum::<u64>()
			+ self.overflow.len() as u64 * OVERFLOW_ENTRY_BYTES
			+ size_of::<Self>() as u64
	}

	pub fn approximate_memory(&self) -> StateMemory {
		StateMemory::new(Count::new(self.len), ByteSize::from_bytes(self.bytes()))
	}
}

#[cfg(test)]
mod tests {
	use super::{MIN_BUCKETS, MembershipIndex, filter_bytes};

	// Deterministic 64-bit key-hash stream (splitmix64) standing in for xxh3 output;
	// the index only ever sees hashes, so the tests drive it the same way.
	fn hash_of(i: u64) -> u64 {
		let mut z = i.wrapping_add(0x9E37_79B9_7F4A_7C15);
		z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
		z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
		z ^ (z >> 31)
	}

	fn unbounded() -> MembershipIndex {
		MembershipIndex::new(u64::MAX)
	}

	#[test]
	fn a_live_key_is_never_reported_absent() {
		// The exact-negative guarantee is the whole contract: a false negative here
		// would make StateCache treat live persisted state as nonexistent and silently
		// restart a group's aggregation from scratch. Insert 50k, remove every other
		// one, and verify every survivor is still reported present.
		let mut index = unbounded();
		for i in 0..50_000u64 {
			assert!(index.insert(hash_of(i)), "unbounded index must accept every insert");
		}
		for i in (0..50_000u64).step_by(2) {
			assert!(index.remove(hash_of(i)), "a tracked key must be removable");
		}
		for i in (1..50_000u64).step_by(2) {
			assert!(index.contains(hash_of(i)), "live key {i} was reported absent - false negative");
		}
		assert_eq!(index.len(), 25_000);
	}

	#[test]
	fn false_positive_rate_stays_probabilistic_noise() {
		// FPs only cost a wasted point read, but the rate must stay noise, not signal:
		// with 16-bit fingerprints and 8 candidate slots the expected rate is ~0.012%,
		// so 1% over 100k disjoint probes is a loose, deterministic bound that still
		// fails on any systematic defect (e.g. fingerprints colliding to a constant).
		let mut index = unbounded();
		for i in 0..50_000u64 {
			index.insert(hash_of(i));
		}
		let false_positives = (1_000_000..1_100_000u64).filter(|i| index.contains(hash_of(*i))).count();
		assert!(false_positives < 1_000, "FP rate degenerated: {false_positives} / 100000");
	}

	#[test]
	fn duplicate_hashes_are_a_multiset() {
		// Two distinct live keys can share a 64-bit hash slot pair (and always share it
		// when they collide on the full hash). Removing one of them must not erase the
		// other's evidence - that is the exact over-removal bug that would turn a hash
		// collision into silent state loss.
		let mut index = unbounded();
		let hash = hash_of(7);
		assert!(index.insert(hash));
		assert!(index.insert(hash));
		assert!(index.remove(hash));
		assert!(index.contains(hash), "the second instance must survive the first removal");
		assert!(index.remove(hash));
		assert!(!index.contains(hash), "both instances removed - the hash must now read absent");
		assert!(!index.remove(hash), "removing an untracked hash must report failure, not underflow");
	}

	#[test]
	fn a_hot_hash_overflows_to_an_exact_count_instead_of_chaining_toward_the_cap() {
		// Join sides insert one instance per stored row, so a hot join key repeats
		// the same hash hundreds of times. A cuckoo bucket pair holds at most 8
		// copies of a fingerprint and kicks cannot separate identical fingerprints
		// (a duplicate ping-pongs between its own home and alt buckets), so before
		// the overflow map each extra copy doubled the chain until the byte cap
		// discarded the whole index - every hash-join node in the 2026-07-21
		// production profile showed revocations=1 from exactly this. Hot copies
		// must land in the exact side count instead: bounded memory, index alive,
		// len still exact (StateCache promotes values_complete on len equality, so
		// an approximate len would risk a false promotion = silent state loss).
		let mut index = MembershipIndex::with_capacity(64, 16 * 1024 * 1024);
		let baseline = index.bytes();
		let hot = hash_of(1);
		for i in 0..1_000u64 {
			assert!(index.insert(hot), "hot instance {i} must be absorbed, not rejected");
		}
		assert_eq!(index.len(), 1_000, "every hot instance must stay counted exactly");
		assert!(
			index.bytes() < baseline + filter_bytes(2 * MIN_BUCKETS),
			"1000 copies of one hash must not grow the chain: {baseline} -> {}",
			index.bytes()
		);
		for i in 0..999u64 {
			assert!(index.remove(hot), "instance {i} must be removable");
			assert!(index.contains(hot), "the key must stay present while {} instances remain", 999 - i);
		}
		assert!(index.remove(hot));
		assert!(
			!index.contains(hot),
			"the fully drained key must read absent - overflow stays an exact multiset"
		);
		assert_eq!(index.len(), 0);
	}

	#[test]
	fn overflow_still_respects_the_byte_cap() {
		// The overflow map must not become an unbounded escape hatch around the
		// byte cap: when even a side-count entry cannot fit, the insert must
		// surface rejection so the caller discards the index into read-through
		// mode, preserving the hard memory bound the cap promises.
		let cap = filter_bytes(MIN_BUCKETS) + size_of_index();
		let mut index = MembershipIndex::new(cap);
		let hot = hash_of(1);
		let mut rejected = false;
		for _ in 0..1_000 {
			if !index.insert(hot) {
				rejected = true;
				break;
			}
		}
		assert!(rejected, "a cap with no room for an overflow entry must reject, not absorb silently");
	}

	#[test]
	fn growth_chains_new_filters_without_losing_earlier_keys() {
		// A filter sized for 64 keys must chain, not reject, when the population is 100x
		// that - and every key inserted before the chain grew must remain visible.
		let mut index = MembershipIndex::with_capacity(64, u64::MAX);
		let initial_bytes = index.bytes();
		for i in 0..6_400u64 {
			assert!(index.insert(hash_of(i)), "growth must absorb insert {i}");
		}
		assert!(index.bytes() > initial_bytes, "absorbing 100x the sizing hint must have grown the index");
		for i in 0..6_400u64 {
			assert!(index.contains(hash_of(i)), "key {i} lost across chain growth");
		}
	}

	#[test]
	fn a_rejected_insert_leaves_every_tracked_key_intact() {
		// When the byte cap blocks growth the failed insert must roll its kick chain
		// back: the caller discards the index on rejection, but only AFTER seeing the
		// rejection - a partially shuffled filter that lost some earlier key would be
		// serving false negatives in the window before the rejection surfaces.
		let cap = filter_bytes(MIN_BUCKETS) + size_of_index();
		let mut index = MembershipIndex::new(cap);
		let mut accepted = Vec::new();
		let mut rejected_at = None;
		for i in 0..10_000u64 {
			if index.insert(hash_of(i)) {
				accepted.push(i);
			} else {
				rejected_at = Some(i);
				break;
			}
		}
		let rejected_at = rejected_at.expect("a single min-size filter must fill up within 10k inserts");
		assert!(rejected_at as usize >= MIN_BUCKETS, "the filter rejected before any slots could fill");
		for i in accepted {
			assert!(index.contains(hash_of(i)), "key {i} lost by the rolled-back insert");
		}
	}

	fn size_of_index() -> u64 {
		std::mem::size_of::<MembershipIndex>() as u64
	}

	#[test]
	fn identical_operation_sequences_produce_identical_indexes() {
		// Kick selection must be deterministic (seeded xorshift, no thread randomness):
		// DST replay depends on two runs over the same inputs making identical
		// decisions. Behavioural equality over a probe set is the observable contract.
		let build = || {
			let mut index = MembershipIndex::with_capacity(128, u64::MAX);
			for i in 0..5_000u64 {
				index.insert(hash_of(i));
			}
			for i in (0..5_000u64).step_by(3) {
				index.remove(hash_of(i));
			}
			index
		};
		let a = build();
		let b = build();
		assert_eq!(a.len(), b.len());
		assert_eq!(a.bytes(), b.bytes(), "the two runs must have grown identically");
		for i in 0..20_000u64 {
			assert_eq!(a.contains(hash_of(i)), b.contains(hash_of(i)), "divergent answer for probe {i}");
		}
	}

	#[test]
	fn with_capacity_presizes_to_avoid_chaining() {
		// Hydration knows the population up front; sizing for it must make the common
		// case a single filter so contains() stays two bucket probes, and the sizing
		// hint must leave enough slack (85% target load) that the full population fits.
		let mut index = MembershipIndex::with_capacity(10_000, u64::MAX);
		let presized = index.bytes();
		for i in 0..10_000u64 {
			assert!(index.insert(hash_of(i)));
		}
		assert_eq!(index.bytes(), presized, "a presized index must not chain for its declared population");
	}
}
