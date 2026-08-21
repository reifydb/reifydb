// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use reifydb_core::util::bloom::hash_item;
use reifydb_filter::{
	bloom::BloomFilter,
	source::{FilterSlice, KeyFilterSource},
};

#[derive(Debug)]
struct VecSource {
	hashes: Vec<u64>,
	pos: usize,
}

impl VecSource {
	fn new(hashes: Vec<u64>) -> Self {
		Self {
			hashes,
			pos: 0,
		}
	}
}

impl KeyFilterSource for VecSource {
	fn name(&self) -> &'static str {
		"vec"
	}

	fn estimated_len(&self) -> u64 {
		self.hashes.len() as u64
	}

	fn restart(&mut self) {
		self.pos = 0;
	}

	fn next_slice(&mut self, budget: usize) -> FilterSlice {
		let end = (self.pos + budget).min(self.hashes.len());
		let hashes = self.hashes[self.pos..end].to_vec();
		self.pos = end;
		FilterSlice {
			hashes,
			exhausted: self.pos >= self.hashes.len(),
		}
	}
}

fn drain(source: &mut dyn KeyFilterSource, budget: usize) -> (Vec<u64>, Vec<bool>) {
	// Drives a scan to completion the way a rebuild would, recording the exhausted flag of
	// every slice so a caller can assert it was raised exactly once, on the last slice.
	let mut seen = Vec::new();
	let mut flags = Vec::new();
	loop {
		let slice = source.next_slice(budget);
		seen.extend(slice.hashes.iter().copied());
		flags.push(slice.exhausted);
		if slice.exhausted {
			return (seen, flags);
		}
		assert!(flags.len() < 100, "scan did not terminate");
	}
}

#[test]
fn scan_in_several_slices_yields_every_hash_once_in_order() {
	// A rebuild must see each key exactly once: a duplicate inflates the fill ratio and a
	// dropped key makes the new filter answer absent for a key that is present.
	let keys: Vec<u64> = vec![11, 22, 33, 44, 55, 66, 77];
	let mut source = VecSource::new(keys.clone());

	let (seen, flags) = drain(&mut source, 3);

	assert_eq!(seen, keys);
	assert_eq!(flags, vec![false, false, true]);
}

#[test]
fn slice_never_exceeds_budget() {
	// The budget is what bounds a single rebuild step; a source that overshoots it would
	// stall the maintenance actor for an unbounded time.
	let mut source = VecSource::new((0..10).collect());

	for _ in 0..3 {
		let slice = source.next_slice(4);
		assert!(slice.hashes.len() <= 4, "slice of {} exceeded budget 4", slice.hashes.len());
	}
}

#[test]
fn budget_larger_than_remainder_returns_the_remainder_and_exhausts() {
	// Guards the tail of a scan: an oversized budget must not clamp to fewer keys, and must
	// not report more work pending when there is none.
	let mut source = VecSource::new(vec![1, 2, 3, 4, 5]);

	let first = source.next_slice(2);
	assert_eq!(first.hashes, vec![1, 2]);
	assert!(!first.exhausted);

	let rest = source.next_slice(1000);
	assert_eq!(rest.hashes, vec![3, 4, 5]);
	assert!(rest.exhausted);
}

#[test]
fn restart_after_partial_scan_replays_from_the_beginning() {
	// An abandoned rebuild (filter dropped mid-scan) must be able to start over; if restart
	// left the cursor mid-way the next filter would miss every key before it.
	let keys: Vec<u64> = vec![7, 8, 9, 10];
	let mut source = VecSource::new(keys.clone());

	let partial = source.next_slice(2);
	assert_eq!(partial.hashes, vec![7, 8]);
	assert!(!partial.exhausted);

	source.restart();

	let (seen, flags) = drain(&mut source, 2);
	assert_eq!(seen, keys);
	assert_eq!(flags, vec![false, true]);
}

#[test]
fn restart_after_exhausted_scan_replays_the_full_sequence() {
	// The steady state: every periodic rebuild reuses the same source object, so a source
	// that stayed exhausted after its first scan would yield an empty filter forever after.
	let keys: Vec<u64> = vec![100, 200, 300];
	let mut source = VecSource::new(keys.clone());

	let (first_pass, _) = drain(&mut source, 2);
	assert_eq!(first_pass, keys);

	source.restart();

	let (second_pass, flags) = drain(&mut source, 2);
	assert_eq!(second_pass, keys);
	assert_eq!(flags, vec![false, true]);
}

#[test]
fn empty_source_exhausts_on_the_very_first_slice() {
	// A source holding nothing must terminate the scan immediately; signalling more work on
	// the first call spins the rebuild loop on a source that will never produce a key.
	let mut source = VecSource::new(Vec::new());

	let slice = source.next_slice(16);
	assert!(slice.hashes.is_empty());
	assert!(slice.exhausted, "empty source must exhaust on the first call, not a later one");
}

#[test]
fn source_reports_its_own_key_count_and_label() {
	// estimated_len sizes the next filter and detects drift, so it tracks the source's real
	// contents rather than the number of keys already scanned.
	let mut source = VecSource::new(vec![1, 2, 3, 4]);
	assert_eq!(source.estimated_len(), 4);

	source.next_slice(2);
	assert_eq!(source.estimated_len(), 4);

	assert_eq!(source.name(), "vec");
}

#[test]
fn source_is_object_safe() {
	// A later rebuild keeps a heterogeneous Vec<Box<dyn KeyFilterSource>>; adding a generic
	// method or an associated type to the trait would break this line at compile time.
	let mut sources: Vec<Box<dyn KeyFilterSource>> =
		vec![Box::new(VecSource::new(vec![1])), Box::new(VecSource::new(vec![2, 3]))];

	let total: usize = sources.iter_mut().map(|s| s.next_slice(8).hashes.len()).sum();
	assert_eq!(total, 3);
}

#[test]
fn hash_entry_points_agree_with_the_item_entry_points() {
	// The rebuild path feeds precomputed hashes while the hot path passes items; if the two
	// entry points disagreed a rebuilt filter would answer absent for keys that are present.
	let bloom = BloomFilter::new(64);
	let item = "alpha";

	assert_eq!(bloom.might_contain_hash(hash_item(&item)), bloom.might_contain(&item));

	bloom.add(&item);
	assert!(bloom.might_contain_hash(hash_item(&item)));
	assert_eq!(bloom.might_contain_hash(hash_item(&item)), bloom.might_contain(&item));

	let other = "beta";
	bloom.add_hash(hash_item(&other));
	assert!(bloom.might_contain(&other));
}

#[test]
fn a_filter_rebuilt_from_a_source_answers_for_every_key() {
	// End to end over the contract: the hashes a source yields, fed through add_hash, must
	// reproduce the membership answers of a filter built by adding the items directly.
	let items = ["one", "two", "three", "four", "five"];
	let mut source = VecSource::new(items.iter().map(hash_item).collect());

	let rebuilt = BloomFilter::new(64);
	let (seen, _) = drain(&mut source, 2);
	for hash in seen {
		rebuilt.add_hash(hash);
	}

	for item in items {
		assert!(rebuilt.might_contain(&item), "rebuilt filter lost {}", item);
	}
}
