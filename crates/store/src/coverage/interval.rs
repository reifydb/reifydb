// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	collections::{
		BTreeMap,
		Bound::{Excluded, Unbounded},
	},
	mem,
};

use reifydb_core::key::typed::{ExclusiveUpperEnd, Key};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interval<K> {
	pub start: K,
	pub end: ExclusiveUpperEnd<K>,
}

impl<K: Key> Interval<K> {
	pub fn new(start: K, end: ExclusiveUpperEnd<K>) -> Self {
		Self {
			start,
			end,
		}
	}

	pub fn contains(&self, key: &K) -> bool {
		&self.start <= key && self.end.covers(key)
	}

	pub fn is_empty(&self) -> bool {
		!self.end.covers(&self.start)
	}
}

#[derive(Clone, Debug)]
pub struct CoverageSet<K> {
	intervals: BTreeMap<K, ExclusiveUpperEnd<K>>,
}

impl<K: Key> Default for CoverageSet<K> {
	fn default() -> Self {
		Self {
			intervals: BTreeMap::new(),
		}
	}
}

impl<K: Key> CoverageSet<K> {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn extend(&mut self, start: K, end: ExclusiveUpperEnd<K>) {
		if !end.covers(&start) {
			return;
		}

		let mut merged_start = start.clone();
		let mut merged_end = end;
		let mut doomed = Vec::new();

		if let Some((left_start, left_end)) = self.intervals.range::<K, _>(..=&start).next_back()
			&& left_end.cmp_key(&start) != Ordering::Less
		{
			merged_start = left_start.clone();
			merged_end = merged_end.max(left_end.clone());
			doomed.push(left_start.clone());
		}

		for (next_start, next_end) in self.intervals.range::<K, _>((Excluded(&start), Unbounded)) {
			if merged_end.cmp_key(next_start) == Ordering::Less {
				break;
			}
			merged_end = merged_end.max(next_end.clone());
			doomed.push(next_start.clone());
		}

		for key in doomed {
			self.intervals.remove(&key);
		}
		self.intervals.insert(merged_start, merged_end);
	}

	pub fn drop_overlapping(&mut self, start: &K, end: &ExclusiveUpperEnd<K>) {
		if !end.covers(start) {
			return;
		}

		let mut doomed = Vec::new();

		if let Some((left_start, left_end)) = self.intervals.range::<K, _>(..=start).next_back()
			&& left_end.cmp_key(start) == Ordering::Greater
		{
			doomed.push(left_start.clone());
		}

		for (next_start, _) in self.intervals.range::<K, _>((Excluded(start), Unbounded)) {
			if end.cmp_key(next_start) != Ordering::Greater {
				break;
			}
			doomed.push(next_start.clone());
		}

		for key in doomed {
			self.intervals.remove(&key);
		}
	}

	pub fn shrink_key(&mut self, key: &K) {
		self.shrink_range(key, &ExclusiveUpperEnd::just_past(key));
	}

	pub fn shrink_range(&mut self, start: &K, end: &ExclusiveUpperEnd<K>) {
		if !end.covers(start) {
			return;
		}

		let mut doomed = Vec::new();

		if let Some((left_start, left_end)) = self.intervals.range::<K, _>(..=start).next_back()
			&& left_end.cmp_key(start) == Ordering::Greater
		{
			doomed.push(left_start.clone());
		}

		for (next_start, _) in self.intervals.range::<K, _>((Excluded(start), Unbounded)) {
			if end.cmp_key(next_start) != Ordering::Greater {
				break;
			}
			doomed.push(next_start.clone());
		}

		for key in doomed {
			let old_end = self.intervals.remove(&key).unwrap();
			if &key < start {
				self.intervals.insert(key, ExclusiveUpperEnd::Key(start.clone()));
			}
			if *end < old_end {
				let resume = end.key().unwrap().clone();
				self.intervals.insert(resume, old_end);
			}
		}
	}

	pub fn contains(&self, key: &K) -> bool {
		self.covering(key).is_some()
	}

	pub fn covering(&self, key: &K) -> Option<Interval<K>> {
		self.intervals.range::<K, _>(..=key).next_back().and_then(|(start, end)| {
			if end.covers(key) {
				Some(Interval::new(start.clone(), end.clone()))
			} else {
				None
			}
		})
	}

	pub fn overlapping(&self, lo: &K, hi: &ExclusiveUpperEnd<K>) -> Vec<Interval<K>> {
		let mut clipped = Vec::new();
		if !hi.covers(lo) {
			return clipped;
		}

		if let Some((_, end)) = self.intervals.range::<K, _>(..=lo).next_back()
			&& end.cmp_key(lo) == Ordering::Greater
		{
			clipped.push(Interval::new(lo.clone(), end.clone().min(hi.clone())));
		}

		for (start, end) in self.intervals.range::<K, _>((Excluded(lo), Unbounded)) {
			if hi.cmp_key(start) != Ordering::Greater {
				break;
			}
			clipped.push(Interval::new(start.clone(), end.clone().min(hi.clone())));
		}

		clipped
	}

	pub fn gaps(&self, lo: &K, hi: &ExclusiveUpperEnd<K>) -> Vec<Interval<K>> {
		let mut holes = Vec::new();
		if !hi.covers(lo) {
			return holes;
		}

		let mut cursor = Some(lo.clone());
		for covered in self.overlapping(lo, hi) {
			let at = match cursor {
				Some(at) => at,
				None => break,
			};
			if at < covered.start {
				holes.push(Interval::new(at, ExclusiveUpperEnd::Key(covered.start.clone())));
			}
			cursor = covered.end.key().cloned();
		}

		if let Some(at) = cursor
			&& hi.covers(&at)
		{
			holes.push(Interval::new(at, hi.clone()));
		}

		holes
	}

	pub fn iter(&self) -> impl Iterator<Item = Interval<K>> + '_ {
		self.intervals.iter().map(|(start, end)| Interval::new(start.clone(), end.clone()))
	}

	pub fn bytes(&self) -> u64 {
		let per_entry = mem::size_of::<K>() + mem::size_of::<ExclusiveUpperEnd<K>>();
		self.intervals
			.iter()
			.map(|(start, end)| {
				per_entry
					+ start.heap_size() + match end {
					ExclusiveUpperEnd::Key(key) => key.heap_size(),
					ExclusiveUpperEnd::Top => 0,
				}
			})
			.sum::<usize>() as u64
	}

	pub fn len(&self) -> usize {
		self.intervals.len()
	}

	pub fn is_empty(&self) -> bool {
		self.intervals.is_empty()
	}

	pub fn clear(&mut self) {
		self.intervals.clear();
	}
}

#[cfg(test)]
mod tests {
	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::key::typed::{ExclusiveUpperEnd, Key, MultiKey};

	use super::{CoverageSet, Interval};

	fn k(bytes: &str) -> EncodedKey {
		EncodedKey::new(bytes)
	}

	fn successor_of(key: &EncodedKey) -> EncodedKey {
		key.successor().expect("a byte string has no greatest element, so it always has a successor")
	}

	fn iv(start: &str, end: &str) -> Interval<MultiKey> {
		Interval::new(k(start), ExclusiveUpperEnd::of(end))
	}

	fn open(start: &str) -> Interval<MultiKey> {
		Interval::new(k(start), ExclusiveUpperEnd::Top)
	}

	fn snapshot(set: &CoverageSet<MultiKey>) -> Vec<Interval<MultiKey>> {
		set.iter().collect()
	}

	#[test]
	fn interval_contains_start_but_not_end() {
		// Half-open: covering the exclusive end would overstate RAM by exactly one key.
		let interval = iv("c", "f");
		assert!(interval.contains(&k("c")));
		assert!(interval.contains(&k("e")));
		assert!(!interval.contains(&k("f")));
		assert!(!interval.contains(&k("b")));
	}

	#[test]
	fn interval_with_top_end_contains_every_key_at_or_above_start() {
		// Top has no key to compare against, so it must answer covered for anything above start.
		let interval = open("c");
		assert!(interval.contains(&k("c")));
		assert!(interval.contains(&k("zzzz")));
		assert!(!interval.contains(&k("b")));
	}

	#[test]
	fn interval_is_empty_when_end_equals_start() {
		// A zero-width interval must never be treated as a claim over its start key.
		assert!(iv("c", "c").is_empty());
		assert!(iv("f", "c").is_empty());
		assert!(!iv("c", "f").is_empty());
		assert!(!open("c").is_empty());
	}

	#[test]
	fn extend_ignores_an_empty_span() {
		// A zero-width claim must not land as an entry, or later merges inherit a corrupt start.
		let mut set = CoverageSet::new();
		set.extend(k("c"), ExclusiveUpperEnd::of("c"));
		set.extend(k("f"), ExclusiveUpperEnd::of("c"));
		assert_eq!(snapshot(&set), vec![]);
	}

	#[test]
	fn extend_merges_two_touching_intervals() {
		// Touching pages must coalesce, otherwise a paged scan fragments into one gap per page.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("c"));
		set.extend(k("c"), ExclusiveUpperEnd::of("f"));
		assert_eq!(snapshot(&set), vec![iv("a", "f")]);
	}

	#[test]
	fn extend_keeps_intervals_one_key_apart_separate() {
		// Key "b" itself is uncovered, so merging across it would overstate RAM.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("b"));
		set.extend(successor_of(&k("b")), ExclusiveUpperEnd::of("c"));
		assert_eq!(
			snapshot(&set),
			vec![iv("a", "b"), Interval::new(successor_of(&k("b")), ExclusiveUpperEnd::of("c"))]
		);
	}

	#[test]
	fn extend_swallows_every_spanned_interval() {
		// One claim over three islands must leave one interval, not one plus the three survivors.
		let mut set = CoverageSet::new();
		set.extend(k("b"), ExclusiveUpperEnd::of("c"));
		set.extend(k("d"), ExclusiveUpperEnd::of("e"));
		set.extend(k("f"), ExclusiveUpperEnd::of("g"));
		set.extend(k("a"), ExclusiveUpperEnd::of("h"));
		assert_eq!(snapshot(&set), vec![iv("a", "h")]);
	}

	#[test]
	fn extend_inside_an_existing_interval_changes_nothing() {
		// Re-claiming a subset must never narrow the wider claim already held.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("z"));
		set.extend(k("c"), ExclusiveUpperEnd::of("f"));
		assert_eq!(snapshot(&set), vec![iv("a", "z")]);
	}

	#[test]
	fn extend_to_top_swallows_everything_above() {
		// An unbounded claim must win over every bounded end it absorbs.
		let mut set = CoverageSet::new();
		set.extend(k("b"), ExclusiveUpperEnd::of("c"));
		set.extend(k("f"), ExclusiveUpperEnd::of("g"));
		set.extend(k("a"), ExclusiveUpperEnd::Top);
		assert_eq!(snapshot(&set), vec![open("a")]);
	}

	#[test]
	fn shrink_key_in_the_middle_splits_the_interval() {
		// The tail must resume at the successor, or key "m" stays wrongly covered.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("z"));
		set.shrink_key(&k("m"));
		assert_eq!(
			snapshot(&set),
			vec![iv("a", "m"), Interval::new(successor_of(&k("m")), ExclusiveUpperEnd::of("z"))]
		);
	}

	#[test]
	fn shrink_key_at_interval_start_leaves_the_tail() {
		// No zero-width head may be stored when the removed key is the interval start.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("z"));
		set.shrink_key(&k("a"));
		assert_eq!(snapshot(&set), vec![Interval::new(successor_of(&k("a")), ExclusiveUpperEnd::of("z"))]);
	}

	#[test]
	fn shrink_key_removes_a_single_key_interval_entirely() {
		// An interval reduced to nothing must vanish, not linger as a zero-width entry.
		let mut set = CoverageSet::new();
		set.extend(k("b"), ExclusiveUpperEnd::Key(successor_of(&k("b"))));
		set.shrink_key(&k("b"));
		assert_eq!(snapshot(&set), vec![]);
	}

	#[test]
	fn drop_overlapping_never_splits_an_interval() {
		// Retraction must not fragment: punching a hole would leave two intervals where one stood,
		// and the eviction path calls this once per evicted partition forever.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("z"));
		set.drop_overlapping(&k("d"), &ExclusiveUpperEnd::of("m"));
		assert_eq!(snapshot(&set), vec![]);
	}

	#[test]
	fn drop_overlapping_removes_every_interval_the_span_touches() {
		// Every island under the span goes, not just the first one the walk meets; a survivor
		// would claim coverage over keys whose partition was just evicted.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("c"));
		set.extend(k("e"), ExclusiveUpperEnd::of("g"));
		set.extend(k("h"), ExclusiveUpperEnd::of("j"));
		set.extend(k("m"), ExclusiveUpperEnd::of("p"));
		set.drop_overlapping(&k("b"), &ExclusiveUpperEnd::of("i"));
		assert_eq!(snapshot(&set), vec![iv("m", "p")]);
	}

	#[test]
	fn drop_overlapping_leaves_untouched_intervals_alone() {
		// The interval starting left of the span must survive when it ends before the span opens,
		// and the one starting at the exclusive end is not covered either; dropping either would
		// discard coverage the evicted partition never held.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("c"));
		set.extend(k("m"), ExclusiveUpperEnd::of("p"));
		set.drop_overlapping(&k("f"), &ExclusiveUpperEnd::of("m"));
		assert_eq!(snapshot(&set), vec![iv("a", "c"), iv("m", "p")]);
	}

	#[test]
	fn repeated_evict_and_reclaim_cycles_do_not_grow_the_interval_count() {
		// The leak shape: 3.07M intervals against 11k partitions. Cycling a middle span must
		// return to a bounded count, never accumulate one interval per eviction.
		let mut set = CoverageSet::new();
		for _ in 0..64 {
			set.extend(k("a"), ExclusiveUpperEnd::of("z"));
			set.drop_overlapping(&k("d"), &ExclusiveUpperEnd::of("m"));
		}
		assert!(set.len() <= 1, "coverage grew to {} intervals across 64 evict/reclaim cycles", set.len());
	}

	#[test]
	fn shrink_range_splits_one_interval_into_two() {
		// Dropping a middle span must leave the tail claimed, not discard it.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("z"));
		set.shrink_range(&k("d"), &ExclusiveUpperEnd::of("m"));
		assert_eq!(snapshot(&set), vec![iv("a", "d"), iv("m", "z")]);
	}

	#[test]
	fn shrink_range_removes_whole_intervals_and_clips_the_ends() {
		// Every interval the span reaches must be dropped, not only the first one found.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("c"));
		set.extend(k("d"), ExclusiveUpperEnd::of("f"));
		set.extend(k("g"), ExclusiveUpperEnd::of("z"));
		set.shrink_range(&k("b"), &ExclusiveUpperEnd::of("h"));
		assert_eq!(snapshot(&set), vec![iv("a", "b"), iv("h", "z")]);
	}

	#[test]
	fn shrink_range_ignores_an_empty_span() {
		// A zero-width drop must not split an interval in two at that point.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("z"));
		set.shrink_range(&k("c"), &ExclusiveUpperEnd::of("c"));
		assert_eq!(snapshot(&set), vec![iv("a", "z")]);
	}

	#[test]
	fn covering_returns_the_holding_interval() {
		// Lookup must land on the greatest start at or below the key, not the first one stored.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("c"));
		set.extend(k("e"), ExclusiveUpperEnd::of("g"));
		assert_eq!(set.covering(&k("f")), Some(iv("e", "g")));
		assert_eq!(set.covering(&k("d")), None);
	}

	#[test]
	fn contains_is_false_at_the_exclusive_end() {
		// Reporting the end key as covered would serve an absent row as authoritative.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("c"));
		assert!(set.contains(&k("a")));
		assert!(set.contains(&k("b")));
		assert!(!set.contains(&k("c")));
	}

	#[test]
	fn overlapping_clips_the_low_end() {
		// A result starting before lo would claim coverage the caller never asked about.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("z"));
		assert_eq!(set.overlapping(&k("c"), &ExclusiveUpperEnd::of("f")), vec![iv("c", "f")]);
	}

	#[test]
	fn overlapping_clips_the_high_end() {
		// An unclipped end leaks a claim past hi into whatever the caller does with it.
		let mut set = CoverageSet::new();
		set.extend(k("b"), ExclusiveUpperEnd::of("z"));
		assert_eq!(set.overlapping(&k("a"), &ExclusiveUpperEnd::of("f")), vec![iv("b", "f")]);
	}

	#[test]
	fn overlapping_with_top_hi_returns_every_interval_above_lo() {
		// An unbounded query must not stop at the first interval it finds.
		let mut set = CoverageSet::new();
		set.extend(k("b"), ExclusiveUpperEnd::of("c"));
		set.extend(k("f"), ExclusiveUpperEnd::Top);
		assert_eq!(set.overlapping(&k("a"), &ExclusiveUpperEnd::Top), vec![iv("b", "c"), open("f")]);
	}

	#[test]
	fn gaps_on_an_empty_set_is_the_whole_query_range() {
		// With nothing covered the caller must be told to read the entire span from disk.
		let set = CoverageSet::new();
		assert_eq!(set.gaps(&k("a"), &ExclusiveUpperEnd::of("z")), vec![iv("a", "z")]);
	}

	#[test]
	fn gaps_returns_empty_when_lo_is_not_below_hi() {
		// An inverted or zero-width query must yield no span, never a backwards one.
		let set = CoverageSet::new();
		assert_eq!(set.gaps(&k("m"), &ExclusiveUpperEnd::of("c")), vec![]);
		assert_eq!(set.gaps(&k("c"), &ExclusiveUpperEnd::of("c")), vec![]);
	}

	#[test]
	fn gaps_and_overlapping_partition_the_query_range() {
		// Every point in [lo, hi) belongs to exactly one side; a hole means a row is never read.
		let mut set = CoverageSet::new();
		set.extend(k("b"), ExclusiveUpperEnd::of("d"));
		set.extend(k("f"), ExclusiveUpperEnd::of("h"));
		assert_eq!(set.overlapping(&k("a"), &ExclusiveUpperEnd::of("z")), vec![iv("b", "d"), iv("f", "h")]);
		assert_eq!(
			set.gaps(&k("a"), &ExclusiveUpperEnd::of("z")),
			vec![iv("a", "b"), iv("d", "f"), iv("h", "z")]
		);
	}

	#[test]
	fn gaps_emits_no_zero_width_gap_when_coverage_starts_at_lo() {
		// A zero-width gap costs a pointless persistent round trip and inflates the gap guard.
		let mut set = CoverageSet::new();
		set.extend(k("a"), ExclusiveUpperEnd::of("d"));
		assert_eq!(set.gaps(&k("a"), &ExclusiveUpperEnd::of("z")), vec![iv("d", "z")]);
	}

	#[test]
	fn gaps_with_top_hi_keeps_the_open_tail() {
		// The span above the last interval is unbounded and must still be reported.
		let mut set = CoverageSet::new();
		set.extend(k("b"), ExclusiveUpperEnd::of("d"));
		assert_eq!(set.gaps(&k("a"), &ExclusiveUpperEnd::Top), vec![iv("a", "b"), open("d")]);
	}

	#[test]
	fn gaps_with_top_hi_ends_when_coverage_reaches_top() {
		// Coverage running to Top leaves no tail; emitting one would re-read covered rows forever.
		let mut set = CoverageSet::new();
		set.extend(k("b"), ExclusiveUpperEnd::Top);
		assert_eq!(set.gaps(&k("a"), &ExclusiveUpperEnd::Top), vec![iv("a", "b")]);
	}
}
