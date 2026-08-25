// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Ordering,
	collections::{
		BTreeMap,
		Bound::{Excluded, Unbounded},
	},
};

use reifydb_codec::key::encoded::EncodedKey;

use crate::coverage::{Edge, successor};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interval {
	pub start: EncodedKey,
	pub end: Edge,
}

impl Interval {
	pub fn new(start: EncodedKey, end: Edge) -> Self {
		Self {
			start,
			end,
		}
	}

	pub fn contains(&self, key: &EncodedKey) -> bool {
		self.start.as_slice() <= key.as_slice() && self.end.covers(key)
	}

	pub fn is_empty(&self) -> bool {
		!self.end.covers(&self.start)
	}
}

#[derive(Clone, Debug, Default)]
pub struct CoverageSet {
	intervals: BTreeMap<EncodedKey, Edge>,
}

impl CoverageSet {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn extend(&mut self, start: EncodedKey, end: Edge) {
		if !end.covers(&start) {
			return;
		}

		let mut merged_start = start.clone();
		let mut merged_end = end;
		let mut doomed = Vec::new();

		if let Some((left_start, left_end)) = self.intervals.range::<EncodedKey, _>(..=&start).next_back()
			&& left_end.cmp_key(&start) != Ordering::Less
		{
			merged_start = left_start.clone();
			merged_end = merged_end.max(left_end.clone());
			doomed.push(left_start.clone());
		}

		for (next_start, next_end) in self.intervals.range::<EncodedKey, _>((Excluded(&start), Unbounded)) {
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

	pub fn shrink_key(&mut self, key: &EncodedKey) {
		self.shrink_range(key, &Edge::Key(successor(key)));
	}

	pub fn shrink_range(&mut self, start: &EncodedKey, end: &Edge) {
		if !end.covers(start) {
			return;
		}

		let mut doomed = Vec::new();

		if let Some((left_start, left_end)) = self.intervals.range::<EncodedKey, _>(..=start).next_back()
			&& left_end.cmp_key(start) == Ordering::Greater
		{
			doomed.push(left_start.clone());
		}

		for (next_start, _) in self.intervals.range::<EncodedKey, _>((Excluded(start), Unbounded)) {
			if end.cmp_key(next_start) != Ordering::Greater {
				break;
			}
			doomed.push(next_start.clone());
		}

		for key in doomed {
			let old_end = self.intervals.remove(&key).unwrap();
			if key.as_slice() < start.as_slice() {
				self.intervals.insert(key, Edge::Key(start.clone()));
			}
			if *end < old_end {
				let resume = end.key().unwrap().clone();
				self.intervals.insert(resume, old_end);
			}
		}
	}

	pub fn contains(&self, key: &EncodedKey) -> bool {
		self.covering(key).is_some()
	}

	pub fn covering(&self, key: &EncodedKey) -> Option<Interval> {
		self.intervals.range::<EncodedKey, _>(..=key).next_back().and_then(|(start, end)| {
			if end.covers(key) {
				Some(Interval::new(start.clone(), end.clone()))
			} else {
				None
			}
		})
	}

	pub fn overlapping(&self, lo: &EncodedKey, hi: &Edge) -> Vec<Interval> {
		let mut clipped = Vec::new();
		if !hi.covers(lo) {
			return clipped;
		}

		if let Some((_, end)) = self.intervals.range::<EncodedKey, _>(..=lo).next_back()
			&& end.cmp_key(lo) == Ordering::Greater
		{
			clipped.push(Interval::new(lo.clone(), end.clone().min(hi.clone())));
		}

		for (start, end) in self.intervals.range::<EncodedKey, _>((Excluded(lo), Unbounded)) {
			if hi.cmp_key(start) != Ordering::Greater {
				break;
			}
			clipped.push(Interval::new(start.clone(), end.clone().min(hi.clone())));
		}

		clipped
	}

	pub fn gaps(&self, lo: &EncodedKey, hi: &Edge) -> Vec<Interval> {
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
			if at.as_slice() < covered.start.as_slice() {
				holes.push(Interval::new(at, Edge::Key(covered.start.clone())));
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

	pub fn iter(&self) -> impl Iterator<Item = Interval> + '_ {
		self.intervals.iter().map(|(start, end)| Interval::new(start.clone(), end.clone()))
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

	use super::{CoverageSet, Interval};
	use crate::coverage::{Edge, successor};

	fn k(bytes: &str) -> EncodedKey {
		EncodedKey::new(bytes)
	}

	fn iv(start: &str, end: &str) -> Interval {
		Interval::new(k(start), Edge::of(end))
	}

	fn open(start: &str) -> Interval {
		Interval::new(k(start), Edge::Top)
	}

	fn snapshot(set: &CoverageSet) -> Vec<Interval> {
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
		set.extend(k("c"), Edge::of("c"));
		set.extend(k("f"), Edge::of("c"));
		assert_eq!(snapshot(&set), vec![]);
	}

	#[test]
	fn extend_merges_two_touching_intervals() {
		// Touching pages must coalesce, otherwise a paged scan fragments into one gap per page.
		let mut set = CoverageSet::new();
		set.extend(k("a"), Edge::of("c"));
		set.extend(k("c"), Edge::of("f"));
		assert_eq!(snapshot(&set), vec![iv("a", "f")]);
	}

	#[test]
	fn extend_keeps_intervals_one_key_apart_separate() {
		// Key "b" itself is uncovered, so merging across it would overstate RAM.
		let mut set = CoverageSet::new();
		set.extend(k("a"), Edge::of("b"));
		set.extend(successor(&k("b")), Edge::of("c"));
		assert_eq!(snapshot(&set), vec![iv("a", "b"), Interval::new(successor(&k("b")), Edge::of("c"))]);
	}

	#[test]
	fn extend_swallows_every_spanned_interval() {
		// One claim over three islands must leave one interval, not one plus the three survivors.
		let mut set = CoverageSet::new();
		set.extend(k("b"), Edge::of("c"));
		set.extend(k("d"), Edge::of("e"));
		set.extend(k("f"), Edge::of("g"));
		set.extend(k("a"), Edge::of("h"));
		assert_eq!(snapshot(&set), vec![iv("a", "h")]);
	}

	#[test]
	fn extend_inside_an_existing_interval_changes_nothing() {
		// Re-claiming a subset must never narrow the wider claim already held.
		let mut set = CoverageSet::new();
		set.extend(k("a"), Edge::of("z"));
		set.extend(k("c"), Edge::of("f"));
		assert_eq!(snapshot(&set), vec![iv("a", "z")]);
	}

	#[test]
	fn extend_to_top_swallows_everything_above() {
		// An unbounded claim must win over every bounded end it absorbs.
		let mut set = CoverageSet::new();
		set.extend(k("b"), Edge::of("c"));
		set.extend(k("f"), Edge::of("g"));
		set.extend(k("a"), Edge::Top);
		assert_eq!(snapshot(&set), vec![open("a")]);
	}

	#[test]
	fn shrink_key_in_the_middle_splits_the_interval() {
		// The tail must resume at the successor, or key "m" stays wrongly covered.
		let mut set = CoverageSet::new();
		set.extend(k("a"), Edge::of("z"));
		set.shrink_key(&k("m"));
		assert_eq!(snapshot(&set), vec![iv("a", "m"), Interval::new(successor(&k("m")), Edge::of("z"))]);
	}

	#[test]
	fn shrink_key_at_interval_start_leaves_the_tail() {
		// No zero-width head may be stored when the removed key is the interval start.
		let mut set = CoverageSet::new();
		set.extend(k("a"), Edge::of("z"));
		set.shrink_key(&k("a"));
		assert_eq!(snapshot(&set), vec![Interval::new(successor(&k("a")), Edge::of("z"))]);
	}

	#[test]
	fn shrink_key_removes_a_single_key_interval_entirely() {
		// An interval reduced to nothing must vanish, not linger as a zero-width entry.
		let mut set = CoverageSet::new();
		set.extend(k("b"), Edge::Key(successor(&k("b"))));
		set.shrink_key(&k("b"));
		assert_eq!(snapshot(&set), vec![]);
	}

	#[test]
	fn shrink_range_splits_one_interval_into_two() {
		// Dropping a middle span must leave the tail claimed, not discard it.
		let mut set = CoverageSet::new();
		set.extend(k("a"), Edge::of("z"));
		set.shrink_range(&k("d"), &Edge::of("m"));
		assert_eq!(snapshot(&set), vec![iv("a", "d"), iv("m", "z")]);
	}

	#[test]
	fn shrink_range_removes_whole_intervals_and_clips_the_ends() {
		// Every interval the span reaches must be dropped, not only the first one found.
		let mut set = CoverageSet::new();
		set.extend(k("a"), Edge::of("c"));
		set.extend(k("d"), Edge::of("f"));
		set.extend(k("g"), Edge::of("z"));
		set.shrink_range(&k("b"), &Edge::of("h"));
		assert_eq!(snapshot(&set), vec![iv("a", "b"), iv("h", "z")]);
	}

	#[test]
	fn shrink_range_ignores_an_empty_span() {
		// A zero-width drop must not split an interval in two at that point.
		let mut set = CoverageSet::new();
		set.extend(k("a"), Edge::of("z"));
		set.shrink_range(&k("c"), &Edge::of("c"));
		assert_eq!(snapshot(&set), vec![iv("a", "z")]);
	}

	#[test]
	fn covering_returns_the_holding_interval() {
		// Lookup must land on the greatest start at or below the key, not the first one stored.
		let mut set = CoverageSet::new();
		set.extend(k("a"), Edge::of("c"));
		set.extend(k("e"), Edge::of("g"));
		assert_eq!(set.covering(&k("f")), Some(iv("e", "g")));
		assert_eq!(set.covering(&k("d")), None);
	}

	#[test]
	fn contains_is_false_at_the_exclusive_end() {
		// Reporting the end key as covered would serve an absent row as authoritative.
		let mut set = CoverageSet::new();
		set.extend(k("a"), Edge::of("c"));
		assert!(set.contains(&k("a")));
		assert!(set.contains(&k("b")));
		assert!(!set.contains(&k("c")));
	}

	#[test]
	fn overlapping_clips_the_low_end() {
		// A result starting before lo would claim coverage the caller never asked about.
		let mut set = CoverageSet::new();
		set.extend(k("a"), Edge::of("z"));
		assert_eq!(set.overlapping(&k("c"), &Edge::of("f")), vec![iv("c", "f")]);
	}

	#[test]
	fn overlapping_clips_the_high_end() {
		// An unclipped end leaks a claim past hi into whatever the caller does with it.
		let mut set = CoverageSet::new();
		set.extend(k("b"), Edge::of("z"));
		assert_eq!(set.overlapping(&k("a"), &Edge::of("f")), vec![iv("b", "f")]);
	}

	#[test]
	fn overlapping_with_top_hi_returns_every_interval_above_lo() {
		// An unbounded query must not stop at the first interval it finds.
		let mut set = CoverageSet::new();
		set.extend(k("b"), Edge::of("c"));
		set.extend(k("f"), Edge::Top);
		assert_eq!(set.overlapping(&k("a"), &Edge::Top), vec![iv("b", "c"), open("f")]);
	}

	#[test]
	fn gaps_on_an_empty_set_is_the_whole_query_range() {
		// With nothing covered the caller must be told to read the entire span from disk.
		let set = CoverageSet::new();
		assert_eq!(set.gaps(&k("a"), &Edge::of("z")), vec![iv("a", "z")]);
	}

	#[test]
	fn gaps_returns_empty_when_lo_is_not_below_hi() {
		// An inverted or zero-width query must yield no span, never a backwards one.
		let set = CoverageSet::new();
		assert_eq!(set.gaps(&k("m"), &Edge::of("c")), vec![]);
		assert_eq!(set.gaps(&k("c"), &Edge::of("c")), vec![]);
	}

	#[test]
	fn gaps_and_overlapping_partition_the_query_range() {
		// Every point in [lo, hi) belongs to exactly one side; a hole means a row is never read.
		let mut set = CoverageSet::new();
		set.extend(k("b"), Edge::of("d"));
		set.extend(k("f"), Edge::of("h"));
		assert_eq!(set.overlapping(&k("a"), &Edge::of("z")), vec![iv("b", "d"), iv("f", "h")]);
		assert_eq!(set.gaps(&k("a"), &Edge::of("z")), vec![iv("a", "b"), iv("d", "f"), iv("h", "z")]);
	}

	#[test]
	fn gaps_emits_no_zero_width_gap_when_coverage_starts_at_lo() {
		// A zero-width gap costs a pointless persistent round trip and inflates the gap guard.
		let mut set = CoverageSet::new();
		set.extend(k("a"), Edge::of("d"));
		assert_eq!(set.gaps(&k("a"), &Edge::of("z")), vec![iv("d", "z")]);
	}

	#[test]
	fn gaps_with_top_hi_keeps_the_open_tail() {
		// The span above the last interval is unbounded and must still be reported.
		let mut set = CoverageSet::new();
		set.extend(k("b"), Edge::of("d"));
		assert_eq!(set.gaps(&k("a"), &Edge::Top), vec![iv("a", "b"), open("d")]);
	}

	#[test]
	fn gaps_with_top_hi_ends_when_coverage_reaches_top() {
		// Coverage running to Top leaves no tail; emitting one would re-read covered rows forever.
		let mut set = CoverageSet::new();
		set.extend(k("b"), Edge::Top);
		assert_eq!(set.gaps(&k("a"), &Edge::Top), vec![iv("a", "b")]);
	}
}
