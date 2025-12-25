// Copyright (c) reifydb.com 2025
// This file is licensed under the AGPL-3.0-or-later, see license.md file

use core::{
	cmp::Ordering,
	ops::{Bound, RangeBounds},
};
use std::collections::HashSet;

use reifydb_core::{EncodedKey, EncodedKeyRange};
use reifydb_type::util::hex;
use tracing::instrument;

/// Maximum number of ranges before escalating to read_all.
/// This prevents memory bloat from too many small ranges.
const MAX_RANGES_BEFORE_ESCALATION: usize = 64;

/// High-performance conflict manager using HashSet for O(1) lookups
/// and optimized range handling with sorted, merged ranges.
#[derive(Debug, Default, Clone)]
pub struct ConflictManager {
	/// Single key reads - deduplicated automatically by HashSet
	read_keys: HashSet<EncodedKey>,
	/// Range reads - kept sorted by start bound and merged to reduce count.
	/// Invariant: ranges are non-overlapping and sorted.
	read_ranges: Vec<(Bound<EncodedKey>, Bound<EncodedKey>)>,
	/// Full scan flag
	read_all: bool,
	/// Keys that will be written to
	write_keys: HashSet<EncodedKey>,
}

impl ConflictManager {
	pub fn new() -> Self {
		Self {
			read_keys: HashSet::new(),
			read_ranges: Vec::new(),
			read_all: false,
			write_keys: HashSet::new(),
		}
	}

	#[instrument(name = "transaction::conflict::mark_read", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())))]
	pub fn mark_read(&mut self, key: &EncodedKey) {
		self.read_keys.insert(key.clone());
	}

	#[instrument(name = "transaction::conflict::mark_write", level = "trace", skip(self), fields(key_hex = %hex::encode(key.as_ref())))]
	pub fn mark_write(&mut self, key: &EncodedKey) {
		self.write_keys.insert(key.clone());
	}

	#[instrument(name = "transaction::conflict::mark_range", level = "trace", skip(self), fields(range_start = ?range.start_bound(), range_end = ?range.end_bound()))]
	pub fn mark_range(&mut self, range: EncodedKeyRange) {
		// Already tracking all - nothing more to do
		if self.read_all {
			return;
		}

		let start = match range.start_bound() {
			Bound::Included(k) => Bound::Included(k.clone()),
			Bound::Excluded(k) => Bound::Excluded(k.clone()),
			Bound::Unbounded => Bound::Unbounded,
		};

		let end = match range.end_bound() {
			Bound::Included(k) => Bound::Included(k.clone()),
			Bound::Excluded(k) => Bound::Excluded(k.clone()),
			Bound::Unbounded => Bound::Unbounded,
		};

		// Unbounded on both ends = full scan
		if start == Bound::Unbounded && end == Bound::Unbounded {
			self.read_all = true;
			self.read_ranges.clear();
			return;
		}

		// Insert and merge with existing ranges
		self.insert_and_merge(start, end);

		// Escalate to read_all if too many ranges
		if self.read_ranges.len() > MAX_RANGES_BEFORE_ESCALATION {
			self.read_all = true;
			self.read_ranges.clear();
		}
	}

	/// Insert a range and merge with any overlapping or adjacent ranges.
	/// Maintains the invariant that ranges are sorted and non-overlapping.
	fn insert_and_merge(&mut self, start: Bound<EncodedKey>, end: Bound<EncodedKey>) {
		if self.read_ranges.is_empty() {
			self.read_ranges.push((start, end));
			return;
		}

		// Find the insertion point using binary search on start bounds
		let insert_pos = self
			.read_ranges
			.binary_search_by(|(existing_start, _)| Self::compare_start_bounds(existing_start, &start))
			.unwrap_or_else(|pos| pos);

		// Determine the range of existing ranges that might overlap or be adjacent
		// Start from the previous range (if exists) as it might overlap
		let check_start = insert_pos.saturating_sub(1);

		// Find how many ranges we need to merge
		let mut merge_start = None;
		let mut merge_end = insert_pos; // Exclusive
		let mut merged_start = start.clone();
		let mut merged_end = end.clone();

		for i in check_start..self.read_ranges.len() {
			let (existing_start, existing_end) = &self.read_ranges[i];

			if Self::ranges_overlap_or_adjacent(&merged_start, &merged_end, existing_start, existing_end) {
				if merge_start.is_none() {
					merge_start = Some(i);
				}
				merge_end = i + 1;

				// Expand the merged range
				if Self::compare_start_bounds(existing_start, &merged_start) == Ordering::Less {
					merged_start = existing_start.clone();
				}
				if Self::compare_end_bounds(existing_end, &merged_end) == Ordering::Greater {
					merged_end = existing_end.clone();
				}
			} else if Self::compare_start_bounds(existing_start, &merged_end) == Ordering::Greater {
				// This range starts after our merged range ends - no more overlaps possible
				break;
			}
		}

		match merge_start {
			Some(start_idx) => {
				// Replace the merged ranges with the single merged range
				self.read_ranges.drain(start_idx..merge_end);
				self.read_ranges.insert(start_idx, (merged_start, merged_end));
			}
			None => {
				// No overlaps - just insert at the right position
				self.read_ranges.insert(insert_pos, (start, end));
			}
		}
	}

	/// Compare two start bounds for ordering.
	/// Unbounded is considered less than any bounded value.
	fn compare_start_bounds(a: &Bound<EncodedKey>, b: &Bound<EncodedKey>) -> Ordering {
		match (a, b) {
			(Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
			(Bound::Unbounded, _) => Ordering::Less,
			(_, Bound::Unbounded) => Ordering::Greater,
			(Bound::Included(ak), Bound::Included(bk)) => ak.cmp(bk),
			(Bound::Excluded(ak), Bound::Excluded(bk)) => ak.cmp(bk),
			// Included(x) < Excluded(x) for start bounds
			(Bound::Included(ak), Bound::Excluded(bk)) => match ak.cmp(bk) {
				Ordering::Equal => Ordering::Less,
				other => other,
			},
			(Bound::Excluded(ak), Bound::Included(bk)) => match ak.cmp(bk) {
				Ordering::Equal => Ordering::Greater,
				other => other,
			},
		}
	}

	/// Compare two end bounds for ordering.
	/// Unbounded is considered greater than any bounded value.
	fn compare_end_bounds(a: &Bound<EncodedKey>, b: &Bound<EncodedKey>) -> Ordering {
		match (a, b) {
			(Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
			(Bound::Unbounded, _) => Ordering::Greater,
			(_, Bound::Unbounded) => Ordering::Less,
			(Bound::Included(ak), Bound::Included(bk)) => ak.cmp(bk),
			(Bound::Excluded(ak), Bound::Excluded(bk)) => ak.cmp(bk),
			// Included(x) > Excluded(x) for end bounds
			(Bound::Included(ak), Bound::Excluded(bk)) => match ak.cmp(bk) {
				Ordering::Equal => Ordering::Greater,
				other => other,
			},
			(Bound::Excluded(ak), Bound::Included(bk)) => match ak.cmp(bk) {
				Ordering::Equal => Ordering::Less,
				other => other,
			},
		}
	}

	/// Check if two ranges overlap or are adjacent (can be merged).
	fn ranges_overlap_or_adjacent(
		start1: &Bound<EncodedKey>,
		end1: &Bound<EncodedKey>,
		start2: &Bound<EncodedKey>,
		end2: &Bound<EncodedKey>,
	) -> bool {
		// Two ranges overlap or are adjacent if:
		// 1. end1 >= start2 (range1 extends to or past the start of range2)
		// 2. end2 >= start1 (range2 extends to or past the start of range1)
		Self::end_reaches_start(end1, start2) && Self::end_reaches_start(end2, start1)
	}

	/// Check if an end bound reaches (overlaps or touches) a start bound.
	/// Returns true if the ranges would be adjacent or overlapping.
	fn end_reaches_start(end: &Bound<EncodedKey>, start: &Bound<EncodedKey>) -> bool {
		match (end, start) {
			(Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
			(Bound::Included(e), Bound::Included(s)) => e >= s,
			(Bound::Included(e), Bound::Excluded(s)) => e >= s,
			(Bound::Excluded(e), Bound::Included(s)) => e > s,
			(Bound::Excluded(e), Bound::Excluded(s)) => e >= s,
		}
	}

	pub fn mark_iter(&mut self) {
		self.mark_range(EncodedKeyRange::all());
	}

	#[instrument(name = "transaction::conflict::has_conflict", level = "debug", skip(self, other), fields(
		self_read_keys = self.read_keys.len(),
		self_write_keys = self.write_keys.len(),
		other_write_keys = other.write_keys.len()
	), ret)]
	pub fn has_conflict(&self, other: &Self) -> bool {
		// Fast path: dirty write detection (write-write conflict)
		// Use HashSet intersection for O(min(m,n)) performance
		if !self.write_keys.is_disjoint(&other.write_keys) {
			return true;
		}

		// Fast path: if no reads, no read-write conflicts possible
		if self.read_keys.is_empty() && self.read_ranges.is_empty() && !self.read_all {
			return false;
		}

		// Check single key read-write conflicts - O(min(reads, writes))
		if !self.read_keys.is_disjoint(&other.write_keys) {
			return true;
		}

		// Check full scan conflicts
		if self.read_all && !other.write_keys.is_empty() {
			return true;
		}

		// Check range read-write conflicts using optimized algorithm
		if !self.read_ranges.is_empty() && self.has_any_range_conflict(&other.write_keys) {
			return true;
		}

		false
	}

	/// Check if any of our read ranges conflict with the given conflict keys.
	/// Uses an optimized sweep line algorithm when beneficial.
	#[inline]
	fn has_any_range_conflict(&self, write_keys: &HashSet<EncodedKey>) -> bool {
		if write_keys.is_empty() || self.read_ranges.is_empty() {
			return false;
		}

		// For small sets or few ranges, use simple iteration
		// The threshold balances sorting overhead vs iteration cost
		let use_sweep_line = write_keys.len() >= 32 && self.read_ranges.len() >= 2;

		if use_sweep_line {
			// Sort conflict keys once, then sweep through all ranges
			let mut sorted_keys: Vec<_> = write_keys.iter().collect();
			sorted_keys.sort();
			self.sweep_line_check(&sorted_keys)
		} else {
			// For small sets, linear scan is faster
			self.read_ranges
				.iter()
				.any(|(start, end)| write_keys.iter().any(|key| Self::key_in_range(key, start, end)))
		}
	}

	/// Sweep line algorithm: check all sorted ranges against sorted keys.
	/// Complexity: O(R + K) where R = ranges, K = keys (both already sorted).
	fn sweep_line_check(&self, sorted_keys: &[&EncodedKey]) -> bool {
		if sorted_keys.is_empty() {
			return false;
		}

		let mut key_idx = 0;

		// Ranges are sorted by start bound (invariant from insert_and_merge)
		for (start, end) in &self.read_ranges {
			// Binary search to find first key >= start
			let search_start = match start {
				Bound::Included(s) => {
					sorted_keys[key_idx..].binary_search(&s).unwrap_or_else(|pos| pos)
				}
				Bound::Excluded(s) => match sorted_keys[key_idx..].binary_search(&s) {
					Ok(pos) => pos + 1,
					Err(pos) => pos,
				},
				Bound::Unbounded => 0,
			};

			key_idx += search_start;

			if key_idx >= sorted_keys.len() {
				// No more keys to check
				return false;
			}

			// Check if the first potentially matching key is within the range
			let candidate = sorted_keys[key_idx];
			let in_range = match end {
				Bound::Included(e) => candidate <= e,
				Bound::Excluded(e) => candidate < e,
				Bound::Unbounded => true,
			};

			if in_range {
				return true;
			}

			// Advance key_idx past the current range's end for next iteration
			// This is safe because ranges are non-overlapping and sorted
		}

		false
	}

	#[instrument(name = "transaction::conflict::rollback", level = "trace", skip(self))]
	pub fn rollback(&mut self) {
		self.read_keys.clear();
		self.read_ranges.clear();
		self.read_all = false;
		self.write_keys.clear();
	}

	/// Get all keys that were read by this transaction for efficient
	/// conflict detection
	pub fn get_read_keys(&self) -> &HashSet<EncodedKey> {
		&self.read_keys
	}

	/// Get all keys that were written by this transaction
	pub fn get_write_keys(&self) -> &HashSet<EncodedKey> {
		&self.write_keys
	}

	/// Check if this transaction has any range reads or full scans
	pub fn has_range_operations(&self) -> bool {
		!self.read_ranges.is_empty() || self.read_all
	}

	/// Check if a key falls within a range defined by start and end bounds.
	#[inline]
	fn key_in_range(key: &EncodedKey, start: &Bound<EncodedKey>, end: &Bound<EncodedKey>) -> bool {
		let start_ok = match start {
			Bound::Included(s) => key >= s,
			Bound::Excluded(s) => key > s,
			Bound::Unbounded => true,
		};

		let end_ok = match end {
			Bound::Included(e) => key <= e,
			Bound::Excluded(e) => key < e,
			Bound::Unbounded => true,
		};

		start_ok && end_ok
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn create_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes().to_vec())
	}

	#[test]
	fn test_basic_conflict_detection() {
		let mut cm1 = ConflictManager::new();
		let mut cm2 = ConflictManager::new();

		let key = create_key("test");
		cm1.mark_read(&key);
		cm2.mark_write(&key);

		assert!(cm1.has_conflict(&cm2));
		assert!(!cm2.has_conflict(&cm1)); // Asymmetric
	}

	#[test]
	fn test_write_write_conflict() {
		let mut cm1 = ConflictManager::new();
		let mut cm2 = ConflictManager::new();

		let key = create_key("test");
		cm1.mark_write(&key);
		cm2.mark_write(&key);

		assert!(cm1.has_conflict(&cm2));
		assert!(cm2.has_conflict(&cm1)); // Symmetric for write-write
	}

	#[test]
	fn test_no_conflict_different_keys() {
		let mut cm1 = ConflictManager::new();
		let mut cm2 = ConflictManager::new();

		cm1.mark_read(&create_key("key1"));
		cm1.mark_write(&create_key("key1"));
		cm2.mark_read(&create_key("key2"));
		cm2.mark_write(&create_key("key2"));

		assert!(!cm1.has_conflict(&cm2));
		assert!(!cm2.has_conflict(&cm1));
	}

	#[test]
	fn test_range_conflict() {
		let mut cm1 = ConflictManager::new();
		let mut cm2 = ConflictManager::new();

		// cm1 reads range, cm2 writes within range
		let range = EncodedKeyRange::parse("a..z");
		cm1.mark_range(range);

		cm2.mark_write(&create_key("m")); // "m" is in range "a..z"

		assert!(cm1.has_conflict(&cm2));
	}

	#[test]
	fn test_deduplication() {
		let mut cm = ConflictManager::new();
		let key = create_key("test");

		// Add same key multiple times
		cm.mark_read(&key);
		cm.mark_read(&key);
		cm.mark_read(&key);

		// Should only contain one copy
		assert_eq!(cm.get_read_keys().len(), 1);
	}

	#[test]
	fn test_performance_with_many_keys() {
		let mut cm1 = ConflictManager::new();
		let mut cm2 = ConflictManager::new();

		// Add many keys to test HashSet performance
		for i in 0..1000 {
			cm1.mark_read(&create_key(&format!("read_{}", i)));
			cm2.mark_write(&create_key(&format!("write_{}", i)));
		}

		// Add one overlapping key
		let shared_key = create_key("shared");
		cm1.mark_read(&shared_key);
		cm2.mark_write(&shared_key);

		assert!(cm1.has_conflict(&cm2));
	}

	#[test]
	fn test_iter_functionality() {
		let mut cm1 = ConflictManager::new();
		let mut cm2 = ConflictManager::new();

		cm1.mark_iter(); // Full scan
		cm2.mark_write(&create_key("any_key"));

		assert!(cm1.has_conflict(&cm2));
	}

	#[test]
	fn test_range_merging_overlapping() {
		let mut cm = ConflictManager::new();

		// Add overlapping ranges: [a,c) and [b,d) should merge to [a,d)
		cm.mark_range(EncodedKeyRange::parse("a..c"));
		cm.mark_range(EncodedKeyRange::parse("b..d"));

		// Should have only 1 merged range
		assert_eq!(cm.read_ranges.len(), 1);

		// Merged range should cover both
		let mut cm2 = ConflictManager::new();
		cm2.mark_write(&create_key("a")); // In original first range
		assert!(cm.has_conflict(&cm2));

		let mut cm3 = ConflictManager::new();
		cm3.mark_write(&create_key("c")); // In original second range only
		assert!(cm.has_conflict(&cm3));
	}

	#[test]
	fn test_range_merging_adjacent() {
		let mut cm = ConflictManager::new();

		// Add adjacent ranges: [a,b] and [b,c] should merge to [a,c]
		cm.mark_range(EncodedKeyRange::parse("a..=b"));
		cm.mark_range(EncodedKeyRange::parse("b..=c"));

		// Should have only 1 merged range
		assert_eq!(cm.read_ranges.len(), 1);
	}

	#[test]
	fn test_range_merging_non_overlapping() {
		let mut cm = ConflictManager::new();

		// Add non-overlapping ranges: [a,b) and [c,d) should stay separate
		cm.mark_range(EncodedKeyRange::parse("a..b"));
		cm.mark_range(EncodedKeyRange::parse("c..d"));

		// Should have 2 separate ranges
		assert_eq!(cm.read_ranges.len(), 2);
	}

	#[test]
	fn test_range_merging_multiple() {
		let mut cm = ConflictManager::new();

		// Add three overlapping ranges that should all merge
		cm.mark_range(EncodedKeyRange::parse("a..c"));
		cm.mark_range(EncodedKeyRange::parse("e..g"));
		cm.mark_range(EncodedKeyRange::parse("b..f")); // Overlaps with both

		// All should merge into one range [a,g)
		assert_eq!(cm.read_ranges.len(), 1);
	}

	#[test]
	fn test_escalation_to_read_all() {
		let mut cm = ConflictManager::new();

		// Add more than MAX_RANGES_BEFORE_ESCALATION non-overlapping ranges
		for i in 0..=MAX_RANGES_BEFORE_ESCALATION {
			let start = format!("{:04}", i * 2);
			let end = format!("{:04}", i * 2 + 1);
			let range = EncodedKeyRange::parse(&format!("{}..{}", start, end));
			cm.mark_range(range);
		}

		// Should have escalated to read_all
		assert!(cm.read_all);
		assert!(cm.read_ranges.is_empty());
	}

	#[test]
	fn test_read_all_skips_further_ranges() {
		let mut cm = ConflictManager::new();

		cm.mark_iter(); // Full scan sets read_all
		assert!(cm.read_all);

		// Adding more ranges should be a no-op
		cm.mark_range(EncodedKeyRange::parse("a..z"));
		assert!(cm.read_ranges.is_empty());
	}

	#[test]
	fn test_sweep_line_many_ranges_many_keys() {
		let mut cm1 = ConflictManager::new();
		let mut cm2 = ConflictManager::new();

		// Add many non-overlapping ranges using numeric prefixes for proper ordering
		// e.g., "r_00_a..r_00_z", "r_01_a..r_01_z", etc.
		for i in 0..20 {
			let start = format!("r_{:02}_a", i);
			let end = format!("r_{:02}_z", i);
			let range = EncodedKeyRange::parse(&format!("{}..{}", start, end));
			cm1.mark_range(range);
		}

		// Add many conflict keys in a different namespace (no overlap)
		for i in 0..100 {
			cm2.mark_write(&create_key(&format!("write_{:04}", i)));
		}
		// Add one key that IS in one of the ranges: "r_10_m" is between "r_10_a" and "r_10_z"
		cm2.mark_write(&create_key("r_10_m"));

		assert!(cm1.has_conflict(&cm2));
	}

	#[test]
	fn test_sweep_line_no_conflict() {
		let mut cm1 = ConflictManager::new();
		let mut cm2 = ConflictManager::new();

		// Add ranges in "r_*" namespace
		for i in 0..10 {
			let start = format!("r_{:02}_a", i);
			let end = format!("r_{:02}_z", i);
			let range = EncodedKeyRange::parse(&format!("{}..{}", start, end));
			cm1.mark_range(range);
		}

		// Add conflict keys in "write_*" namespace (no overlap)
		for i in 0..100 {
			cm2.mark_write(&create_key(&format!("write_{:04}", i)));
		}

		assert!(!cm1.has_conflict(&cm2));
	}

	#[test]
	fn test_ranges_sorted_after_insertion() {
		let mut cm = ConflictManager::new();

		// Add ranges out of order
		cm.mark_range(EncodedKeyRange::parse("m..n"));
		cm.mark_range(EncodedKeyRange::parse("a..b"));
		cm.mark_range(EncodedKeyRange::parse("z..zz"));

		// Ranges should be sorted by start bound
		assert_eq!(cm.read_ranges.len(), 3);

		// Verify order by checking first range starts with 'a'
		if let (Bound::Included(start), _) = &cm.read_ranges[0] {
			assert_eq!(start.as_ref(), b"a");
		} else {
			panic!("Expected Included bound");
		}
	}
}
