// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use core::{
	cmp::Ordering,
	ops::{Bound, RangeBounds},
};
use std::collections::HashSet;

use reifydb_core::encoded::key::{EncodedKey, EncodedKeyRange};
use reifydb_type::util::hex;
use tracing::instrument;

const MAX_RANGES_BEFORE_ESCALATION: usize = 64;

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum ConflictMode {
	#[default]
	Tracking,
	Disabled,
}

#[derive(Debug, Default, Clone)]
pub struct ConflictManager {
	mode: ConflictMode,

	read_keys: HashSet<EncodedKey>,

	read_ranges: Vec<(Bound<EncodedKey>, Bound<EncodedKey>)>,
	read_all: bool,
	write_keys: HashSet<EncodedKey>,
}

impl ConflictManager {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn disabled() -> Self {
		Self {
			mode: ConflictMode::Disabled,
			..Self::default()
		}
	}

	pub fn set_disabled(&mut self) {
		self.mode = ConflictMode::Disabled;
	}

	#[instrument(name = "transaction::conflict::mark_read", level = "trace", skip(self), fields(key_hex = %hex::display(key.as_ref())))]
	pub fn mark_read(&mut self, key: &EncodedKey) {
		if self.mode == ConflictMode::Disabled {
			return;
		}
		self.read_keys.insert(key.clone());
	}

	#[instrument(name = "transaction::conflict::mark_write", level = "trace", skip(self), fields(key_hex = %hex::display(key.as_ref())))]
	pub fn mark_write(&mut self, key: &EncodedKey) {
		if self.mode == ConflictMode::Disabled {
			return;
		}
		self.write_keys.insert(key.clone());
	}

	pub fn reserve_writes(&mut self, additional: usize) {
		if self.mode == ConflictMode::Disabled {
			return;
		}
		self.write_keys.reserve(additional);
	}

	#[instrument(name = "transaction::conflict::mark_range", level = "trace", skip(self), fields(range_start = ?range.start_bound(), range_end = ?range.end_bound()))]
	pub fn mark_range(&mut self, range: EncodedKeyRange) {
		if self.mode == ConflictMode::Disabled {
			return;
		}

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

		if start == Bound::Unbounded && end == Bound::Unbounded {
			self.read_all = true;
			self.read_ranges.clear();
			return;
		}

		self.insert_and_merge(start, end);

		if self.read_ranges.len() > MAX_RANGES_BEFORE_ESCALATION {
			self.read_all = true;
			self.read_ranges.clear();
		}
	}

	fn insert_and_merge(&mut self, start: Bound<EncodedKey>, end: Bound<EncodedKey>) {
		if self.read_ranges.is_empty() {
			self.read_ranges.push((start, end));
			return;
		}

		let insert_pos = self
			.read_ranges
			.binary_search_by(|(existing_start, _)| Self::compare_start_bounds(existing_start, &start))
			.unwrap_or_else(|pos| pos);

		let check_start = insert_pos.saturating_sub(1);

		let mut merge_start = None;
		let mut merge_end = insert_pos;
		let mut merged_start = start.clone();
		let mut merged_end = end.clone();

		for i in check_start..self.read_ranges.len() {
			let (existing_start, existing_end) = &self.read_ranges[i];

			if Self::ranges_overlap_or_adjacent(&merged_start, &merged_end, existing_start, existing_end) {
				if merge_start.is_none() {
					merge_start = Some(i);
				}
				merge_end = i + 1;

				if Self::compare_start_bounds(existing_start, &merged_start) == Ordering::Less {
					merged_start = existing_start.clone();
				}
				if Self::compare_end_bounds(existing_end, &merged_end) == Ordering::Greater {
					merged_end = existing_end.clone();
				}
			} else if Self::compare_start_bounds(existing_start, &merged_end) == Ordering::Greater {
				break;
			}
		}

		match merge_start {
			Some(start_idx) => {
				self.read_ranges.drain(start_idx..merge_end);
				self.read_ranges.insert(start_idx, (merged_start, merged_end));
			}
			None => {
				self.read_ranges.insert(insert_pos, (start, end));
			}
		}
	}

	fn compare_start_bounds(a: &Bound<EncodedKey>, b: &Bound<EncodedKey>) -> Ordering {
		match (a, b) {
			(Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
			(Bound::Unbounded, _) => Ordering::Less,
			(_, Bound::Unbounded) => Ordering::Greater,
			(Bound::Included(ak), Bound::Included(bk)) => ak.cmp(bk),
			(Bound::Excluded(ak), Bound::Excluded(bk)) => ak.cmp(bk),

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

	fn compare_end_bounds(a: &Bound<EncodedKey>, b: &Bound<EncodedKey>) -> Ordering {
		match (a, b) {
			(Bound::Unbounded, Bound::Unbounded) => Ordering::Equal,
			(Bound::Unbounded, _) => Ordering::Greater,
			(_, Bound::Unbounded) => Ordering::Less,
			(Bound::Included(ak), Bound::Included(bk)) => ak.cmp(bk),
			(Bound::Excluded(ak), Bound::Excluded(bk)) => ak.cmp(bk),

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

	fn ranges_overlap_or_adjacent(
		start1: &Bound<EncodedKey>,
		end1: &Bound<EncodedKey>,
		start2: &Bound<EncodedKey>,
		end2: &Bound<EncodedKey>,
	) -> bool {
		Self::end_reaches_start(end1, start2) && Self::end_reaches_start(end2, start1)
	}

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
		if !self.write_keys.is_disjoint(&other.write_keys) {
			return true;
		}

		if self.read_keys.is_empty() && self.read_ranges.is_empty() && !self.read_all {
			return false;
		}

		if !self.read_keys.is_disjoint(&other.write_keys) {
			return true;
		}

		if self.read_all && !other.write_keys.is_empty() {
			return true;
		}

		if !self.read_ranges.is_empty() && self.has_any_range_conflict(&other.write_keys) {
			return true;
		}

		false
	}

	#[inline]
	fn has_any_range_conflict(&self, write_keys: &HashSet<EncodedKey>) -> bool {
		if write_keys.is_empty() || self.read_ranges.is_empty() {
			return false;
		}

		let use_sweep_line = write_keys.len() >= 32 && self.read_ranges.len() >= 2;

		if use_sweep_line {
			let mut sorted_keys: Vec<_> = write_keys.iter().collect();
			sorted_keys.sort();
			self.sweep_line_check(&sorted_keys)
		} else {
			self.read_ranges
				.iter()
				.any(|(start, end)| write_keys.iter().any(|key| Self::key_in_range(key, start, end)))
		}
	}

	fn sweep_line_check(&self, sorted_keys: &[&EncodedKey]) -> bool {
		if sorted_keys.is_empty() {
			return false;
		}

		let mut key_idx = 0;

		for (start, end) in &self.read_ranges {
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
				return false;
			}

			let candidate = sorted_keys[key_idx];
			let in_range = match end {
				Bound::Included(e) => candidate <= e,
				Bound::Excluded(e) => candidate < e,
				Bound::Unbounded => true,
			};

			if in_range {
				return true;
			}
		}

		false
	}

	#[instrument(name = "transaction::conflict::rollback", level = "trace", skip(self))]
	pub fn rollback(&mut self) {
		self.read_keys.clear();
		self.read_ranges.clear();
		self.read_all = false;
		self.write_keys.clear();

		self.mode = ConflictMode::Tracking;
	}

	pub fn get_read_keys(&self) -> &HashSet<EncodedKey> {
		&self.read_keys
	}

	pub fn get_write_keys(&self) -> &HashSet<EncodedKey> {
		&self.write_keys
	}

	pub fn has_range_operations(&self) -> bool {
		!self.read_ranges.is_empty() || self.read_all
	}

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
