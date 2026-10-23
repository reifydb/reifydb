// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use core::{cmp::Ordering, ops::Bound};
use std::{collections::HashSet, sync::OnceLock};

use reifydb_core::{
	interface::catalog::storage::StorageId,
	key::{
		any::TaggedKey,
		bound::{TaggedKeyBound, TaggedKeyBoundRange},
	},
};
use reifydb_value::value::partition::Partition;
use tracing::instrument;

const MAX_RANGES_BEFORE_ESCALATION: usize = 1024;

fn compare_start_bounds<T: Ord>(a: &Bound<T>, b: &Bound<T>) -> Ordering {
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

fn compare_end_bounds<T: Ord>(a: &Bound<T>, b: &Bound<T>) -> Ordering {
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

fn end_reaches_start<T: Ord>(end: &Bound<T>, start: &Bound<T>) -> bool {
	match (end, start) {
		(Bound::Unbounded, _) | (_, Bound::Unbounded) => true,
		(Bound::Included(e), Bound::Included(s)) => e >= s,
		(Bound::Included(e), Bound::Excluded(s)) => e >= s,
		(Bound::Excluded(e), Bound::Included(s)) => e > s,
		(Bound::Excluded(e), Bound::Excluded(s)) => e >= s,
	}
}

fn ranges_overlap_or_adjacent<T: Ord>(start1: &Bound<T>, end1: &Bound<T>, start2: &Bound<T>, end2: &Bound<T>) -> bool {
	end_reaches_start(end1, start2) && end_reaches_start(end2, start1)
}

#[derive(Debug, Clone)]
struct RangeSet<T> {
	ranges: Vec<(Bound<T>, Bound<T>)>,
}

impl<T> Default for RangeSet<T> {
	fn default() -> Self {
		Self {
			ranges: Vec::new(),
		}
	}
}

impl<T: Ord + Clone> RangeSet<T> {
	fn len(&self) -> usize {
		self.ranges.len()
	}

	fn is_empty(&self) -> bool {
		self.ranges.is_empty()
	}

	fn clear(&mut self) {
		self.ranges.clear();
	}

	fn insert_and_merge(&mut self, start: Bound<T>, end: Bound<T>) {
		if self.ranges.is_empty() {
			self.ranges.push((start, end));
			return;
		}

		let insert_pos = self
			.ranges
			.binary_search_by(|(existing_start, _)| compare_start_bounds(existing_start, &start))
			.unwrap_or_else(|pos| pos);

		let check_start = insert_pos.saturating_sub(1);

		let mut merge_start = None;
		let mut merge_end = insert_pos;
		let mut merged_start = start.clone();
		let mut merged_end = end.clone();

		for i in check_start..self.ranges.len() {
			let (existing_start, existing_end) = &self.ranges[i];

			if ranges_overlap_or_adjacent(&merged_start, &merged_end, existing_start, existing_end) {
				if merge_start.is_none() {
					merge_start = Some(i);
				}
				merge_end = i + 1;

				if compare_start_bounds(existing_start, &merged_start) == Ordering::Less {
					merged_start = existing_start.clone();
				}
				if compare_end_bounds(existing_end, &merged_end) == Ordering::Greater {
					merged_end = existing_end.clone();
				}
			} else if compare_start_bounds(existing_start, &merged_end) == Ordering::Greater {
				break;
			}
		}

		match merge_start {
			Some(start_idx) => {
				self.ranges.drain(start_idx..merge_end);
				self.ranges.insert(start_idx, (merged_start, merged_end));
			}
			None => {
				self.ranges.insert(insert_pos, (start, end));
			}
		}
	}

	fn any_in_sorted(&self, sorted: &[T]) -> bool {
		self.ranges.iter().any(|(start, end)| {
			let first = match start {
				Bound::Included(s) => sorted.partition_point(|key| key < s),
				Bound::Excluded(s) => sorted.partition_point(|key| key <= s),
				Bound::Unbounded => 0,
			};
			sorted.get(first).is_some_and(|candidate| match end {
				Bound::Included(e) => candidate <= e,
				Bound::Excluded(e) => candidate < e,
				Bound::Unbounded => true,
			})
		})
	}
}

#[derive(Debug, Default, Clone, Copy, PartialEq)]
pub enum ConflictMode {
	#[default]
	Tracking,
	Disabled,
}

#[derive(Debug, Default, Clone)]
pub struct ConflictManager {
	mode: ConflictMode,

	read_keys: HashSet<TaggedKey>,

	read_ranges: RangeSet<TaggedKeyBound>,
	read_all: bool,
	write_keys: HashSet<TaggedKey>,
	member_reads: HashSet<PartitionMember>,
	member_writes: HashSet<PartitionMember>,
	sorted_writes: OnceLock<Vec<TaggedKeyBound>>,
}

pub type PartitionMember = (StorageId, Partition);

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

	#[instrument(name = "transaction::conflict::mark_read", level = "trace", skip(self, key))]
	pub fn mark_read(&mut self, key: &TaggedKey) {
		if self.mode == ConflictMode::Disabled {
			return;
		}
		self.read_keys.insert(key.clone());
	}

	#[instrument(name = "transaction::conflict::mark_write", level = "trace", skip(self, key))]
	pub fn mark_write(&mut self, key: &TaggedKey) {
		if self.mode == ConflictMode::Disabled {
			return;
		}
		if let TaggedKey::PartitionedRow(row) = key {
			self.member_writes.insert((row.storage, row.partition));
		}
		self.write_keys.insert(key.clone());
	}

	pub fn mark_member_read(&mut self, storage: StorageId, partition: Partition) {
		if self.mode == ConflictMode::Disabled {
			return;
		}
		self.member_reads.insert((storage, partition));
	}

	pub fn reserve_writes(&mut self, additional: usize) {
		if self.mode == ConflictMode::Disabled {
			return;
		}
		self.write_keys.reserve(additional);
	}

	#[instrument(name = "transaction::conflict::mark_range", level = "trace", skip(self, range))]
	pub fn mark_range(&mut self, range: TaggedKeyBoundRange) {
		if !self.accepts_range() {
			return;
		}

		if matches!(range.start, Bound::Unbounded) && matches!(range.end, Bound::Unbounded) {
			self.escalate_to_read_all();
			return;
		}

		self.read_ranges.insert_and_merge(range.start, range.end);
		self.escalate_if_saturated();
	}

	fn accepts_range(&self) -> bool {
		self.mode != ConflictMode::Disabled && !self.read_all
	}

	fn escalate_to_read_all(&mut self) {
		self.read_all = true;
		self.read_ranges.clear();
	}

	fn escalate_if_saturated(&mut self) {
		if self.read_ranges.len() > MAX_RANGES_BEFORE_ESCALATION {
			self.escalate_to_read_all();
		}
	}

	pub fn mark_iter(&mut self) {
		self.mark_range(TaggedKeyBoundRange::all());
	}

	#[instrument(name = "transaction::conflict::has_conflict", level = "trace", skip(self, other), fields(
		self_read_keys = self.read_keys.len(),
		self_write_keys = self.write_keys.len(),
		other_write_keys = other.write_keys.len()
	), ret)]
	pub fn has_conflict(&self, other: &Self) -> bool {
		if !self.write_keys.is_disjoint(&other.write_keys) {
			return true;
		}

		if self.read_keys.is_empty() && self.member_reads.is_empty() && !self.has_range_operations() {
			return false;
		}

		if !self.read_keys.is_disjoint(&other.write_keys) {
			return true;
		}

		if !self.member_reads.is_disjoint(&other.member_writes) {
			return true;
		}

		if self.read_all && !other.write_keys.is_empty() {
			return true;
		}

		if self.has_any_range_conflict(other) {
			return true;
		}

		false
	}

	#[inline]
	fn has_any_range_conflict(&self, other: &Self) -> bool {
		if other.write_keys.is_empty() || self.read_ranges.is_empty() {
			return false;
		}
		self.read_ranges.any_in_sorted(other.sorted_writes())
	}

	fn sorted_writes(&self) -> &[TaggedKeyBound] {
		self.sorted_writes.get_or_init(|| {
			let mut sorted: Vec<TaggedKeyBound> =
				self.write_keys.iter().cloned().map(TaggedKeyBound::Key).collect();
			sorted.sort_unstable();
			sorted
		})
	}

	#[instrument(name = "transaction::conflict::rollback", level = "trace", skip(self))]
	pub fn rollback(&mut self) {
		self.read_keys.clear();
		self.read_ranges.clear();
		self.read_all = false;
		self.write_keys.clear();
		self.member_reads.clear();
		self.member_writes.clear();
		self.sorted_writes = OnceLock::new();

		self.mode = ConflictMode::Tracking;
	}

	pub fn get_read_keys(&self) -> &HashSet<TaggedKey> {
		&self.read_keys
	}

	pub fn get_write_keys(&self) -> &HashSet<TaggedKey> {
		&self.write_keys
	}

	pub fn get_member_reads(&self) -> &HashSet<PartitionMember> {
		&self.member_reads
	}

	pub fn get_member_writes(&self) -> &HashSet<PartitionMember> {
		&self.member_writes
	}

	pub fn has_range_operations(&self) -> bool {
		!self.read_ranges.is_empty() || self.read_all
	}
}

#[cfg(test)]
mod tests {
	use reifydb_core::{
		interface::catalog::{
			id::{IndexId, TableId},
			object::ObjectId,
		},
		key::catalog::IndexEntryKey,
		value::index::encoded::EncodedIndexKey,
	};

	use super::*;

	// IndexEntry appends its tail verbatim, so encoded order still matches the raw string order.
	fn create_key(s: &str) -> TaggedKey {
		IndexEntryKey::new(
			ObjectId::Table(TableId(1)),
			IndexId::primary(1u64),
			EncodedIndexKey::new(s.as_bytes()),
		)
		.into()
	}

	fn create_bound(s: &str) -> TaggedKeyBound {
		TaggedKeyBound::Key(create_key(s))
	}

	fn create_range(start: &str, end: Bound<&str>) -> TaggedKeyBoundRange {
		let end = match end {
			Bound::Included(e) => Bound::Included(create_bound(e)),
			Bound::Excluded(e) => Bound::Excluded(create_bound(e)),
			Bound::Unbounded => Bound::Unbounded,
		};
		TaggedKeyBoundRange {
			start: Bound::Included(create_bound(start)),
			end,
		}
	}

	#[test]
	fn test_range_merging_overlapping() {
		let mut cm = ConflictManager::new();

		cm.mark_range(create_range("a", Bound::Excluded("c")));
		cm.mark_range(create_range("b", Bound::Excluded("d")));

		assert_eq!(cm.read_ranges.len(), 1);

		let mut cm2 = ConflictManager::new();
		cm2.mark_write(&create_key("a")); // only in the first input range
		assert!(cm.has_conflict(&cm2));

		let mut cm3 = ConflictManager::new();
		cm3.mark_write(&create_key("c")); // only in the second input range
		assert!(cm.has_conflict(&cm3));
	}

	#[test]
	fn test_range_merging_adjacent() {
		let mut cm = ConflictManager::new();

		cm.mark_range(create_range("a", Bound::Included("b")));
		cm.mark_range(create_range("b", Bound::Included("c")));

		assert_eq!(cm.read_ranges.len(), 1);
	}

	#[test]
	fn test_range_merging_non_overlapping() {
		let mut cm = ConflictManager::new();

		cm.mark_range(create_range("a", Bound::Excluded("b")));
		cm.mark_range(create_range("c", Bound::Excluded("d")));

		assert_eq!(cm.read_ranges.len(), 2);
	}

	#[test]
	fn test_range_merging_multiple() {
		let mut cm = ConflictManager::new();

		cm.mark_range(create_range("a", Bound::Excluded("c")));
		cm.mark_range(create_range("e", Bound::Excluded("g")));
		cm.mark_range(create_range("b", Bound::Excluded("f"))); // bridges the two disjoint ranges

		assert_eq!(cm.read_ranges.len(), 1);
	}

	#[test]
	fn test_escalation_to_read_all() {
		let mut cm = ConflictManager::new();

		for i in 0..=MAX_RANGES_BEFORE_ESCALATION {
			let start = format!("{:04}", i * 2);
			let end = format!("{:04}", i * 2 + 1);
			let range = create_range(&start, Bound::Excluded(&end));
			cm.mark_range(range);
		}

		assert!(cm.read_all);
		assert!(cm.read_ranges.is_empty());
	}

	#[test]
	fn test_read_all_skips_further_ranges() {
		let mut cm = ConflictManager::new();

		cm.mark_iter(); // a full scan escalates straight to read_all
		assert!(cm.read_all);

		cm.mark_range(create_range("a", Bound::Excluded("z")));
		assert!(cm.read_ranges.is_empty());
	}

	#[test]
	fn test_ranges_sorted_after_insertion() {
		let mut cm = ConflictManager::new();

		// Merging relies on start-bound order, so insertion order must not survive.
		cm.mark_range(create_range("m", Bound::Excluded("n")));
		cm.mark_range(create_range("a", Bound::Excluded("b")));
		cm.mark_range(create_range("z", Bound::Excluded("zz")));

		assert_eq!(cm.read_ranges.len(), 3);

		if let (Bound::Included(start), _) = &cm.read_ranges.ranges[0] {
			assert_eq!(start, &create_bound("a"));
		} else {
			panic!("Expected Included bound");
		}
	}

	#[test]
	fn rollback_clears_the_range_set() {
		// A rolled-back transaction must not carry its old ranges into the next conflict check.
		let mut cm = ConflictManager::new();
		cm.mark_range(create_range("a", Bound::Excluded("c")));
		assert!(cm.has_range_operations());

		cm.rollback();

		assert!(!cm.has_range_operations());
		assert!(cm.read_ranges.is_empty());
	}

	#[test]
	fn one_write_inside_a_range_among_many_outside_still_conflicts() {
		// The sorted search must find the single hit and must not invent one among misses.
		let mut cm = ConflictManager::new();
		cm.mark_range(create_range("0100", Bound::Excluded("0200")));
		cm.mark_range(create_range("0300", Bound::Excluded("0400")));

		let mut hits = ConflictManager::new();
		for i in 0..40 {
			hits.mark_write(&create_key(&format!("{:04}", 9000 + i)));
		}
		hits.mark_write(&create_key("0150"));
		assert!(cm.has_conflict(&hits), "the one write inside a range must conflict");

		let mut misses = ConflictManager::new();
		for i in 0..40 {
			misses.mark_write(&create_key(&format!("{:04}", 9000 + i)));
		}
		assert!(!cm.has_conflict(&misses), "writes outside every range must not conflict");
	}
}
