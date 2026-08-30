// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Reverse,
	collections::{BTreeMap, HashMap, HashSet, btree_map},
	iter::Peekable,
	ops::RangeBounds,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::common::CommitVersion;
use reifydb_value::reifydb_assertions;

use super::entry::{Value, entry_bytes_with};

pub(super) type VersionMap = BTreeMap<Reverse<CommitVersion>, Value>;

#[derive(Default)]
pub(super) struct RowMap {
	entries: BTreeMap<EncodedKey, VersionMap>,
	current_bytes: u64,
	historical_bytes: u64,
	min_version: Option<CommitVersion>,
}

impl RowMap {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn insert(&mut self, key: EncodedKey, version: CommitVersion, value: Value) {
		let key_heap = key.heap_bytes();
		let bytes = entry_bytes_with(key_heap, &value);

		let versions = self.entries.entry(key).or_default();
		let previous_newest = versions.keys().next().map(|Reverse(v)| *v);
		let replaced = versions.insert(Reverse(version), value);
		let stays_newest = previous_newest.is_none_or(|newest| version >= newest);
		let demoted_bytes = previous_newest
			.filter(|newest| version > *newest)
			.and_then(|newest| versions.get(&Reverse(newest)))
			.map(|demoted| entry_bytes_with(key_heap, demoted));

		if let Some(replaced) = replaced {
			let replaced_bytes = entry_bytes_with(key_heap, &replaced);
			if previous_newest == Some(version) {
				self.current_bytes = self.current_bytes.saturating_sub(replaced_bytes);
			} else {
				self.historical_bytes = self.historical_bytes.saturating_sub(replaced_bytes);
			}
		}

		if let Some(demoted) = demoted_bytes {
			self.current_bytes = self.current_bytes.saturating_sub(demoted);
			self.historical_bytes = self.historical_bytes.saturating_add(demoted);
		}

		if stays_newest {
			self.current_bytes = self.current_bytes.saturating_add(bytes);
		} else {
			self.historical_bytes = self.historical_bytes.saturating_add(bytes);
		}

		self.min_version = Some(self.min_version.map_or(version, |current| current.min(version)));
	}

	pub fn get(&self, key: &[u8], version: CommitVersion) -> Option<(CommitVersion, &Value)> {
		self.entries
			.get(key)
			.and_then(|versions| versions.range(Reverse(version)..).next())
			.map(|(Reverse(found), value)| (*found, value))
	}

	pub fn versions_for(&self, key: &[u8]) -> Option<&VersionMap> {
		self.entries.get(key)
	}

	pub fn range<R>(&self, bounds: R) -> btree_map::Range<'_, EncodedKey, VersionMap>
	where
		R: RangeBounds<[u8]>,
	{
		self.entries.range::<[u8], R>(bounds)
	}

	pub fn iter(&self) -> btree_map::Iter<'_, EncodedKey, VersionMap> {
		self.entries.iter()
	}

	pub fn key_count(&self) -> usize {
		self.entries.len()
	}

	pub fn current_bytes(&self) -> u64 {
		self.current_bytes
	}

	pub fn historical_bytes(&self) -> u64 {
		self.historical_bytes
	}

	pub fn bytes(&self) -> u64 {
		self.current_bytes.saturating_add(self.historical_bytes)
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	pub fn min_version(&self) -> Option<CommitVersion> {
		self.min_version
	}

	pub fn without(&self, dropped: &HashMap<EncodedKey, HashSet<CommitVersion>>) -> (RowMap, Vec<Removed>) {
		let mut kept = RowMap::new();
		let mut removed = Vec::new();
		for (key, versions) in self.entries.iter() {
			let dropped_here = dropped.get(key);
			for (Reverse(version), value) in versions.iter() {
				if dropped_here.is_some_and(|set| set.contains(version)) {
					removed.push(Removed {
						key: key.clone(),
						version: *version,
						value: value.clone(),
					});
				} else {
					kept.insert(key.clone(), *version, value.clone());
				}
			}
		}
		(kept, removed)
	}
}

pub(super) struct Removed {
	pub key: EncodedKey,
	pub version: CommitVersion,
	pub value: Value,
}

pub(super) fn newest_across<'a>(
	maps: impl Iterator<Item = &'a VersionMap>,
	version: CommitVersion,
) -> Option<(CommitVersion, &'a Value)> {
	maps.filter_map(|versions| {
		versions.range(Reverse(version)..).next().map(|(Reverse(found), value)| (*found, value))
	})
	.max_by_key(|(found, _)| *found)
}

pub(super) struct MergedRows<'a, I>
where
	I: Iterator<Item = (&'a EncodedKey, &'a VersionMap)>,
{
	iters: Vec<Peekable<I>>,
	reverse: bool,
	group: Vec<&'a VersionMap>,
}

impl<'a, I> MergedRows<'a, I>
where
	I: Iterator<Item = (&'a EncodedKey, &'a VersionMap)>,
{
	pub fn new(iters: Vec<I>, reverse: bool) -> Self {
		let group = Vec::with_capacity(iters.len());
		Self {
			iters: iters.into_iter().map(|iter| iter.peekable()).collect(),
			reverse,
			group,
		}
	}

	pub fn next_group(&mut self) -> Option<(&'a EncodedKey, &[&'a VersionMap])> {
		let mut target: Option<&'a EncodedKey> = None;
		for iter in self.iters.iter_mut() {
			let Some((key, _)) = iter.peek() else {
				continue;
			};
			target = match target {
				None => Some(key),
				Some(best) if self.reverse && *key > best => Some(key),
				Some(best) if !self.reverse && *key < best => Some(key),
				keep => keep,
			};
		}
		let target = target?;

		self.group.clear();
		for iter in self.iters.iter_mut() {
			if iter.peek().is_some_and(|(key, _)| *key == target) {
				let (_, versions) = iter.next().expect("a peeked iterator yields");
				self.group.push(versions);
			}
		}
		Some((target, &self.group))
	}
}

#[derive(Default)]
pub(super) struct ActiveRows {
	rows: RowMap,
}

impl ActiveRows {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn rows(&self) -> &RowMap {
		&self.rows
	}

	pub fn insert(&mut self, key: EncodedKey, version: CommitVersion, value: Value) {
		self.rows.insert(key, version, value);
	}

	pub fn min_version(&self) -> Option<CommitVersion> {
		self.rows.min_version()
	}

	pub fn compact(&mut self, dropped: &HashMap<EncodedKey, HashSet<CommitVersion>>) -> Vec<Removed> {
		let (rows, removed) = self.rows.without(dropped);
		self.rows = rows;
		removed
	}

	pub fn bytes(&self) -> u64 {
		self.rows.bytes()
	}

	pub fn is_empty(&self) -> bool {
		self.rows.is_empty()
	}

	pub fn close(self) -> ClosedRows {
		reifydb_assertions! {
			assert!(
				!self.rows.is_empty(),
				"closing an empty active map mints a closed map with no version range, and the flush \
				 orders closed maps by that range"
			);
		}
		let min_version = self.rows.min_version().expect("a non-empty row map has a minimum version");
		ClosedRows {
			rows: self.rows,
			min_version,
		}
	}
}

pub(super) struct ClosedRows {
	rows: RowMap,
	min_version: CommitVersion,
}

pub(super) struct CompactedRows {
	pub rows: ClosedRows,
	pub removed: Vec<Removed>,
}

impl ClosedRows {
	pub fn rows(&self) -> &RowMap {
		&self.rows
	}

	pub fn min_version(&self) -> CommitVersion {
		self.min_version
	}

	pub fn compact(&self, dropped: &HashMap<EncodedKey, HashSet<CommitVersion>>) -> CompactedRows {
		let (rows, removed) = self.rows.without(dropped);
		CompactedRows {
			rows: ClosedRows {
				min_version: rows.min_version().unwrap_or(self.min_version),
				rows,
			},
			removed,
		}
	}
}

#[cfg(test)]
mod tests {
	use std::ops::Bound;

	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	fn key(name: &str) -> EncodedKey {
		EncodedKey::new(name.as_bytes().to_vec())
	}

	fn val(bytes: &str) -> Value {
		Some(CowVec::new(bytes.as_bytes().to_vec()))
	}

	fn walked_bytes(rows: &RowMap) -> u64 {
		rows.iter()
			.map(|(key, versions)| {
				let heap = key.heap_bytes();
				versions.values().map(|value| entry_bytes_with(heap, value)).sum::<u64>()
			})
			.sum()
	}

	fn walked_version_count(rows: &RowMap) -> usize {
		rows.iter().map(|(_, versions)| versions.len()).sum()
	}

	fn walked_current_bytes(rows: &RowMap) -> u64 {
		rows.iter()
			.filter_map(|(key, versions)| {
				versions.values().next().map(|value| entry_bytes_with(key.heap_bytes(), value))
			})
			.sum()
	}

	#[test]
	fn a_lone_version_of_a_key_is_billed_as_current() {
		// Every key's newest version is its current one; billing a first write as historical understates
		// the current residency the flush uses to size a slice.
		let mut rows = RowMap::new();
		rows.insert(key("a"), CommitVersion(1), val("one"));

		assert_eq!(rows.key_count(), 1);
		assert_eq!(walked_version_count(&rows), 1);
		assert_eq!(rows.historical_bytes(), 0, "a key with a single version has no history");
		assert_eq!(rows.current_bytes(), walked_current_bytes(&rows));
	}

	#[test]
	fn a_newer_version_demotes_the_previous_newest_into_history() {
		// The newest version must move to historical when it is superseded, otherwise both versions are
		// billed as current and the current residency double-counts the key.
		let mut rows = RowMap::new();
		rows.insert(key("a"), CommitVersion(1), val("one"));
		let first = rows.current_bytes();
		rows.insert(key("a"), CommitVersion(2), val("twotwo"));

		assert_eq!(rows.key_count(), 1, "a second version is not a second key");
		assert_eq!(walked_version_count(&rows), 2);
		assert_eq!(rows.historical_bytes(), first, "the superseded version carries its own bytes down");
		assert_eq!(rows.current_bytes(), walked_current_bytes(&rows));
		assert_eq!(rows.bytes(), walked_bytes(&rows));
	}

	#[test]
	fn a_version_landing_below_the_newest_never_disturbs_current() {
		// Commits can land out of order; an older version must be billed as history without moving the
		// standing current version, or the current tally drifts on every late arrival.
		let mut rows = RowMap::new();
		rows.insert(key("a"), CommitVersion(5), val("five"));
		let current_after_newest = rows.current_bytes();
		rows.insert(key("a"), CommitVersion(2), val("two"));

		assert_eq!(rows.current_bytes(), current_after_newest, "a late older version is not current");
		assert_eq!(walked_version_count(&rows), 2);
		assert_eq!(rows.bytes(), walked_bytes(&rows));
	}

	#[test]
	fn rewriting_a_version_in_place_rebills_it_rather_than_stacking() {
		// Re-inserting a version already present replaces it; counting the new bytes without retiring the
		// old ones leaks residency that nothing will ever reclaim.
		let mut rows = RowMap::new();
		rows.insert(key("a"), CommitVersion(1), val("short"));
		rows.insert(key("a"), CommitVersion(1), val("a much longer value"));

		assert_eq!(walked_version_count(&rows), 1, "a rewrite is not a new version");
		assert_eq!(rows.current_bytes(), walked_current_bytes(&rows));
		assert_eq!(rows.bytes(), walked_bytes(&rows));
	}

	#[test]
	fn rewriting_a_historical_version_in_place_rebills_only_history() {
		// The same rewrite one version below the newest must settle against the historical tally; billing
		// it to current would inflate the residency the flush sizes its slice from.
		let mut rows = RowMap::new();
		rows.insert(key("a"), CommitVersion(2), val("newest"));
		let current = rows.current_bytes();
		rows.insert(key("a"), CommitVersion(1), val("old"));
		rows.insert(key("a"), CommitVersion(1), val("an old value made longer"));

		assert_eq!(rows.current_bytes(), current, "the newest version was never touched");
		assert_eq!(walked_version_count(&rows), 2);
		assert_eq!(rows.bytes(), walked_bytes(&rows));
	}

	#[test]
	fn the_tally_matches_a_full_walk_across_mixed_mutations() {
		// The counters are maintained incrementally, so any path that forgets an adjustment shows up only
		// as drift against an independent walk of what is actually stored.
		let mut rows = RowMap::new();
		rows.insert(key("a"), CommitVersion(1), val("a1"));
		rows.insert(key("b"), CommitVersion(1), val("b1"));
		rows.insert(key("a"), CommitVersion(3), val("a3-longer"));
		rows.insert(key("a"), CommitVersion(2), val("a2"));
		rows.insert(key("b"), CommitVersion(4), val("b4"));
		rows.insert(key("c"), CommitVersion(2), None);
		rows.insert(key("b"), CommitVersion(4), val("b4-rewritten"));

		assert_eq!(rows.bytes(), walked_bytes(&rows));
		assert_eq!(rows.current_bytes(), walked_current_bytes(&rows));
		assert_eq!(rows.historical_bytes(), walked_bytes(&rows) - walked_current_bytes(&rows));
		assert_eq!(rows.key_count(), 3);
		assert_eq!(walked_version_count(&rows), 6);
	}

	#[test]
	fn a_tombstone_is_a_version_like_any_other() {
		// A delete is stored as a none value and still occupies a version; skipping it would let a deleted
		// key read through to the value it shadowed.
		let mut rows = RowMap::new();
		rows.insert(key("a"), CommitVersion(1), val("live"));
		rows.insert(key("a"), CommitVersion(2), None);

		assert_eq!(walked_version_count(&rows), 2);
		assert_eq!(rows.get(b"a", CommitVersion(2)), Some((CommitVersion(2), &None)));
		assert_eq!(rows.bytes(), walked_bytes(&rows));
	}

	#[test]
	fn a_read_sees_the_newest_version_at_or_below_the_asked_one() {
		// A read at version v must not see writes above v, and must not skip past the newest write at or
		// below it, or a snapshot read returns a row from the wrong point in time.
		let mut rows = RowMap::new();
		rows.insert(key("a"), CommitVersion(1), val("one"));
		rows.insert(key("a"), CommitVersion(3), val("three"));
		rows.insert(key("a"), CommitVersion(5), val("five"));

		assert_eq!(rows.get(b"a", CommitVersion(4)), Some((CommitVersion(3), &val("three"))));
		assert_eq!(rows.get(b"a", CommitVersion(5)), Some((CommitVersion(5), &val("five"))));
		assert_eq!(rows.get(b"a", CommitVersion(2)), Some((CommitVersion(1), &val("one"))));
		assert_eq!(rows.get(b"a", CommitVersion(0)), None, "nothing was written at or below version 0");
		assert_eq!(rows.get(b"missing", CommitVersion(5)), None);
	}

	#[test]
	fn the_oldest_version_is_the_smallest_whatever_the_insertion_order() {
		// The flush gates on this version, so one that tracks insertion order rather than version order
		// would let a map be skipped while it still holds writes below the cutoff.
		let mut rows = RowMap::new();
		rows.insert(key("a"), CommitVersion(7), val("seven"));
		rows.insert(key("b"), CommitVersion(2), val("two"));
		rows.insert(key("c"), CommitVersion(9), val("nine"));
		rows.insert(key("d"), CommitVersion(4), val("four"));

		assert_eq!(rows.min_version(), Some(CommitVersion(2)));
	}

	#[test]
	fn an_empty_row_map_has_no_oldest_version() {
		// A closed map with no oldest version cannot be gated against a cutoff, which is why closing an
		// empty active map is refused rather than defaulted to zero.
		let rows = RowMap::new();

		assert_eq!(rows.min_version(), None);
		assert!(rows.is_empty());
		assert_eq!(rows.bytes(), 0);
	}

	#[test]
	fn closing_records_the_oldest_version_of_the_rows_it_holds() {
		// Every flush gate skips a closed map by this version, so one above the map's true oldest write
		// hides that write from the sweep forever.
		let mut active = ActiveRows::new();
		active.insert(key("a"), CommitVersion(3), val("three"));
		active.insert(key("b"), CommitVersion(8), val("eight"));

		let closed = active.close();

		assert_eq!(closed.min_version(), CommitVersion(3));
	}

	#[test]
	fn range_walks_keys_in_order_within_the_bounds() {
		// The commit-buffer scan merges this iterator against the closed maps by key, so it must yield keys
		// in ascending order and honour the bounds it was given.
		let mut rows = RowMap::new();
		for name in ["a", "b", "c", "d"] {
			rows.insert(key(name), CommitVersion(1), val(name));
		}

		let seen: Vec<&[u8]> = rows
			.range::<(Bound<&[u8]>, Bound<&[u8]>)>((
				Bound::Excluded(b"a".as_slice()),
				Bound::Included(b"c".as_slice()),
			))
			.map(|(key, _)| key.as_slice())
			.collect();

		assert_eq!(seen, vec![b"b".as_slice(), b"c".as_slice()]);
	}
}
