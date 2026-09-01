// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::{Ordering, Reverse},
	collections::{BTreeMap, BinaryHeap, HashMap, HashSet, btree_map},
	ops::RangeBounds,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, metrics::heap::HeapSize};
use reifydb_value::reifydb_assertions;

use crate::entry::{Value, entry_bytes_with};

pub(super) type VersionMap = BTreeMap<Reverse<CommitVersion>, Value>;

#[derive(Clone, Default)]
pub(super) struct RowMap {
	entries: BTreeMap<EncodedKey, VersionMap>,
	current_bytes: u64,
	historical_bytes: u64,
	versions: BTreeMap<CommitVersion, u32>,
}

impl RowMap {
	pub fn insert(&mut self, key: EncodedKey, version: CommitVersion, value: Value) {
		let key_heap = key.heap_size();
		let bytes = entry_bytes_with(key_heap, &value);

		let versions = self.entries.entry(key).or_default();
		let previous_newest = versions.keys().next().map(|Reverse(v)| *v);
		let replaced = versions.insert(Reverse(version), value);
		let stays_newest = previous_newest.is_none_or(|newest| version >= newest);
		let demoted_bytes = previous_newest
			.filter(|newest| version > *newest)
			.and_then(|newest| versions.get(&Reverse(newest)))
			.map(|demoted| entry_bytes_with(key_heap, demoted));

		match replaced {
			Some(replaced) => {
				let replaced_bytes = entry_bytes_with(key_heap, &replaced);
				if previous_newest == Some(version) {
					self.current_bytes = self.current_bytes.saturating_sub(replaced_bytes);
				} else {
					self.historical_bytes = self.historical_bytes.saturating_sub(replaced_bytes);
				}
			}
			None => *self.versions.entry(version).or_insert(0) += 1,
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
	}

	pub fn remove(&mut self, dropped: &HashMap<EncodedKey, HashSet<CommitVersion>>) -> Vec<Removed> {
		let mut removed = Vec::new();
		for (key, versions) in dropped {
			let Some(held) = self.entries.get_mut(key) else {
				continue;
			};
			let key_heap = key.heap_size();
			for version in versions {
				let was_newest = held.keys().next() == Some(&Reverse(*version));
				let Some(value) = held.remove(&Reverse(*version)) else {
					continue;
				};
				let bytes = entry_bytes_with(key_heap, &value);
				if was_newest {
					self.current_bytes = self.current_bytes.saturating_sub(bytes);
					if let Some((_, promoted)) = held.iter().next() {
						let promoted = entry_bytes_with(key_heap, promoted);
						self.historical_bytes = self.historical_bytes.saturating_sub(promoted);
						self.current_bytes = self.current_bytes.saturating_add(promoted);
					}
				} else {
					self.historical_bytes = self.historical_bytes.saturating_sub(bytes);
				}
				Self::forget(&mut self.versions, *version);
				removed.push(Removed {
					key: key.clone(),
					version: *version,
					value,
				});
			}
			if held.is_empty() {
				self.entries.remove(key);
			}
		}
		removed
	}

	fn forget(versions: &mut BTreeMap<CommitVersion, u32>, version: CommitVersion) {
		let count = versions.get_mut(&version).expect("every stored version is counted");
		*count -= 1;
		if *count == 0 {
			versions.remove(&version);
		}
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
		self.versions.keys().next().copied()
	}

	pub fn max_version(&self) -> Option<CommitVersion> {
		self.versions.keys().next_back().copied()
	}
}

pub(super) struct Removed {
	pub key: EncodedKey,
	pub version: CommitVersion,
	pub value: Value,
}

pub(super) fn lookup<'a>(
	maps: impl Iterator<Item = &'a RowMap>,
	key: &[u8],
	version: CommitVersion,
) -> Option<(CommitVersion, &'a Value)> {
	let mut best: Option<(CommitVersion, &'a Value)> = None;
	for rows in maps {
		if rows.min_version().is_none_or(|min| min > version) {
			continue;
		}
		if let Some((found, _)) = best
			&& rows.max_version().is_some_and(|max| max <= found)
		{
			continue;
		}
		if let Some((found, value)) = rows.get(key, version)
			&& best.is_none_or(|(best, _)| found > best)
		{
			best = Some((found, value));
		}
	}
	best
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

struct Head<'a> {
	key: &'a EncodedKey,
	versions: &'a VersionMap,
	source: usize,
	reverse: bool,
}

impl PartialEq for Head<'_> {
	fn eq(&self, other: &Self) -> bool {
		self.key == other.key
	}
}

impl Eq for Head<'_> {}

impl PartialOrd for Head<'_> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Head<'_> {
	fn cmp(&self, other: &Self) -> Ordering {
		let order = self.key.cmp(other.key);
		if self.reverse {
			order
		} else {
			order.reverse()
		}
	}
}

pub(super) struct MergedRows<'a, I>
where
	I: Iterator<Item = (&'a EncodedKey, &'a VersionMap)>,
{
	iters: Vec<I>,
	heads: BinaryHeap<Head<'a>>,
	reverse: bool,
	group: Vec<&'a VersionMap>,
}

impl<'a, I> MergedRows<'a, I>
where
	I: Iterator<Item = (&'a EncodedKey, &'a VersionMap)>,
{
	pub fn new(mut iters: Vec<I>, reverse: bool) -> Self {
		let mut heads = BinaryHeap::with_capacity(iters.len());
		for (source, iter) in iters.iter_mut().enumerate() {
			if let Some((key, versions)) = iter.next() {
				heads.push(Head {
					key,
					versions,
					source,
					reverse,
				});
			}
		}
		Self {
			iters,
			heads,
			reverse,
			group: Vec::new(),
		}
	}

	fn advance(&mut self, source: usize) {
		if let Some((key, versions)) = self.iters[source].next() {
			self.heads.push(Head {
				key,
				versions,
				source,
				reverse: self.reverse,
			});
		}
	}

	pub fn next_group(&mut self) -> Option<(&'a EncodedKey, &[&'a VersionMap])> {
		let first = self.heads.pop()?;
		let target = first.key;
		self.group.clear();
		self.group.push(first.versions);
		self.advance(first.source);
		while self.heads.peek().is_some_and(|head| head.key == target) {
			let head = self.heads.pop().expect("a peeked heap yields");
			self.group.push(head.versions);
			self.advance(head.source);
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
		self.rows.remove(dropped)
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
		ClosedRows {
			rows: self.rows,
		}
	}
}

#[derive(Clone)]
pub(super) struct ClosedRows {
	rows: RowMap,
}

impl ClosedRows {
	pub fn rows(&self) -> &RowMap {
		&self.rows
	}

	pub fn min_version(&self) -> CommitVersion {
		self.rows.min_version().expect("a closed map holds at least one row")
	}

	pub fn compact(&mut self, dropped: &HashMap<EncodedKey, HashSet<CommitVersion>>) -> Vec<Removed> {
		self.rows.remove(dropped)
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
				let heap = key.heap_size();
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
				versions.values().next().map(|value| entry_bytes_with(key.heap_size(), value))
			})
			.sum()
	}

	#[test]
	fn a_lone_version_of_a_key_is_billed_as_current() {
		// Every key's newest version is its current one; billing a first write as historical understates
		// the current residency the flush uses to size a slice.
		let mut rows = RowMap::default();
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
		let mut rows = RowMap::default();
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
		let mut rows = RowMap::default();
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
		let mut rows = RowMap::default();
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
		let mut rows = RowMap::default();
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
		let mut rows = RowMap::default();
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
		let mut rows = RowMap::default();
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
		let mut rows = RowMap::default();
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
		let mut rows = RowMap::default();
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
		let rows = RowMap::default();

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
		let mut rows = RowMap::default();
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

	#[test]
	fn removing_the_newest_version_promotes_the_next_one_into_current() {
		// The flush drops a key's newest version when it moves to the persistent tier; the version left
		// behind becomes current, and a tally that leaves it billed as history under-reports the residency
		// the flush sizes its next slice from.
		let mut rows = RowMap::default();
		rows.insert(key("a"), CommitVersion(1), val("one"));
		rows.insert(key("a"), CommitVersion(2), val("two-two"));
		rows.insert(key("a"), CommitVersion(3), val("three"));

		let removed = rows.remove(&HashMap::from([(key("a"), HashSet::from([CommitVersion(3)]))]));

		assert_eq!(removed.len(), 1);
		assert_eq!(removed[0].version, CommitVersion(3));
		assert_eq!(removed[0].value, val("three"));
		assert_eq!(walked_version_count(&rows), 2);
		assert_eq!(rows.current_bytes(), walked_current_bytes(&rows));
		assert_eq!(rows.bytes(), walked_bytes(&rows));
		assert_eq!(rows.max_version(), Some(CommitVersion(2)));
		assert_eq!(rows.get(b"a", CommitVersion(9)), Some((CommitVersion(2), &val("two-two"))));
	}

	#[test]
	fn removing_a_historical_version_touches_only_the_historical_tally() {
		// Historical GC only ever drops superseded versions; if that reached into the current tally the
		// flush would size slices from a residency the buffer no longer holds.
		let mut rows = RowMap::default();
		rows.insert(key("a"), CommitVersion(1), val("one"));
		rows.insert(key("a"), CommitVersion(2), val("two"));
		let current = rows.current_bytes();

		rows.remove(&HashMap::from([(key("a"), HashSet::from([CommitVersion(1)]))]));

		assert_eq!(rows.current_bytes(), current);
		assert_eq!(rows.historical_bytes(), 0);
		assert_eq!(rows.min_version(), Some(CommitVersion(2)));
		assert_eq!(walked_version_count(&rows), 1);
	}

	#[test]
	fn removing_every_version_of_a_key_forgets_the_key_and_its_versions() {
		// A key with no versions left must not linger as an empty entry: it would count as a stored key
		// and keep the map's version range alive for a flush gate that has nothing left to flush.
		let mut rows = RowMap::default();
		rows.insert(key("a"), CommitVersion(4), val("four"));
		rows.insert(key("b"), CommitVersion(6), val("six"));

		let removed = rows.remove(&HashMap::from([(key("a"), HashSet::from([CommitVersion(4)]))]));

		assert_eq!(removed.len(), 1);
		assert_eq!(rows.key_count(), 1);
		assert_eq!(rows.min_version(), Some(CommitVersion(6)));
		assert_eq!(rows.max_version(), Some(CommitVersion(6)));
		assert_eq!(rows.bytes(), walked_bytes(&rows));

		rows.remove(&HashMap::from([(key("b"), HashSet::from([CommitVersion(6)]))]));

		assert!(rows.is_empty());
		assert_eq!(rows.min_version(), None);
		assert_eq!(rows.bytes(), 0);
	}

	#[test]
	fn removing_a_version_the_map_does_not_hold_changes_nothing() {
		// A drop batch names versions across every map of a kind; a map that holds the key at other
		// versions only must leave them and its tallies alone.
		let mut rows = RowMap::default();
		rows.insert(key("a"), CommitVersion(2), val("two"));
		let bytes = rows.bytes();

		let removed = rows.remove(&HashMap::from([
			(key("a"), HashSet::from([CommitVersion(1)])),
			(key("zz"), HashSet::from([CommitVersion(2)])),
		]));

		assert!(removed.is_empty());
		assert_eq!(rows.bytes(), bytes);
		assert_eq!(walked_version_count(&rows), 1);
		assert_eq!(rows.min_version(), Some(CommitVersion(2)));
	}

	#[test]
	fn a_version_shared_by_two_keys_stays_in_range_until_both_are_gone() {
		// One commit writes many keys at the same version; forgetting the version when the first key
		// leaves would lift the map's oldest version above rows it still holds.
		let mut rows = RowMap::default();
		rows.insert(key("a"), CommitVersion(1), val("a"));
		rows.insert(key("b"), CommitVersion(1), val("b"));
		rows.insert(key("c"), CommitVersion(2), val("c"));

		rows.remove(&HashMap::from([(key("a"), HashSet::from([CommitVersion(1)]))]));
		assert_eq!(rows.min_version(), Some(CommitVersion(1)));

		rows.remove(&HashMap::from([(key("b"), HashSet::from([CommitVersion(1)]))]));
		assert_eq!(rows.min_version(), Some(CommitVersion(2)));
	}

	#[test]
	fn merging_yields_each_key_once_in_order_with_every_map_that_holds_it() {
		// The range scan resolves a key's version from the group it is handed; a key split into two
		// groups would surface twice, and a group missing a map would read a stale version.
		let mut a = RowMap::default();
		let mut b = RowMap::default();
		let mut c = RowMap::default();
		for name in ["a", "c", "e"] {
			a.insert(key(name), CommitVersion(1), val(name));
		}
		for name in ["b", "c"] {
			b.insert(key(name), CommitVersion(2), val(name));
		}
		for name in ["c", "d"] {
			c.insert(key(name), CommitVersion(3), val(name));
		}
		let maps = [&a, &b, &c];
		let unbounded = || (Bound::<&[u8]>::Unbounded, Bound::<&[u8]>::Unbounded);

		let mut forward = MergedRows::new(maps.iter().map(|rows| rows.range(unbounded())).collect(), false);
		let mut seen = Vec::new();
		while let Some((key, group)) = forward.next_group() {
			seen.push((key.as_slice().to_vec(), group.len()));
		}
		assert_eq!(
			seen,
			vec![
				(b"a".to_vec(), 1),
				(b"b".to_vec(), 1),
				(b"c".to_vec(), 3),
				(b"d".to_vec(), 1),
				(b"e".to_vec(), 1)
			]
		);

		let mut backward =
			MergedRows::new(maps.iter().map(|rows| rows.range(unbounded()).rev()).collect(), true);
		let mut seen = Vec::new();
		while let Some((key, group)) = backward.next_group() {
			seen.push((key.as_slice().to_vec(), group.len()));
		}
		assert_eq!(
			seen,
			vec![
				(b"e".to_vec(), 1),
				(b"d".to_vec(), 1),
				(b"c".to_vec(), 3),
				(b"b".to_vec(), 1),
				(b"a".to_vec(), 1)
			]
		);
	}
}
