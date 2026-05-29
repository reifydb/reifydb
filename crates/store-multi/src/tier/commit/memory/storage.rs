// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::{Ordering, Reverse},
	collections::{HashMap, HashSet},
	ops::Bound,
	sync::Arc,
};

use reifydb_core::{common::CommitVersion, encoded::key::EncodedKey, interface::store::EntryKind};
use reifydb_type::{Result, util::cowvec::CowVec};
use tracing::{Span, field, instrument};

use crate::{
	MultiVersionScope,
	tier::{
		HistoricalCursor, RangeBatch, RangeCursor, RawEntry, TierBackend, TierBatch, TierStorage, VersionedGetResult,
		commit::memory::entry::{CurrentMap, Entries, Entry, HistoricalMap},
	},
};

type EvictablePersist = Vec<(EncodedKey, CommitVersion, Option<CowVec<u8>>)>;
type EvictableDrop = Vec<(EncodedKey, CommitVersion)>;

#[derive(Clone)]
pub struct MemoryPrimitiveStorage {
	inner: Arc<MemoryPrimitiveStorageInner>,
}

struct MemoryPrimitiveStorageInner {
	entries: Entries,
}

impl Default for MemoryPrimitiveStorage {
	fn default() -> Self {
		Self::new()
	}
}

impl MemoryPrimitiveStorage {
	#[instrument(name = "store::multi::memory::new", level = "debug")]
	pub fn new() -> Self {
		Self {
			inner: Arc::new(MemoryPrimitiveStorageInner {
				entries: Entries::default(),
			}),
		}
	}

	pub fn count_current(&self, table: EntryKind) -> Result<u64> {
		Ok(self.inner.entries.data.get(&table).map(|e| e.current.read().len() as u64).unwrap_or(0))
	}

	pub fn list_all_entry_kinds(&self) -> Result<Vec<EntryKind>> {
		Ok(self.inner.entries.data.keys())
	}

	pub fn count_historical(&self, table: EntryKind) -> Result<u64> {
		Ok(self.inner
			.entries
			.data
			.get(&table)
			.map(|e| {
				let hist = e.historical.read();
				hist.values().map(|m| m.len() as u64).sum()
			})
			.unwrap_or(0))
	}

	#[inline]
	#[instrument(name = "store::multi::memory::get_or_create_table", level = "trace", skip(self), fields(table = ?table))]
	fn get_or_create_table(&self, table: EntryKind) -> Entry {
		self.inner.entries.data.get_or_insert_with(table, Entry::new)
	}

	#[inline]
	#[instrument(name = "store::multi::memory::set::table", level = "trace", skip(self, entries), fields(
		table = ?table,
		entry_count = entries.len(),
	))]
	fn process_table(
		&self,
		table: EntryKind,
		version: CommitVersion,
		entries: Vec<(EncodedKey, Option<CowVec<u8>>)>,
	) {
		let table_entry = self.get_or_create_table(table);
		let mut current = table_entry.current.write();
		let mut historical = table_entry.historical.write();

		for (key, value) in entries {
			if let Some((pre_version, pre_value)) = current.get(&key) {
				if *pre_version < version {
					let pre_version = *pre_version;
					let pre_value = pre_value.clone();
					#[cfg(reifydb_assertions)]
					{
						assert!(
							version.0 > pre_version.0,
							"promoting current entry to historical requires the incoming version to exceed it, otherwise the same version appears in both tiers and point-reads return the wrong entry (version={} pre_version={})",
							version.0,
							pre_version.0
						);
					}
					historical
						.entry(key.clone())
						.or_default()
						.insert(Reverse(pre_version), pre_value);

					current.insert(key, (version, value));
				} else {
					historical.entry(key).or_default().insert(Reverse(version), value);
				}
			} else {
				current.insert(key, (version, value));
			}
		}
	}

	pub fn collect_evictable_below(
		&self,
		table: EntryKind,
		cutoff: CommitVersion,
	) -> (EvictablePersist, EvictableDrop) {
		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => return (Vec::new(), Vec::new()),
		};
		let current = entry.current.read();
		let historical = entry.historical.read();

		let historical_entries: usize = historical.values().map(|versions| versions.len()).sum();

		let mut latest: HashMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)> =
			HashMap::with_capacity(current.len() + historical.len());
		let mut to_drop: Vec<(EncodedKey, CommitVersion)> =
			Vec::with_capacity(current.len() + historical_entries);

		for (key, (v, val)) in current.iter() {
			if *v <= cutoff {
				to_drop.push((key.clone(), *v));
				latest.insert(key.clone(), (*v, val.clone()));
			}
		}
		for (key, versions) in historical.iter() {
			for (Reverse(v), val) in versions.iter() {
				if *v <= cutoff {
					to_drop.push((key.clone(), *v));
					match latest.get(key) {
						Some((best, _)) if *best >= *v => {}
						_ => {
							latest.insert(key.clone(), (*v, val.clone()));
						}
					}
				}
			}
		}

		let to_persist = latest.into_iter().map(|(key, (v, val))| (key, v, val)).collect();
		(to_persist, to_drop)
	}
}

impl TierStorage for MemoryPrimitiveStorage {
	#[instrument(name = "store::multi::memory::get", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len(), version = version.0))]
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<VersionedGetResult> {
		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => return Ok(VersionedGetResult::NotFound),
		};

		let current = entry.current.read();
		if let Some((cur_version, value)) = current.get(key)
			&& *cur_version <= version
		{
			return Ok(match value {
				Some(v) => VersionedGetResult::Value {
					value: v.clone(),
					version: *cur_version,
				},
				None => VersionedGetResult::Tombstone,
			});
		}
		drop(current);

		let historical = entry.historical.read();
		if let Some(versions) = historical.get(key) {
			for (Reverse(v), value) in versions.range(Reverse(version)..) {
				if *v <= version {
					return Ok(match value {
						Some(val) => VersionedGetResult::Value {
							value: val.clone(),
							version: *v,
						},
						None => VersionedGetResult::Tombstone,
					});
				}
			}
		}

		Ok(VersionedGetResult::NotFound)
	}

	#[instrument(name = "store::multi::memory::contains", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len(), version = version.0), ret)]
	fn contains(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<bool> {
		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => return Ok(false),
		};

		let current = entry.current.read();
		if let Some((cur_version, value)) = current.get(key)
			&& *cur_version <= version
		{
			return Ok(value.is_some());
		}
		drop(current);

		let historical = entry.historical.read();
		if let Some(versions) = historical.get(key) {
			for (Reverse(v), value) in versions.range(Reverse(version)..) {
				if *v <= version {
					return Ok(value.is_some());
				}
			}
		}

		Ok(false)
	}

	#[instrument(name = "store::multi::memory::set", level = "trace", skip(self, batches), fields(
		table_count = batches.len(),
		total_entry_count = field::Empty,
		version = version.0
	))]
	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<()> {
		let total_entries: usize = batches.values().map(|v| v.len()).sum();

		batches.into_iter().for_each(|(table, entries)| {
			self.process_table(table, version, entries);
		});

		Span::current().record("total_entry_count", total_entries);
		Ok(())
	}

	#[instrument(name = "store::multi::memory::range_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size, scope = ?scope))]
	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
		};

		let cursor_key = cursor.last_key.clone();

		let current = entry.current.read();
		let historical = entry.historical.read();

		let mut entries: Vec<RawEntry> = Vec::with_capacity(batch_size + 1);

		let iter_start: Bound<&[u8]> = match &cursor_key {
			Some(last) => Bound::Excluded(last.as_slice()),
			None => start,
		};

		let iter_end: Bound<&[u8]> = end;

		let mut cur_iter = current.range::<[u8], _>((iter_start, iter_end)).peekable();
		let mut hist_iter = historical.range::<[u8], _>((iter_start, iter_end)).peekable();

		while entries.len() <= batch_size {
			let (take_cur, take_hist) = match (cur_iter.peek(), hist_iter.peek()) {
				(None, None) => break,
				(Some(_), None) => (true, false),
				(None, Some(_)) => (false, true),
				(Some((kc, _)), Some((kh, _))) => match kc.cmp(kh) {
					Ordering::Less => (true, false),
					Ordering::Greater => (false, true),
					Ordering::Equal => (true, true),
				},
			};

			if take_cur && take_hist {
				let (key, (cur_version, cur_value)) = cur_iter.next().unwrap();
				let (_, versions) = hist_iter.next().unwrap();
				if scope.contains(*cur_version) {
					entries.push(RawEntry {
						key: key.clone(),
						version: *cur_version,
						value: cur_value.clone(),
					});
				} else if *cur_version > scope.read() {
					for (Reverse(v), value) in versions.range(Reverse(scope.read())..) {
						if scope.contains(*v) {
							entries.push(RawEntry {
								key: key.clone(),
								version: *v,
								value: value.clone(),
							});
							break;
						}
						if let MultiVersionScope::Between {
							after,
							..
						} = scope
							&& *v <= after
						{
							break;
						}
					}
				}
			} else if take_cur {
				let (key, (cur_version, cur_value)) = cur_iter.next().unwrap();
				if scope.contains(*cur_version) {
					entries.push(RawEntry {
						key: key.clone(),
						version: *cur_version,
						value: cur_value.clone(),
					});
				}
			} else {
				let (key, versions) = hist_iter.next().unwrap();
				for (Reverse(v), value) in versions.range(Reverse(scope.read())..) {
					if scope.contains(*v) {
						entries.push(RawEntry {
							key: key.clone(),
							version: *v,
							value: value.clone(),
						});
						break;
					}
					if let MultiVersionScope::Between {
						after,
						..
					} = scope
						&& *v <= after
					{
						break;
					}
				}
			}
		}

		let has_more = entries.len() > batch_size;
		if has_more {
			entries.truncate(batch_size);
		}

		if let Some(last_entry) = entries.last() {
			cursor.last_key = Some(last_entry.key.clone());
		}
		if !has_more {
			cursor.exhausted = true;
		}

		Ok(RangeBatch {
			entries,
			has_more,
		})
	}

	#[instrument(name = "store::multi::memory::range_rev_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size, scope = ?scope))]
	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
		};

		let cursor_key = cursor.last_key.clone();

		let current = entry.current.read();
		let historical = entry.historical.read();

		let mut entries: Vec<RawEntry> = Vec::with_capacity(batch_size + 1);

		let iter_start: Bound<&[u8]> = start;

		let iter_end: Bound<&[u8]> = match &cursor_key {
			Some(last) => Bound::Excluded(last.as_slice()),
			None => end,
		};

		let mut cur_iter = current.range::<[u8], _>((iter_start, iter_end)).rev().peekable();
		let mut hist_iter = historical.range::<[u8], _>((iter_start, iter_end)).rev().peekable();

		while entries.len() <= batch_size {
			let (take_cur, take_hist) = match (cur_iter.peek(), hist_iter.peek()) {
				(None, None) => break,
				(Some(_), None) => (true, false),
				(None, Some(_)) => (false, true),
				(Some((kc, _)), Some((kh, _))) => match kc.cmp(kh) {
					Ordering::Greater => (true, false),
					Ordering::Less => (false, true),
					Ordering::Equal => (true, true),
				},
			};

			if take_cur && take_hist {
				let (key, (cur_version, cur_value)) = cur_iter.next().unwrap();
				let (_, versions) = hist_iter.next().unwrap();
				if scope.contains(*cur_version) {
					entries.push(RawEntry {
						key: key.clone(),
						version: *cur_version,
						value: cur_value.clone(),
					});
				} else if *cur_version > scope.read() {
					for (Reverse(v), value) in versions.range(Reverse(scope.read())..) {
						if scope.contains(*v) {
							entries.push(RawEntry {
								key: key.clone(),
								version: *v,
								value: value.clone(),
							});
							break;
						}
						if let MultiVersionScope::Between {
							after,
							..
						} = scope
							&& *v <= after
						{
							break;
						}
					}
				}
			} else if take_cur {
				let (key, (cur_version, cur_value)) = cur_iter.next().unwrap();
				if scope.contains(*cur_version) {
					entries.push(RawEntry {
						key: key.clone(),
						version: *cur_version,
						value: cur_value.clone(),
					});
				}
			} else {
				let (key, versions) = hist_iter.next().unwrap();
				for (Reverse(v), value) in versions.range(Reverse(scope.read())..) {
					if scope.contains(*v) {
						entries.push(RawEntry {
							key: key.clone(),
							version: *v,
							value: value.clone(),
						});
						break;
					}
					if let MultiVersionScope::Between {
						after,
						..
					} = scope
						&& *v <= after
					{
						break;
					}
				}
			}
		}

		let has_more = entries.len() > batch_size;
		if has_more {
			entries.truncate(batch_size);
		}

		if let Some(last_entry) = entries.last() {
			cursor.last_key = Some(last_entry.key.clone());
		}
		if !has_more {
			cursor.exhausted = true;
		}

		Ok(RangeBatch {
			entries,
			has_more,
		})
	}

	#[instrument(name = "store::multi::memory::ensure_table", level = "trace", skip(self), fields(table = ?table))]
	fn ensure_table(&self, table: EntryKind) -> Result<()> {
		let _ = self.get_or_create_table(table);
		Ok(())
	}

	#[instrument(name = "store::multi::memory::clear_table", level = "debug", skip(self), fields(table = ?table))]
	fn clear_table(&self, table: EntryKind) -> Result<()> {
		if let Some(entry) = self.inner.entries.data.get(&table) {
			*entry.current.write() = CurrentMap::new();
			*entry.historical.write() = HistoricalMap::new();
		}
		Ok(())
	}

	#[instrument(name = "store::multi::memory::drop", level = "debug", skip(self, batches), fields(
		table_count = batches.len(),
		total_entry_count = field::Empty
	))]
	fn drop(&self, batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>>) -> Result<()> {
		let total_entries: usize = batches.values().map(|v| v.len()).sum();

		for (table, entries) in batches {
			let table_entry = self.get_or_create_table(table);
			let mut current = table_entry.current.write();
			let mut historical = table_entry.historical.write();

			let mut by_key: HashMap<EncodedKey, Vec<CommitVersion>> = HashMap::new();
			for (key, version) in entries {
				by_key.entry(key).or_default().push(version);
			}

			for (key, dropped_versions) in by_key {
				let dropped_set: HashSet<CommitVersion> = dropped_versions.iter().copied().collect();

				let cur_version = current.get(&key).map(|(v, _)| *v);
				let stored_hist_covered = historical
					.get(&key)
					.map(|m| m.keys().all(|Reverse(v)| dropped_set.contains(v)))
					.unwrap_or(true);
				let stored_cur_covered = cur_version.is_none_or(|v| dropped_set.contains(&v));

				if stored_cur_covered && stored_hist_covered {
					current.remove(&key);
					historical.remove(&key);
					continue;
				}

				for version in dropped_versions {
					let cur_matches = current.get(&key).map(|(v, _)| *v) == Some(version);
					if cur_matches {
						let popped = historical.get_mut(&key).and_then(|v| v.pop_first());
						let now_empty = historical.get(&key).is_some_and(|v| v.is_empty());
						if now_empty {
							historical.remove(&key);
						}
						match popped {
							Some((Reverse(promoted_v), promoted_value)) => {
								current.insert(
									key.clone(),
									(promoted_v, promoted_value),
								);
							}
							None => {
								current.remove(&key);
							}
						}
					} else {
						let now_empty = if let Some(versions) = historical.get_mut(&key) {
							versions.remove(&Reverse(version));
							versions.is_empty()
						} else {
							false
						};
						if now_empty {
							historical.remove(&key);
						}
					}
				}
			}
		}

		Span::current().record("total_entry_count", total_entries);
		Ok(())
	}

	#[instrument(name = "store::multi::memory::get_all_versions", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len()))]
	fn get_all_versions(&self, table: EntryKind, key: &[u8]) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => return Ok(Vec::new()),
		};

		let current = entry.current.read();
		let current_hit = current.get(key).map(|(cur_version, value)| (*cur_version, value.clone()));
		drop(current);

		let historical = entry.historical.read();
		let hist_versions = historical.get(key);

		let mut versions: Vec<(CommitVersion, Option<CowVec<u8>>)> =
			Vec::with_capacity(current_hit.is_some() as usize + hist_versions.map_or(0, |v| v.len()));
		if let Some(hit) = current_hit {
			versions.push(hit);
		}
		if let Some(hist_versions) = hist_versions {
			for (Reverse(v), value) in hist_versions.iter() {
				versions.push((*v, value.clone()));
			}
		}

		versions.sort_by(|a, b| b.0.cmp(&a.0));

		Ok(versions)
	}

	#[instrument(name = "store::multi::memory::scan_historical_below", level = "trace", skip(self, cursor), fields(table = ?table, cutoff = cutoff.0, batch_size = batch_size))]
	fn scan_historical_below(
		&self,
		table: EntryKind,
		cutoff: CommitVersion,
		cursor: &mut HistoricalCursor,
		batch_size: usize,
	) -> Result<Vec<(EncodedKey, CommitVersion)>> {
		if cursor.exhausted || batch_size == 0 {
			return Ok(Vec::new());
		}

		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => {
				cursor.exhausted = true;
				return Ok(Vec::new());
			}
		};

		let historical = entry.historical.read();

		let mut collected: Vec<(EncodedKey, CommitVersion)> = Vec::new();
		let mut over_limit = false;

		for (key, versions) in historical.iter() {
			match (cursor.last_key.as_ref(), cursor.last_version) {
				(Some(lk), _) if key < lk => continue,
				(Some(lk), Some(lv)) if key == lk => {
					for (Reverse(v), _value) in versions.iter().rev() {
						if *v <= lv {
							continue;
						}
						if *v >= cutoff {
							continue;
						}
						collected.push((key.clone(), *v));
						if collected.len() > batch_size {
							over_limit = true;
							break;
						}
					}
				}
				_ => {
					for (Reverse(v), _value) in versions.iter().rev() {
						if *v >= cutoff {
							continue;
						}
						collected.push((key.clone(), *v));
						if collected.len() > batch_size {
							over_limit = true;
							break;
						}
					}
				}
			}

			if over_limit {
				break;
			}
		}

		collected.sort_by(|a, b| a.0.as_slice().cmp(b.0.as_slice()).then(a.1.0.cmp(&b.1.0)));

		let has_more = collected.len() > batch_size;
		if has_more {
			collected.truncate(batch_size);
		}

		if let Some(last) = collected.last() {
			cursor.last_key = Some(last.0.clone());
			cursor.last_version = Some(last.1);
		}
		if !has_more {
			cursor.exhausted = true;
		}

		Ok(collected)
	}
}

impl TierBackend for MemoryPrimitiveStorage {}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{id::TableId, shape::ShapeId};

	use super::*;

	#[test]
	fn test_basic_operations() {
		let storage = MemoryPrimitiveStorage::new();

		let key = EncodedKey::new(b"key1".to_vec());
		let version = CommitVersion(1);

		// Put and get
		storage.set(
			version,
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value1".to_vec())))])]),
		)
		.unwrap();

		let value = storage.get(EntryKind::Multi, &key, version).unwrap().value();
		assert_eq!(value.as_deref(), Some(b"value1".as_slice()));

		// Contains
		assert!(storage.contains(EntryKind::Multi, &key, version).unwrap());

		assert!(!storage.contains(EntryKind::Multi, b"nonexistent", version).unwrap());

		// Delete (tombstone)
		let version2 = CommitVersion(2);
		storage.set(version2, HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();
		assert!(!storage.contains(EntryKind::Multi, &key, version2).unwrap());
	}

	#[test]
	fn test_source_tables() {
		let storage = MemoryPrimitiveStorage::new();

		let source1 = ShapeId::Table(TableId(1));
		let source2 = ShapeId::Table(TableId(2));

		let key = EncodedKey::new(b"key".to_vec());
		let version = CommitVersion(1);

		storage.set(
			version,
			HashMap::from([(
				EntryKind::Source(source1),
				vec![(key.clone(), Some(CowVec::new(b"table1".to_vec())))],
			)]),
		)
		.unwrap();
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Source(source2),
				vec![(key.clone(), Some(CowVec::new(b"table2".to_vec())))],
			)]),
		)
		.unwrap();

		assert_eq!(
			storage.get(EntryKind::Source(source1), &key, version).unwrap().value().as_deref(),
			Some(b"table1".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Source(source2), &key, version).unwrap().value().as_deref(),
			Some(b"table2".as_slice())
		);
	}

	#[test]
	fn test_version_promotion_to_historical() {
		let storage = MemoryPrimitiveStorage::new();

		let key = EncodedKey::new(b"key1".to_vec());

		// Insert version 1
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();

		// Insert version 2 (v1 should be promoted to historical)
		storage.set(
			CommitVersion(2),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v2".to_vec())))])]),
		)
		.unwrap();

		// Insert version 3 (v2 should be promoted to historical)
		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();

		// Get at version 3 should return v3 (from current)
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice())
		);

		// Get at version 2 should return v2 (from historical)
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().as_deref(),
			Some(b"v2".as_slice())
		);

		// Get at version 1 should return v1 (from historical)
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().as_deref(),
			Some(b"v1".as_slice())
		);
	}

	#[test]
	fn test_insert_older_version() {
		let storage = MemoryPrimitiveStorage::new();

		let key = EncodedKey::new(b"key1".to_vec());

		// Insert version 3 first
		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();

		// Insert version 1 (older - should go directly to historical)
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();

		// Get at version 3 should return v3 (current)
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice())
		);

		// Get at version 1 should return v1 (historical)
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().as_deref(),
			Some(b"v1".as_slice())
		);

		// Get at version 2 should return v1 (largest version <= 2)
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().as_deref(),
			Some(b"v1".as_slice())
		);
	}

	#[test]
	fn test_range_next() {
		let storage = MemoryPrimitiveStorage::new();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(EncodedKey::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
					(EncodedKey::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
					(EncodedKey::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				100,
			)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);

		// Verify order
		assert_eq!(&*batch.entries[0].key, b"a");
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(&*batch.entries[2].key, b"c");
	}

	#[test]
	fn test_range_rev_next() {
		let storage = MemoryPrimitiveStorage::new();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(EncodedKey::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
					(EncodedKey::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
					(EncodedKey::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_rev_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				100,
			)
			.unwrap();

		assert_eq!(batch.entries.len(), 3);
		assert!(!batch.has_more);
		assert!(cursor.exhausted);

		// Verify reverse order
		assert_eq!(&*batch.entries[0].key, b"c");
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(&*batch.entries[2].key, b"a");
	}

	#[test]
	fn test_range_streaming_pagination() {
		let storage = MemoryPrimitiveStorage::new();

		let version = CommitVersion(1);

		// Insert 10 entries
		let entries: Vec<_> =
			(0..10u8).map(|i| (EncodedKey::new(vec![i]), Some(CowVec::new(vec![i * 10])))).collect();
		storage.set(version, HashMap::from([(EntryKind::Multi, entries)])).unwrap();

		// Use a single cursor to stream through all entries
		let mut cursor = RangeCursor::new();

		// First batch of 3
		let batch1 = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);

		assert_eq!(&*batch1.entries[0].key, &[0]);
		assert_eq!(&*batch1.entries[2].key, &[2]);

		// Second batch of 3
		let batch2 = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.exhausted);

		assert_eq!(&*batch2.entries[0].key, &[3]);
		assert_eq!(&*batch2.entries[2].key, &[5]);

		// Third batch of 3
		let batch3 = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert_eq!(batch3.entries.len(), 3);
		assert!(batch3.has_more);
		assert!(!cursor.exhausted);

		assert_eq!(&*batch3.entries[0].key, &[6]);
		assert_eq!(&*batch3.entries[2].key, &[8]);

		// Fourth batch - only 1 entry remaining
		let batch4 = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert_eq!(batch4.entries.len(), 1);
		assert!(!batch4.has_more);
		assert!(cursor.exhausted);

		assert_eq!(&*batch4.entries[0].key, &[9]);

		// Fifth call - exhausted
		let batch5 = storage
			.range_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert!(batch5.entries.is_empty());
	}

	#[test]
	fn test_range_reving_pagination() {
		let storage = MemoryPrimitiveStorage::new();

		let version = CommitVersion(1);

		// Insert 10 entries
		let entries: Vec<_> =
			(0..10u8).map(|i| (EncodedKey::new(vec![i]), Some(CowVec::new(vec![i * 10])))).collect();
		storage.set(version, HashMap::from([(EntryKind::Multi, entries)])).unwrap();

		// Use a single cursor to stream in reverse
		let mut cursor = RangeCursor::new();

		// First batch of 3 (reverse)
		let batch1 = storage
			.range_rev_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);

		assert_eq!(&*batch1.entries[0].key, &[9]);
		assert_eq!(&*batch1.entries[2].key, &[7]);

		// Second batch
		let batch2 = storage
			.range_rev_next(
				EntryKind::Multi,
				&mut cursor,
				Bound::Unbounded,
				Bound::Unbounded,
				MultiVersionScope::AsOf {
					read: version,
				},
				3,
			)
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.exhausted);

		assert_eq!(&*batch2.entries[0].key, &[6]);
		assert_eq!(&*batch2.entries[2].key, &[4]);
	}

	#[test]
	fn test_drop_from_historical() {
		let storage = MemoryPrimitiveStorage::new();

		let key = EncodedKey::new(b"key1".to_vec());

		// Insert versions 1, 2, 3
		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{}", v).into_bytes())))],
				)]),
			)
			.unwrap();
		}

		// Version 3 is in current, versions 1 and 2 are in historical
		// Drop version 1 (from historical)
		storage.drop(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(1))])])).unwrap();

		// Version 1 should no longer be accessible
		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().is_none());

		// Versions 2 and 3 should still work
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().as_deref(),
			Some(b"v2".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice())
		);
	}

	#[test]
	fn test_tombstones() {
		let storage = MemoryPrimitiveStorage::new();

		let key = EncodedKey::new(b"key1".to_vec());

		// Insert version 1 with value
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value".to_vec())))])]),
		)
		.unwrap();

		// Insert version 2 with tombstone
		storage.set(CommitVersion(2), HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();

		// Get at version 2 should return None (tombstone)
		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().is_none());
		assert!(!storage.contains(EntryKind::Multi, &key, CommitVersion(2)).unwrap());

		// Get at version 1 should return value
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().as_deref(),
			Some(b"value".as_slice())
		);
	}

	#[test]
	fn test_collect_evictable_below_keeps_versions_above_cutoff() {
		let storage = MemoryPrimitiveStorage::new();
		let key = EncodedKey::new(b"k".to_vec());
		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{v}").into_bytes())))],
				)]),
			)
			.unwrap();
		}

		// cutoff = 2: the latest version <= 2 is v2 (what a reader in [2, 3) resolves to, so it
		// must be persisted); both v1 and v2 are dropped; v3 (> cutoff) stays resident.
		let (to_persist, to_drop) = storage.collect_evictable_below(EntryKind::Multi, CommitVersion(2));
		assert_eq!(to_persist.len(), 1);
		assert_eq!(to_persist[0].0, key);
		assert_eq!(to_persist[0].1, CommitVersion(2));
		assert_eq!(to_persist[0].2.as_deref(), Some(b"v2".as_slice()));
		let dropped: HashSet<CommitVersion> = to_drop.iter().map(|(_, v)| *v).collect();
		assert_eq!(dropped, HashSet::from([CommitVersion(1), CommitVersion(2)]));

		// After dropping the <= cutoff versions from the buffer (in the real pass v2 is persisted
		// to sqlite first), v3 stays readable while v1/v2 are gone from the buffer.
		storage.drop(HashMap::from([(EntryKind::Multi, to_drop)])).unwrap();
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice())
		);
		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().is_none());
		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().is_none());
	}

	#[test]
	fn test_collect_evictable_below_empty_when_all_above_cutoff() {
		let storage = MemoryPrimitiveStorage::new();
		let key = EncodedKey::new(b"k".to_vec());
		storage.set(
			CommitVersion(5),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v".to_vec())))])]),
		)
		.unwrap();
		let (to_persist, to_drop) = storage.collect_evictable_below(EntryKind::Multi, CommitVersion(3));
		assert!(to_persist.is_empty());
		assert!(to_drop.is_empty());
	}

	#[test]
	fn test_collect_evictable_below_persists_exactly_one_value_per_key() {
		// With several historical versions <= cutoff, only the LATEST-<=cutoff value may be persisted: that
		// is the single value a reader at the cutoff snapshot resolves to. Persisting an older one (or more
		// than one) would either corrupt the resolved value or bloat the persistent tier. This guards the
		// inner "best >= v" tie-break in collect_evictable_below.
		let storage = MemoryPrimitiveStorage::new();
		let key = EncodedKey::new(b"k".to_vec());
		for v in 1..=5u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{v}").into_bytes())))],
				)]),
			)
			.unwrap();
		}

		// cutoff = 4: v1..=v4 are all evictable, but the persisted value must be exactly v4.
		let (to_persist, to_drop) = storage.collect_evictable_below(EntryKind::Multi, CommitVersion(4));
		assert_eq!(to_persist.len(), 1, "exactly one value persisted per key");
		assert_eq!(to_persist[0].1, CommitVersion(4), "the latest version <= cutoff");
		assert_eq!(to_persist[0].2.as_deref(), Some(b"v4".as_slice()));

		// All four <= cutoff versions are scheduled to drop; v5 (> cutoff) is not.
		let dropped: HashSet<CommitVersion> = to_drop.iter().map(|(_, v)| *v).collect();
		assert_eq!(
			dropped,
			HashSet::from([CommitVersion(1), CommitVersion(2), CommitVersion(3), CommitVersion(4)])
		);
	}

	#[test]
	fn test_collect_evictable_below_persists_tombstone_when_it_is_the_latest() {
		// If the latest-<=cutoff version is a tombstone, the eviction must carry the tombstone (None) to the
		// persistent tier - otherwise a later read would resurrect the pre-delete value. This guards against
		// the sweep silently dropping deletes.
		let storage = MemoryPrimitiveStorage::new();
		let key = EncodedKey::new(b"k".to_vec());
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();
		storage.set(CommitVersion(2), HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();

		let (to_persist, to_drop) = storage.collect_evictable_below(EntryKind::Multi, CommitVersion(2));
		assert_eq!(to_persist.len(), 1);
		assert_eq!(to_persist[0].1, CommitVersion(2), "the tombstone is the latest version");
		assert!(to_persist[0].2.is_none(), "the persisted latest value must be the tombstone, not v1");
		assert_eq!(to_drop.len(), 2, "both v1 and the tombstone are dropped from the buffer");
	}

	#[test]
	fn test_collect_evictable_below_only_drops_historical_when_current_is_above_cutoff() {
		// Current is v5 (> cutoff) but a historical v2 (<= cutoff) exists. Only the historical version may be
		// evicted; the current version stays resident and must NOT be persisted (it is still hot). This is the
		// path where a key is actively written but old snapshots are aging out.
		let storage = MemoryPrimitiveStorage::new();
		let key = EncodedKey::new(b"k".to_vec());
		storage.set(
			CommitVersion(2),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v2".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(5),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v5".to_vec())))])]),
		)
		.unwrap();

		let (to_persist, to_drop) = storage.collect_evictable_below(EntryKind::Multi, CommitVersion(3));
		assert_eq!(to_persist.len(), 1);
		assert_eq!(to_persist[0].1, CommitVersion(2), "only the aged-out historical version is persisted");
		assert_eq!(to_persist[0].2.as_deref(), Some(b"v2".as_slice()));
		let dropped: HashSet<CommitVersion> = to_drop.iter().map(|(_, v)| *v).collect();
		assert_eq!(dropped, HashSet::from([CommitVersion(2)]), "v5 (current, > cutoff) is never dropped");

		// After dropping v2, v5 still reads and v3 falls through (no resident historical anymore).
		storage.drop(HashMap::from([(EntryKind::Multi, to_drop)])).unwrap();
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(5)).unwrap().value().as_deref(),
			Some(b"v5".as_slice())
		);
		assert!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().is_none(),
			"the v2 a reader at snapshot 3 used to see is gone from the buffer after eviction"
		);
	}

	#[test]
	fn test_collect_evictable_below_handles_multiple_keys_independently() {
		// The cutoff applies per version, not per key. A key whose only version is above the cutoff must be
		// left fully resident even while a sibling key is evicted. This guards a regression where a shared
		// scan could over-collect across keys.
		let storage = MemoryPrimitiveStorage::new();
		let cold = EncodedKey::new(b"cold".to_vec());
		let hot = EncodedKey::new(b"hot".to_vec());
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(cold.clone(), Some(CowVec::new(b"cold1".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(9),
			HashMap::from([(EntryKind::Multi, vec![(hot.clone(), Some(CowVec::new(b"hot9".to_vec())))])]),
		)
		.unwrap();

		let (to_persist, to_drop) = storage.collect_evictable_below(EntryKind::Multi, CommitVersion(5));
		assert_eq!(to_persist.len(), 1, "only the cold key is evictable below the cutoff");
		assert_eq!(to_persist[0].0, cold);
		assert!(to_drop.iter().all(|(k, _)| *k == cold), "the hot key must not be scheduled for drop");
	}
}
