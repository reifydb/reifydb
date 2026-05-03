// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2025 ReifyDB

use std::{
	cmp::Reverse,
	collections::{HashMap, HashSet},
	ops::Bound,
	sync::Arc,
};

use reifydb_core::{
	common::CommitVersion,
	interface::{
		catalog::{flow::FlowNodeId, id::TableId, shape::ShapeId},
		store::EntryKind,
	},
};
use reifydb_type::{Result, util::cowvec::CowVec};
use tracing::{Span, field, instrument};

use super::entry::{CurrentMap, Entries, Entry, HistoricalMap, entry_id_to_key};
use crate::tier::{HistoricalCursor, RangeBatch, RangeCursor, RawEntry, TierBackend, TierBatch, TierStorage};

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
		let table_key = entry_id_to_key(table);
		Ok(self.inner.entries.data.get(&table_key).map(|e| e.current.read().len() as u64).unwrap_or(0))
	}

	pub fn list_all_entry_kinds(&self) -> Result<Vec<EntryKind>> {
		let mut out = Vec::new();
		for key in self.inner.entries.data.keys() {
			if key == "multi" {
				out.push(EntryKind::Multi);
			} else if let Some(rest) = key.strip_prefix("source:")
				&& let Ok(id) = rest.parse::<u64>()
			{
				out.push(EntryKind::Source(ShapeId::Table(TableId(id))));
			} else if let Some(rest) = key.strip_prefix("operator:")
				&& let Ok(id) = rest.parse::<u64>()
			{
				out.push(EntryKind::Operator(FlowNodeId(id)));
			}
		}
		Ok(out)
	}

	pub fn count_historical(&self, table: EntryKind) -> Result<u64> {
		let table_key = entry_id_to_key(table);
		Ok(self.inner
			.entries
			.data
			.get(&table_key)
			.map(|e| {
				let hist = e.historical.read();
				hist.values().map(|m| m.len() as u64).sum()
			})
			.unwrap_or(0))
	}

	#[inline]
	#[instrument(name = "store::multi::memory::get_or_create_table", level = "trace", skip(self), fields(table = ?table))]
	fn get_or_create_table(&self, table: EntryKind) -> Entry {
		let table_key = entry_id_to_key(table);
		self.inner.entries.data.get_or_insert_with(table_key, Entry::new)
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
		entries: Vec<(CowVec<u8>, Option<CowVec<u8>>)>,
	) {
		let table_entry = self.get_or_create_table(table);
		let mut current = table_entry.current.write();
		let mut historical = table_entry.historical.write();

		for (key, value) in entries {
			if let Some((pre_version, pre_value)) = current.get(&key) {
				if *pre_version < version {
					let pre_version = *pre_version;
					let pre_value = pre_value.clone();
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
}

impl TierStorage for MemoryPrimitiveStorage {
	#[instrument(name = "store::multi::memory::get", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len(), version = version.0))]
	fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<Option<CowVec<u8>>> {
		let table_key = entry_id_to_key(table);
		let entry = match self.inner.entries.data.get(&table_key) {
			Some(e) => e,
			None => return Ok(None),
		};

		let key = CowVec::new(key.to_vec());

		let current = entry.current.read();
		if let Some((cur_version, value)) = current.get(&key)
			&& *cur_version <= version
		{
			return Ok(value.clone());
		}
		drop(current);

		let historical = entry.historical.read();
		if let Some(versions) = historical.get(&key) {
			for (Reverse(v), value) in versions.range(Reverse(version)..) {
				if *v <= version {
					return Ok(value.clone());
				}
			}
		}

		Ok(None)
	}

	#[instrument(name = "store::multi::memory::contains", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len(), version = version.0), ret)]
	fn contains(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> Result<bool> {
		let table_key = entry_id_to_key(table);
		let entry = match self.inner.entries.data.get(&table_key) {
			Some(e) => e,
			None => return Ok(false),
		};

		let key = CowVec::new(key.to_vec());

		let current = entry.current.read();
		if let Some((cur_version, value)) = current.get(&key)
			&& *cur_version <= version
		{
			return Ok(value.is_some());
		}
		drop(current);

		let historical = entry.historical.read();
		if let Some(versions) = historical.get(&key) {
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

	#[instrument(name = "store::multi::memory::range_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size, version = version.0))]
	fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let table_key = entry_id_to_key(table);
		let entry = match self.inner.entries.data.get(&table_key) {
			Some(e) => e,
			None => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
		};

		let start_key = match start {
			Bound::Included(k) | Bound::Excluded(k) => Some(CowVec::new(k.to_vec())),
			Bound::Unbounded => None,
		};
		let end_key = match end {
			Bound::Included(k) | Bound::Excluded(k) => Some(CowVec::new(k.to_vec())),
			Bound::Unbounded => None,
		};

		let cursor_key = cursor.last_key.clone();

		let current = entry.current.read();
		let historical = entry.historical.read();

		let mut entries: Vec<RawEntry> = Vec::with_capacity(batch_size + 1);

		let iter_start: Bound<&CowVec<u8>> = match &cursor_key {
			Some(last) => Bound::Excluded(last),
			None => match &start_key {
				Some(k) => match start {
					Bound::Included(_) => Bound::Included(k),
					Bound::Excluded(_) => Bound::Excluded(k),
					Bound::Unbounded => Bound::Unbounded,
				},
				None => Bound::Unbounded,
			},
		};

		let iter_end: Bound<&CowVec<u8>> = match &end_key {
			Some(k) => match end {
				Bound::Included(_) => Bound::Included(k),
				Bound::Excluded(_) => Bound::Excluded(k),
				Bound::Unbounded => Bound::Unbounded,
			},
			None => Bound::Unbounded,
		};

		let current_keys: Vec<_> = current.range::<CowVec<u8>, _>((iter_start, iter_end)).collect();

		for (key, (cur_version, cur_value)) in current_keys {
			if entries.len() > batch_size {
				break;
			}

			if *cur_version <= version {
				entries.push(RawEntry {
					key: key.clone(),
					version: *cur_version,
					value: cur_value.clone(),
				});
			} else if let Some(versions) = historical.get(key) {
				for (Reverse(v), value) in versions.range(Reverse(version)..) {
					if *v <= version {
						entries.push(RawEntry {
							key: key.clone(),
							version: *v,
							value: value.clone(),
						});
						break;
					}
				}
			}
		}

		for (key, versions) in historical.range::<CowVec<u8>, _>((iter_start, iter_end)) {
			if entries.len() > batch_size {
				break;
			}

			if current.contains_key(key) {
				continue;
			}

			for (Reverse(v), value) in versions.range(Reverse(version)..) {
				if *v <= version {
					entries.push(RawEntry {
						key: key.clone(),
						version: *v,
						value: value.clone(),
					});
					break;
				}
			}
		}

		entries.sort_by(|a, b| a.key.cmp(&b.key));

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

	#[instrument(name = "store::multi::memory::range_rev_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size, version = version.0))]
	fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		version: CommitVersion,
		batch_size: usize,
	) -> Result<RangeBatch> {
		if cursor.exhausted {
			return Ok(RangeBatch::empty());
		}

		let table_key = entry_id_to_key(table);
		let entry = match self.inner.entries.data.get(&table_key) {
			Some(e) => e,
			None => {
				cursor.exhausted = true;
				return Ok(RangeBatch::empty());
			}
		};

		let start_key = match start {
			Bound::Included(k) | Bound::Excluded(k) => Some(CowVec::new(k.to_vec())),
			Bound::Unbounded => None,
		};
		let end_key = match end {
			Bound::Included(k) | Bound::Excluded(k) => Some(CowVec::new(k.to_vec())),
			Bound::Unbounded => None,
		};

		let cursor_key = cursor.last_key.clone();

		let current = entry.current.read();
		let historical = entry.historical.read();

		let mut entries: Vec<RawEntry> = Vec::with_capacity(batch_size + 1);

		let iter_start: Bound<&CowVec<u8>> = match &start_key {
			Some(k) => match start {
				Bound::Included(_) => Bound::Included(k),
				Bound::Excluded(_) => Bound::Excluded(k),
				Bound::Unbounded => Bound::Unbounded,
			},
			None => Bound::Unbounded,
		};

		let iter_end: Bound<&CowVec<u8>> = match &cursor_key {
			Some(last) => Bound::Excluded(last),
			None => match &end_key {
				Some(k) => match end {
					Bound::Included(_) => Bound::Included(k),
					Bound::Excluded(_) => Bound::Excluded(k),
					Bound::Unbounded => Bound::Unbounded,
				},
				None => Bound::Unbounded,
			},
		};

		let current_keys: Vec<_> = current.range::<CowVec<u8>, _>((iter_start, iter_end)).rev().collect();

		for (key, (cur_version, cur_value)) in current_keys {
			if entries.len() > batch_size {
				break;
			}

			if *cur_version <= version {
				entries.push(RawEntry {
					key: key.clone(),
					version: *cur_version,
					value: cur_value.clone(),
				});
			} else if let Some(versions) = historical.get(key) {
				for (Reverse(v), value) in versions.range(Reverse(version)..) {
					if *v <= version {
						entries.push(RawEntry {
							key: key.clone(),
							version: *v,
							value: value.clone(),
						});
						break;
					}
				}
			}
		}

		for (key, versions) in historical.range::<CowVec<u8>, _>((iter_start, iter_end)).rev() {
			if entries.len() > batch_size {
				break;
			}

			if current.contains_key(key) {
				continue;
			}

			for (Reverse(v), value) in versions.range(Reverse(version)..) {
				if *v <= version {
					entries.push(RawEntry {
						key: key.clone(),
						version: *v,
						value: value.clone(),
					});
					break;
				}
			}
		}

		entries.sort_by(|a, b| b.key.cmp(&a.key));

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
		let table_key = entry_id_to_key(table);
		if let Some(entry) = self.inner.entries.data.get(&table_key) {
			*entry.current.write() = CurrentMap::new();
			*entry.historical.write() = HistoricalMap::new();
		}
		Ok(())
	}

	#[instrument(name = "store::multi::memory::drop", level = "debug", skip(self, batches), fields(
		table_count = batches.len(),
		total_entry_count = field::Empty
	))]
	fn drop(&self, batches: HashMap<EntryKind, Vec<(CowVec<u8>, CommitVersion)>>) -> Result<()> {
		let total_entries: usize = batches.values().map(|v| v.len()).sum();

		for (table, entries) in batches {
			let table_entry = self.get_or_create_table(table);
			let mut current = table_entry.current.write();
			let mut historical = table_entry.historical.write();

			let mut by_key: HashMap<CowVec<u8>, Vec<CommitVersion>> = HashMap::new();
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
		let table_key = entry_id_to_key(table);
		let entry = match self.inner.entries.data.get(&table_key) {
			Some(e) => e,
			None => return Ok(Vec::new()),
		};

		let key = CowVec::new(key.to_vec());
		let mut versions: Vec<(CommitVersion, Option<CowVec<u8>>)> = Vec::new();

		let current = entry.current.read();
		if let Some((cur_version, value)) = current.get(&key) {
			versions.push((*cur_version, value.clone()));
		}
		drop(current);

		let historical = entry.historical.read();
		if let Some(hist_versions) = historical.get(&key) {
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
	) -> Result<Vec<(CowVec<u8>, CommitVersion)>> {
		if cursor.exhausted || batch_size == 0 {
			return Ok(Vec::new());
		}

		let table_key = entry_id_to_key(table);
		let entry = match self.inner.entries.data.get(&table_key) {
			Some(e) => e,
			None => {
				cursor.exhausted = true;
				return Ok(Vec::new());
			}
		};

		let historical = entry.historical.read();

		let mut collected: Vec<(CowVec<u8>, CommitVersion)> = Vec::new();
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

		let key = CowVec::new(b"key1".to_vec());
		let version = CommitVersion(1);

		// Put and get
		storage.set(
			version,
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value1".to_vec())))])]),
		)
		.unwrap();

		let value = storage.get(EntryKind::Multi, &key, version).unwrap();
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

		let key = CowVec::new(b"key".to_vec());
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
			storage.get(EntryKind::Source(source1), &key, version).unwrap().as_deref(),
			Some(b"table1".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Source(source2), &key, version).unwrap().as_deref(),
			Some(b"table2".as_slice())
		);
	}

	#[test]
	fn test_version_promotion_to_historical() {
		let storage = MemoryPrimitiveStorage::new();

		let key = CowVec::new(b"key1".to_vec());

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
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().as_deref(),
			Some(b"v3".as_slice())
		);

		// Get at version 2 should return v2 (from historical)
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().as_deref(),
			Some(b"v2".as_slice())
		);

		// Get at version 1 should return v1 (from historical)
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().as_deref(),
			Some(b"v1".as_slice())
		);
	}

	#[test]
	fn test_insert_older_version() {
		let storage = MemoryPrimitiveStorage::new();

		let key = CowVec::new(b"key1".to_vec());

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
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().as_deref(),
			Some(b"v3".as_slice())
		);

		// Get at version 1 should return v1 (historical)
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().as_deref(),
			Some(b"v1".as_slice())
		);

		// Get at version 2 should return v1 (largest version <= 2)
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().as_deref(),
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
					(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
					(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
					(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 100)
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
					(CowVec::new(b"a".to_vec()), Some(CowVec::new(b"1".to_vec()))),
					(CowVec::new(b"b".to_vec()), Some(CowVec::new(b"2".to_vec()))),
					(CowVec::new(b"c".to_vec()), Some(CowVec::new(b"3".to_vec()))),
				],
			)]),
		)
		.unwrap();

		let mut cursor = RangeCursor::new();
		let batch = storage
			.range_rev_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 100)
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
			(0..10u8).map(|i| (CowVec::new(vec![i]), Some(CowVec::new(vec![i * 10])))).collect();
		storage.set(version, HashMap::from([(EntryKind::Multi, entries)])).unwrap();

		// Use a single cursor to stream through all entries
		let mut cursor = RangeCursor::new();

		// First batch of 3
		let batch1 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);

		assert_eq!(&*batch1.entries[0].key, &[0]);
		assert_eq!(&*batch1.entries[2].key, &[2]);

		// Second batch of 3
		let batch2 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert_eq!(batch2.entries.len(), 3);
		assert!(batch2.has_more);
		assert!(!cursor.exhausted);

		assert_eq!(&*batch2.entries[0].key, &[3]);
		assert_eq!(&*batch2.entries[2].key, &[5]);

		// Third batch of 3
		let batch3 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert_eq!(batch3.entries.len(), 3);
		assert!(batch3.has_more);
		assert!(!cursor.exhausted);

		assert_eq!(&*batch3.entries[0].key, &[6]);
		assert_eq!(&*batch3.entries[2].key, &[8]);

		// Fourth batch - only 1 entry remaining
		let batch4 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert_eq!(batch4.entries.len(), 1);
		assert!(!batch4.has_more);
		assert!(cursor.exhausted);

		assert_eq!(&*batch4.entries[0].key, &[9]);

		// Fifth call - exhausted
		let batch5 = storage
			.range_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert!(batch5.entries.is_empty());
	}

	#[test]
	fn test_range_reving_pagination() {
		let storage = MemoryPrimitiveStorage::new();

		let version = CommitVersion(1);

		// Insert 10 entries
		let entries: Vec<_> =
			(0..10u8).map(|i| (CowVec::new(vec![i]), Some(CowVec::new(vec![i * 10])))).collect();
		storage.set(version, HashMap::from([(EntryKind::Multi, entries)])).unwrap();

		// Use a single cursor to stream in reverse
		let mut cursor = RangeCursor::new();

		// First batch of 3 (reverse)
		let batch1 = storage
			.range_rev_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
			.unwrap();
		assert_eq!(batch1.entries.len(), 3);
		assert!(batch1.has_more);
		assert!(!cursor.exhausted);

		assert_eq!(&*batch1.entries[0].key, &[9]);
		assert_eq!(&*batch1.entries[2].key, &[7]);

		// Second batch
		let batch2 = storage
			.range_rev_next(EntryKind::Multi, &mut cursor, Bound::Unbounded, Bound::Unbounded, version, 3)
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

		let key = CowVec::new(b"key1".to_vec());

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
		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().is_none());

		// Versions 2 and 3 should still work
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().as_deref(),
			Some(b"v2".as_slice())
		);
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().as_deref(),
			Some(b"v3".as_slice())
		);
	}

	#[test]
	fn test_tombstones() {
		let storage = MemoryPrimitiveStorage::new();

		let key = CowVec::new(b"key1".to_vec());

		// Insert version 1 with value
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value".to_vec())))])]),
		)
		.unwrap();

		// Insert version 2 with tombstone
		storage.set(CommitVersion(2), HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();

		// Get at version 2 should return None (tombstone)
		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().is_none());
		assert!(!storage.contains(EntryKind::Multi, &key, CommitVersion(2)).unwrap());

		// Get at version 1 should return value
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().as_deref(),
			Some(b"value".as_slice())
		);
	}
}
