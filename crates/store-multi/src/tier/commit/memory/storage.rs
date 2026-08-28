// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::{Ordering, Reverse},
	collections::{HashMap, HashSet},
	ops::Bound,
	sync::Arc,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, interface::store::EntryKind};
use reifydb_value::{Result, byte_size::ByteSize, reifydb_assertions, util::cowvec::CowVec};
use tracing::{Span, field, instrument};

use crate::{
	MultiVersionScope,
	tier::{
		DisplacedValues, HistoricalCursor, RangeBatch, RangeCursor, RawEntry, TierBackend, TierBatch,
		TierStorage, VersionedGetResult,
		commit::{
			census::CommitCensus,
			memory::entry::{
				CurrentMap, Entries, Entry, HistoricalMap, OldestIndex, entry_bytes, entry_bytes_with,
				oldest_version, reconcile_oldest,
			},
		},
	},
};

type EvictablePersist = Vec<(EncodedKey, CommitVersion, Option<CowVec<u8>>)>;
type EvictableDrop = Vec<EvictedVersion>;

#[derive(Clone, Debug)]
pub struct EvictedVersion {
	pub key: EncodedKey,
	pub version: CommitVersion,
	pub value_bytes: ByteSize,
	pub current: bool,
}

fn value_bytes_of(value: &Option<CowVec<u8>>) -> ByteSize {
	ByteSize::from_bytes(value.as_ref().map(|v| v.len() as u64).unwrap_or(0))
}

#[derive(Clone)]
pub struct MemoryRowStorage {
	inner: Arc<MemoryRowStorageInner>,
}

struct MemoryRowStorageInner {
	entries: Entries,
}

impl Default for MemoryRowStorage {
	fn default() -> Self {
		Self::new()
	}
}

impl MemoryRowStorage {
	#[instrument(name = "store::multi::memory::new", level = "debug")]
	pub fn new() -> Self {
		Self {
			inner: Arc::new(MemoryRowStorageInner {
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

	fn collect_oldest_pending(&self) -> Vec<(EntryKind, CommitVersion)> {
		self.inner
			.entries
			.data
			.keys()
			.into_iter()
			.filter_map(|kind| {
				let entry = self.inner.entries.data.get(&kind)?;
				let oldest = entry.oldest.read();
				let version = *oldest.keys().next()?;
				Some((kind, version))
			})
			.collect()
	}

	pub fn list_entry_kinds_by_oldest_pending(&self) -> Result<Vec<EntryKind>> {
		let mut pending = self.collect_oldest_pending();
		pending.sort_by_key(|(_, version)| *version);
		Ok(pending.into_iter().map(|(kind, _)| kind).collect())
	}

	pub fn oldest_pending_for(&self, kind: EntryKind) -> Option<CommitVersion> {
		let entry = self.inner.entries.data.get(&kind)?;
		let oldest = entry.oldest.read();
		oldest.keys().next().copied()
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

	pub fn current_resident_bytes(&self) -> ByteSize {
		let total = self
			.inner
			.entries
			.data
			.keys()
			.into_iter()
			.filter_map(|kind| self.inner.entries.data.get(&kind))
			.map(|entry| entry.bytes.current())
			.sum();
		ByteSize::from_bytes(total)
	}

	pub fn historical_resident_bytes(&self) -> ByteSize {
		let total = self
			.inner
			.entries
			.data
			.keys()
			.into_iter()
			.filter_map(|kind| self.inner.entries.data.get(&kind))
			.map(|entry| entry.bytes.historical())
			.sum();
		ByteSize::from_bytes(total)
	}

	pub fn census(&self) -> CommitCensus {
		let mut counted = 0u64;
		let mut walked = 0u64;
		for kind in self.inner.entries.data.keys() {
			let Some(entry) = self.inner.entries.data.get(&kind) else {
				continue;
			};
			let current = entry.current.read();
			let historical = entry.historical.read();
			counted = counted.saturating_add(entry.bytes.current());
			counted = counted.saturating_add(entry.bytes.historical());
			for (key, (_, value)) in current.iter() {
				walked = walked.saturating_add(entry_bytes(key, value));
			}
			for (key, versions) in historical.iter() {
				for value in versions.values() {
					walked = walked.saturating_add(entry_bytes(key, value));
				}
			}
		}
		CommitCensus {
			counted: ByteSize::from_bytes(counted),
			walked: ByteSize::from_bytes(walked),
		}
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
		displaced: &mut DisplacedValues,
	) {
		let table_entry = self.get_or_create_table(table);
		let (mut current, mut historical) = table_entry.write_pair();
		let mut oldest = table_entry.oldest.write();

		for (key, value) in entries {
			if let Some((pre_version, pre_value)) = current.get(&key) {
				if *pre_version < version {
					let pre_version = *pre_version;
					let pre_value = pre_value.clone();
					reifydb_assertions! {
						assert!(
							version.0 > pre_version.0,
							"promoting current entry to historical requires the incoming version to exceed it, otherwise the same version appears in both tiers and point-reads return the wrong entry (version={} pre_version={})",
							version.0,
							pre_version.0
						);
					}
					displaced.push((key.clone(), pre_value.as_ref().map_or(0, |v| v.len() as u64)));
					let pre_bytes = entry_bytes(&key, &pre_value);
					let new_bytes = entry_bytes(&key, &value);
					let replaced = historical
						.entry(key.clone())
						.or_default()
						.insert(Reverse(pre_version), pre_value);
					table_entry.bytes.add_historical(pre_bytes);
					if let Some(replaced) = replaced {
						table_entry.bytes.sub_historical(entry_bytes(&key, &replaced));
					}

					current.insert(key, (version, value));
					table_entry.bytes.add_current(new_bytes);
					table_entry.bytes.sub_current(pre_bytes);
				} else {
					let key_heap = key.heap_bytes();
					let new_bytes = entry_bytes_with(key_heap, &value);
					let old_oldest = oldest_version(&current, &historical, &key);
					let new_oldest = Some(old_oldest.map_or(version, |o| o.min(version)));
					let index_key = key.clone();
					let replaced =
						historical.entry(key).or_default().insert(Reverse(version), value);
					table_entry.bytes.add_historical(new_bytes);
					if let Some(replaced) = replaced {
						table_entry.bytes.sub_historical(entry_bytes_with(key_heap, &replaced));
					}
					reconcile_oldest(&mut oldest, &index_key, old_oldest, new_oldest);
				}
			} else {
				let new_bytes = entry_bytes(&key, &value);
				oldest.entry(version).or_default().insert(key.clone());
				current.insert(key, (version, value));
				table_entry.bytes.add_current(new_bytes);
			}
		}
	}

	pub fn oldest_pending_version(&self) -> Option<CommitVersion> {
		self.collect_oldest_pending().into_iter().map(|(_, version)| version).min()
	}

	pub fn collect_evictable_below(
		&self,
		table: EntryKind,
		cutoff: CommitVersion,
		budget: ByteSize,
	) -> (EvictablePersist, EvictableDrop, ByteSize, bool) {
		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => return (Vec::new(), Vec::new(), ByteSize::ZERO, false),
		};
		let current = entry.current.read();
		let historical = entry.historical.read();
		let oldest = entry.oldest.read();

		let budget = budget.as_bytes();
		let mut consumed = 0u64;
		let mut selected = 0usize;
		let mut latest: HashMap<EncodedKey, (CommitVersion, Option<CowVec<u8>>)> = HashMap::new();
		let mut to_drop: EvictableDrop = Vec::new();
		let mut more = false;
		'select: for (_bucket, keys) in oldest.range(..=cutoff) {
			for key in keys {
				if selected > 0 && consumed >= budget {
					more = true;
					break 'select;
				}
				selected += 1;
				let key_heap = key.heap_bytes();
				if let Some((v, val)) = current.get(key)
					&& *v <= cutoff
				{
					consumed += entry_bytes_with(key_heap, val);
					to_drop.push(EvictedVersion {
						key: key.clone(),
						version: *v,
						value_bytes: value_bytes_of(val),
						current: true,
					});
					latest.insert(key.clone(), (*v, val.clone()));
				}
				if let Some(versions) = historical.get(key) {
					for (Reverse(v), val) in versions.iter() {
						if *v <= cutoff {
							consumed += entry_bytes_with(key_heap, val);
							to_drop.push(EvictedVersion {
								key: key.clone(),
								version: *v,
								value_bytes: value_bytes_of(val),
								current: false,
							});
							match latest.get(key) {
								Some((best, _)) if *best >= *v => {}
								_ => {
									latest.insert(key.clone(), (*v, val.clone()));
								}
							}
						}
					}
				}
			}
		}

		let to_persist = latest.into_iter().map(|(key, (v, val))| (key, v, val)).collect();
		(to_persist, to_drop, ByteSize::from_bytes(consumed), more)
	}
}

impl TierStorage for MemoryRowStorage {
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
	fn set(&self, version: CommitVersion, batches: TierBatch) -> Result<DisplacedValues> {
		let total_entries: usize = batches.values().map(|v| v.len()).sum();

		let mut displaced = DisplacedValues::with_capacity(total_entries);
		batches.into_iter().for_each(|(table, entries)| {
			self.process_table(table, version, entries, &mut displaced);
		});

		Span::current().record("total_entry_count", total_entries);
		Ok(displaced)
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
		if cursor.is_exhausted() {
			return Ok(RangeBatch::empty());
		}

		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => {
				cursor.finish();
				return Ok(RangeBatch::empty());
			}
		};

		let cursor_key = cursor.last_key().cloned();

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
						} = scope && *v <= after
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
					} = scope && *v <= after
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
			cursor.advance(last_entry.key.clone());
		}
		if !has_more {
			cursor.finish();
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
		if cursor.is_exhausted() {
			return Ok(RangeBatch::empty());
		}

		let entry = match self.inner.entries.data.get(&table) {
			Some(e) => e,
			None => {
				cursor.finish();
				return Ok(RangeBatch::empty());
			}
		};

		let cursor_key = cursor.last_key().cloned();

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
						} = scope && *v <= after
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
					} = scope && *v <= after
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
			cursor.advance(last_entry.key.clone());
		}
		if !has_more {
			cursor.finish();
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
			*entry.oldest.write() = OldestIndex::new();
			entry.bytes.reset();
		}
		Ok(())
	}
}

impl MemoryRowStorage {
	#[instrument(name = "store::multi::memory::drop", level = "debug", skip(self, batches), fields(
		table_count = batches.len(),
		total_entry_count = field::Empty
	))]
	pub fn compact(
		&self,
		batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>>,
	) -> Result<Vec<EvictedVersion>> {
		let total_entries: usize = batches.values().map(|v| v.len()).sum();
		let mut removed: Vec<EvictedVersion> = Vec::with_capacity(total_entries);

		for (table, entries) in batches {
			let table_entry = self.get_or_create_table(table);
			let (mut current, mut historical) = table_entry.write_pair();
			let mut oldest = table_entry.oldest.write();

			let mut by_key: HashMap<EncodedKey, Vec<CommitVersion>> = HashMap::new();
			for (key, version) in entries {
				by_key.entry(key).or_default().push(version);
			}

			for (key, dropped_versions) in by_key {
				let old_oldest = oldest_version(&current, &historical, &key);
				let dropped_set: HashSet<CommitVersion> = dropped_versions.iter().copied().collect();

				let cur_version = current.get(&key).map(|(v, _)| *v);
				let stored_hist_covered = historical
					.get(&key)
					.map(|m| m.keys().all(|Reverse(v)| dropped_set.contains(v)))
					.unwrap_or(true);
				let stored_cur_covered = cur_version.is_none_or(|v| dropped_set.contains(&v));

				if stored_cur_covered && stored_hist_covered {
					if let Some((version, value)) = current.remove(&key) {
						table_entry.bytes.sub_current(entry_bytes(&key, &value));
						removed.push(EvictedVersion {
							key: key.clone(),
							version,
							value_bytes: value_bytes_of(&value),
							current: true,
						});
					}
					if let Some(versions) = historical.remove(&key) {
						for (Reverse(version), value) in versions.iter() {
							table_entry.bytes.sub_historical(entry_bytes(&key, value));
							removed.push(EvictedVersion {
								key: key.clone(),
								version: *version,
								value_bytes: value_bytes_of(value),
								current: false,
							});
						}
					}
					reconcile_oldest(&mut oldest, &key, old_oldest, None);
					continue;
				}

				for version in dropped_versions {
					let cur_matches = current.get(&key).map(|(v, _)| *v) == Some(version);
					if cur_matches {
						if let Some((version, value)) = current.remove(&key) {
							table_entry.bytes.sub_current(entry_bytes(&key, &value));
							removed.push(EvictedVersion {
								key: key.clone(),
								version,
								value_bytes: value_bytes_of(&value),
								current: true,
							});
						}
					} else {
						let now_empty = if let Some(versions) = historical.get_mut(&key) {
							if let Some(value) = versions.remove(&Reverse(version)) {
								table_entry
									.bytes
									.sub_historical(entry_bytes(&key, &value));
								removed.push(EvictedVersion {
									key: key.clone(),
									version,
									value_bytes: value_bytes_of(&value),
									current: false,
								});
							}
							versions.is_empty()
						} else {
							false
						};
						if now_empty {
							historical.remove(&key);
						}
					}
				}

				let new_oldest = oldest_version(&current, &historical, &key);
				reconcile_oldest(&mut oldest, &key, old_oldest, new_oldest);
			}
		}

		Span::current().record("total_entry_count", total_entries);
		Ok(removed)
	}

	#[instrument(name = "store::multi::memory::get_all_versions", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len()))]
	pub fn get_all_versions(
		&self,
		table: EntryKind,
		key: &[u8],
	) -> Result<Vec<(CommitVersion, Option<CowVec<u8>>)>> {
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
	pub fn scan_historical_below(
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

impl TierBackend for MemoryRowStorage {}

#[cfg(test)]
pub mod tests {
	use reifydb_core::interface::catalog::{id::TableId, storage::StorageId};

	use super::*;

	const UNBOUNDED: ByteSize = ByteSize::from_bytes(u64::MAX);

	fn budget_of(entries: &[(EncodedKey, Option<CowVec<u8>>)]) -> ByteSize {
		// The budget is expressed in the same accounting the buffer's residency counters use, so a test
		// budget must be derived from entry_bytes rather than from the value length alone.
		ByteSize::from_bytes(entries.iter().map(|(key, value)| entry_bytes(key, value)).sum())
	}

	fn keyed(name: &str, value: &[u8]) -> (EncodedKey, Option<CowVec<u8>>) {
		(EncodedKey::new(name.as_bytes().to_vec()), Some(CowVec::new(value.to_vec())))
	}

	fn seed(storage: &MemoryRowStorage, version: u64, entries: &[(EncodedKey, Option<CowVec<u8>>)]) {
		for (key, value) in entries {
			storage.set(
				CommitVersion(version),
				HashMap::from([(EntryKind::Multi, vec![(key.clone(), value.clone())])]),
			)
			.unwrap();
		}
	}

	#[test]
	fn test_basic_operations() {
		let storage = MemoryRowStorage::new();

		let key = EncodedKey::new(b"key1");
		let version = CommitVersion(1);

		storage.set(
			version,
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value1".to_vec())))])]),
		)
		.unwrap();

		let value = storage.get(EntryKind::Multi, &key, version).unwrap().value();
		assert_eq!(value.as_deref(), Some(b"value1".as_slice()));

		assert!(storage.contains(EntryKind::Multi, &key, version).unwrap());

		assert!(!storage.contains(EntryKind::Multi, b"nonexistent", version).unwrap());

		let version2 = CommitVersion(2);
		storage.set(version2, HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();
		assert!(!storage.contains(EntryKind::Multi, &key, version2).unwrap());
	}

	#[test]
	fn test_source_tables() {
		let storage = MemoryRowStorage::new();

		let source1 = StorageId::Table(TableId(1));
		let source2 = StorageId::Table(TableId(2));

		let key = EncodedKey::new(b"key");
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
		let storage = MemoryRowStorage::new();

		let key = EncodedKey::new(b"key1");

		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();

		storage.set(
			CommitVersion(2),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v2".to_vec())))])]),
		)
		.unwrap();

		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice())
		);

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().as_deref(),
			Some(b"v2".as_slice())
		);

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().as_deref(),
			Some(b"v1".as_slice())
		);
	}

	#[test]
	fn test_insert_older_version() {
		// An out-of-order older commit must stay resolvable: a read takes the largest version <= the
		// snapshot, so the v2 snapshot resolves to v1.
		let storage = MemoryRowStorage::new();

		let key = EncodedKey::new(b"key1");

		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();

		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice())
		);

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().as_deref(),
			Some(b"v1".as_slice())
		);

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().as_deref(),
			Some(b"v1".as_slice())
		);
	}

	#[test]
	fn test_range_next() {
		let storage = MemoryRowStorage::new();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(EncodedKey::new(b"a"), Some(CowVec::new(b"1".to_vec()))),
					(EncodedKey::new(b"b"), Some(CowVec::new(b"2".to_vec()))),
					(EncodedKey::new(b"c"), Some(CowVec::new(b"3".to_vec()))),
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
		assert!(cursor.is_exhausted());

		assert_eq!(&*batch.entries[0].key, b"a");
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(&*batch.entries[2].key, b"c");
	}

	#[test]
	fn test_range_rev_next() {
		let storage = MemoryRowStorage::new();

		let version = CommitVersion(1);
		storage.set(
			version,
			HashMap::from([(
				EntryKind::Multi,
				vec![
					(EncodedKey::new(b"a"), Some(CowVec::new(b"1".to_vec()))),
					(EncodedKey::new(b"b"), Some(CowVec::new(b"2".to_vec()))),
					(EncodedKey::new(b"c"), Some(CowVec::new(b"3".to_vec()))),
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
		assert!(cursor.is_exhausted());

		assert_eq!(&*batch.entries[0].key, b"c");
		assert_eq!(&*batch.entries[1].key, b"b");
		assert_eq!(&*batch.entries[2].key, b"a");
	}

	#[test]
	fn test_range_streaming_pagination() {
		let storage = MemoryRowStorage::new();

		let version = CommitVersion(1);

		let entries: Vec<_> =
			(0..10u8).map(|i| (EncodedKey::new(vec![i]), Some(CowVec::new(vec![i * 10])))).collect();
		storage.set(version, HashMap::from([(EntryKind::Multi, entries)])).unwrap();

		let mut cursor = RangeCursor::new();

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
		assert!(!cursor.is_exhausted());

		assert_eq!(&*batch1.entries[0].key, &[0]);
		assert_eq!(&*batch1.entries[2].key, &[2]);

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
		assert!(!cursor.is_exhausted());

		assert_eq!(&*batch2.entries[0].key, &[3]);
		assert_eq!(&*batch2.entries[2].key, &[5]);

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
		assert!(!cursor.is_exhausted());

		assert_eq!(&*batch3.entries[0].key, &[6]);
		assert_eq!(&*batch3.entries[2].key, &[8]);

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
		assert!(cursor.is_exhausted());

		assert_eq!(&*batch4.entries[0].key, &[9]);

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
		let storage = MemoryRowStorage::new();

		let version = CommitVersion(1);

		let entries: Vec<_> =
			(0..10u8).map(|i| (EncodedKey::new(vec![i]), Some(CowVec::new(vec![i * 10])))).collect();
		storage.set(version, HashMap::from([(EntryKind::Multi, entries)])).unwrap();

		let mut cursor = RangeCursor::new();

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
		assert!(!cursor.is_exhausted());

		assert_eq!(&*batch1.entries[0].key, &[9]);
		assert_eq!(&*batch1.entries[2].key, &[7]);

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
		assert!(!cursor.is_exhausted());

		assert_eq!(&*batch2.entries[0].key, &[6]);
		assert_eq!(&*batch2.entries[2].key, &[4]);
	}

	#[test]
	fn test_drop_from_historical() {
		let storage = MemoryRowStorage::new();

		let key = EncodedKey::new(b"key1");

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

		storage.compact(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(1))])])).unwrap();

		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().is_none());

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
	fn compact_returns_each_removed_historical_version_flagged_as_not_current() {
		// The storage metric only ever decrements from what compact reports back, so a version removed
		// physically but omitted from the return value inflates historical_count forever.
		let storage = MemoryRowStorage::new();

		let key = EncodedKey::new(b"key1");

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

		let removed = storage
			.compact(HashMap::from([(
				EntryKind::Multi,
				vec![(key.clone(), CommitVersion(1)), (key.clone(), CommitVersion(2))],
			)]))
			.unwrap();

		let mut versions: Vec<u64> = removed.iter().map(|entry| entry.version.0).collect();
		versions.sort_unstable();
		assert_eq!(versions, vec![1, 2]);
		assert!(removed.iter().all(|entry| !entry.current));
		assert!(removed.iter().all(|entry| entry.value_bytes == ByteSize::from_bytes(2)));
	}

	#[test]
	fn compact_reports_the_live_version_and_leaves_surviving_history_in_place() {
		// Dropping the live version does not promote the newest survivor, so the removal is only visible
		// to the metric through the returned record; the survivors must stay in historical, unreported
		// and still readable, which is what makes skipping the promotion safe.
		let storage = MemoryRowStorage::new();

		let key = EncodedKey::new(b"key1");

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

		let removed = storage
			.compact(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(3))])]))
			.unwrap();

		assert_eq!(removed.len(), 1, "only the live version was dropped");
		assert_eq!(removed[0].version, CommitVersion(3));
		assert!(removed[0].current, "the dropped version was the live one");
		assert_eq!(removed[0].value_bytes, ByteSize::from_bytes(2));

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v2".as_slice()),
			"the newest survivor is still readable from historical without being promoted"
		);
	}

	#[test]
	fn compact_reports_the_live_entry_when_every_version_of_a_key_is_removed() {
		// The metric routes a current removal to the current counters and a historical one to the
		// historical counters, so mislabelling the live entry moves rows between the two columns
		// instead of clearing them.
		let storage = MemoryRowStorage::new();

		let key = EncodedKey::new(b"key1");

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

		let removed = storage
			.compact(HashMap::from([(
				EntryKind::Multi,
				vec![
					(key.clone(), CommitVersion(1)),
					(key.clone(), CommitVersion(2)),
					(key.clone(), CommitVersion(3)),
				],
			)]))
			.unwrap();

		assert_eq!(removed.len(), 3);

		let live: Vec<u64> =
			removed.iter().filter(|entry| entry.current).map(|entry| entry.version.0).collect();
		assert_eq!(live, vec![3]);

		let mut historical: Vec<u64> =
			removed.iter().filter(|entry| !entry.current).map(|entry| entry.version.0).collect();
		historical.sort_unstable();
		assert_eq!(historical, vec![1, 2]);
	}

	#[test]
	fn test_tombstones() {
		let storage = MemoryRowStorage::new();

		let key = EncodedKey::new(b"key1");

		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"value".to_vec())))])]),
		)
		.unwrap();

		storage.set(CommitVersion(2), HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();

		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().is_none());
		assert!(!storage.contains(EntryKind::Multi, &key, CommitVersion(2)).unwrap());

		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().as_deref(),
			Some(b"value".as_slice())
		);
	}

	#[test]
	fn test_collect_evictable_below_keeps_versions_above_cutoff() {
		let storage = MemoryRowStorage::new();
		let key = EncodedKey::new(b"k");
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

		// v2 is what a reader in [2, 3) resolves to, so it is the value that must be persisted.
		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(2), UNBOUNDED);
		assert_eq!(to_persist.len(), 1);
		assert_eq!(to_persist[0].0, key);
		assert_eq!(to_persist[0].1, CommitVersion(2));
		assert_eq!(to_persist[0].2.as_deref(), Some(b"v2".as_slice()));
		let dropped: HashSet<CommitVersion> = to_drop.iter().map(|e| e.version).collect();
		assert_eq!(dropped, HashSet::from([CommitVersion(1), CommitVersion(2)]));

		storage.compact(HashMap::from([(
			EntryKind::Multi,
			to_drop.into_iter().map(|e| (e.key, e.version)).collect(),
		)]))
		.unwrap();
		assert_eq!(
			storage.get(EntryKind::Multi, &key, CommitVersion(3)).unwrap().value().as_deref(),
			Some(b"v3".as_slice())
		);
		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(2)).unwrap().value().is_none());
		assert!(storage.get(EntryKind::Multi, &key, CommitVersion(1)).unwrap().value().is_none());
	}

	#[test]
	fn test_collect_evictable_below_empty_when_all_above_cutoff() {
		let storage = MemoryRowStorage::new();
		let key = EncodedKey::new(b"k");
		storage.set(
			CommitVersion(5),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v".to_vec())))])]),
		)
		.unwrap();
		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(3), UNBOUNDED);
		assert!(to_persist.is_empty());
		assert!(to_drop.is_empty());
	}

	#[test]
	fn test_collect_evictable_below_persists_exactly_one_value_per_key() {
		// Only the latest-<=cutoff value may be persisted: it is the single value a reader at the cutoff
		// snapshot resolves to, so persisting an older one corrupts that resolution.
		let storage = MemoryRowStorage::new();
		let key = EncodedKey::new(b"k");
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

		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(4), UNBOUNDED);
		assert_eq!(to_persist.len(), 1, "exactly one value persisted per key");
		assert_eq!(to_persist[0].1, CommitVersion(4), "the latest version <= cutoff");
		assert_eq!(to_persist[0].2.as_deref(), Some(b"v4".as_slice()));

		let dropped: HashSet<CommitVersion> = to_drop.iter().map(|e| e.version).collect();
		assert_eq!(
			dropped,
			HashSet::from([CommitVersion(1), CommitVersion(2), CommitVersion(3), CommitVersion(4)])
		);
	}

	#[test]
	fn test_collect_evictable_below_persists_tombstone_when_it_is_the_latest() {
		// A tombstone that is the latest-<=cutoff version must be carried to the persistent tier; dropping
		// it lets a later read resurrect the pre-delete value.
		let storage = MemoryRowStorage::new();
		let key = EncodedKey::new(b"k");
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v1".to_vec())))])]),
		)
		.unwrap();
		storage.set(CommitVersion(2), HashMap::from([(EntryKind::Multi, vec![(key.clone(), None)])])).unwrap();

		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(2), UNBOUNDED);
		assert_eq!(to_persist.len(), 1);
		assert_eq!(to_persist[0].1, CommitVersion(2), "the tombstone is the latest version");
		assert!(to_persist[0].2.is_none(), "the persisted latest value must be the tombstone, not v1");
		assert_eq!(to_drop.len(), 2, "both v1 and the tombstone are dropped from the buffer");
	}

	#[test]
	fn test_collect_evictable_below_only_drops_historical_when_current_is_above_cutoff() {
		// A key that is actively written while old snapshots age out: only the historical version may be
		// evicted, the current one is still hot and must not be persisted.
		let storage = MemoryRowStorage::new();
		let key = EncodedKey::new(b"k");
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

		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(3), UNBOUNDED);
		assert_eq!(to_persist.len(), 1);
		assert_eq!(to_persist[0].1, CommitVersion(2), "only the aged-out historical version is persisted");
		assert_eq!(to_persist[0].2.as_deref(), Some(b"v2".as_slice()));
		let dropped: HashSet<CommitVersion> = to_drop.iter().map(|e| e.version).collect();
		assert_eq!(dropped, HashSet::from([CommitVersion(2)]), "v5 (current, > cutoff) is never dropped");

		storage.compact(HashMap::from([(
			EntryKind::Multi,
			to_drop.into_iter().map(|e| (e.key, e.version)).collect(),
		)]))
		.unwrap();
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
		// The cutoff applies per version, not per key: a key whose only version is above it must stay
		// fully resident even while a sibling key is evicted.
		let storage = MemoryRowStorage::new();
		let cold = EncodedKey::new(b"cold");
		let hot = EncodedKey::new(b"hot");
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

		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(5), UNBOUNDED);
		assert_eq!(to_persist.len(), 1, "only the cold key is evictable below the cutoff");
		assert_eq!(to_persist[0].0, cold);
		assert!(to_drop.iter().all(|e| e.key == cold), "the hot key must not be scheduled for drop");
	}

	#[test]
	fn test_collect_evictable_below_bounds_to_budget_and_drains_across_calls() {
		// The budget bounds a flush slice so one transaction never persists the whole evictable set;
		// looping bounded calls must still drain exactly the below-cutoff set, no more, no less.
		let storage = MemoryRowStorage::new();
		let entries: Vec<(EncodedKey, Option<CowVec<u8>>)> =
			(0..5u8).map(|i| keyed(&format!("k{i}"), &[i])).collect();
		seed(&storage, 1, &entries);
		let budget = budget_of(&entries[..2]);

		let (to_persist, to_drop, consumed, more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(1), budget);
		assert_eq!(to_persist.len(), 2, "the budget only covers two entries' worth of bytes");
		assert_eq!(to_drop.len(), 2);
		assert_eq!(consumed, budget, "the reported spend must be the bytes actually selected");
		assert!(more, "three keys remain below the cutoff");

		let mut drained = to_persist.len();
		let mut compaction_batch: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
		compaction_batch.insert(EntryKind::Multi, to_drop.into_iter().map(|e| (e.key, e.version)).collect());
		storage.compact(compaction_batch).unwrap();
		loop {
			let (p, d, _consumed, more) =
				storage.collect_evictable_below(EntryKind::Multi, CommitVersion(1), budget);
			if p.is_empty() {
				assert!(!more, "an empty collect must not claim more remains");
				break;
			}
			drained += p.len();
			let mut batch: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>> = HashMap::new();
			batch.insert(EntryKind::Multi, d.into_iter().map(|e| (e.key, e.version)).collect());
			storage.compact(batch).unwrap();
			if !more {
				break;
			}
		}
		assert_eq!(drained, 5, "every below-cutoff key is drained exactly once");
	}

	#[test]
	fn collect_evictable_below_bounds_the_slice_by_bytes_not_by_key_count() {
		// A key-count budget lets one slice pull an unbounded number of bytes: N fat rows cost the same
		// as N thin ones. The same byte budget must therefore admit strictly fewer fat keys than thin
		// ones, which is exactly what a count-based cap cannot do.
		let thin: Vec<(EncodedKey, Option<CowVec<u8>>)> =
			(0..8u8).map(|i| keyed(&format!("k{i}"), &[i])).collect();
		let fat: Vec<(EncodedKey, Option<CowVec<u8>>)> =
			(0..8u8).map(|i| keyed(&format!("k{i}"), &vec![i; 4096])).collect();

		let budget = budget_of(&thin[..4]);

		let thin_storage = MemoryRowStorage::new();
		seed(&thin_storage, 1, &thin);
		let (thin_persist, _, _, thin_more) =
			thin_storage.collect_evictable_below(EntryKind::Multi, CommitVersion(1), budget);

		let fat_storage = MemoryRowStorage::new();
		seed(&fat_storage, 1, &fat);
		let (fat_persist, _, fat_consumed, fat_more) =
			fat_storage.collect_evictable_below(EntryKind::Multi, CommitVersion(1), budget);

		assert_eq!(thin_persist.len(), 4, "four thin entries is exactly what the budget buys");
		assert!(thin_more, "four of the eight thin keys are still pending");
		assert_eq!(fat_persist.len(), 1, "a single fat entry already exceeds the same byte budget");
		assert!(fat_more, "the remaining fat keys are still pending");
		assert!(
			fat_consumed.as_bytes() > budget.as_bytes(),
			"the one admitted fat entry is what pushed the slice over the budget"
		);
	}

	#[test]
	fn collect_evictable_below_always_admits_one_entry_even_when_the_budget_cannot_cover_it() {
		// A slice must never collect nothing while work is pending: an entry no budget can cover would
		// otherwise be skipped every slice forever, and the key it pins holds the durable frontier at its
		// commit version, which clamps the tombstone reap cutoff to zero.
		let storage = MemoryRowStorage::new();
		let entries = vec![keyed("huge-a", &vec![7u8; 65536]), keyed("huge-b", &vec![9u8; 65536])];
		seed(&storage, 1, &entries);

		let (to_persist, to_drop, consumed, more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(1), ByteSize::from_bytes(1));
		assert_eq!(to_persist.len(), 1, "the oversized entry must be admitted, not skipped");
		assert_eq!(to_drop.len(), 1);
		assert!(consumed.as_bytes() > 65536, "the whole oversized entry counts against the slice");
		assert!(more, "the second oversized entry is still pending");

		let (to_persist, _, consumed, more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(1), ByteSize::ZERO);
		assert_eq!(to_persist.len(), 1, "even a zero budget must still release exactly one entry");
		assert!(consumed.as_bytes() > 65536);
		assert!(more, "a zero budget yields after the one entry it was forced to take");
	}

	#[test]
	fn collect_evictable_below_counts_every_evicted_version_of_a_key_against_the_budget() {
		// A key's superseded versions leave the buffer in the same slice as its live one, so a budget
		// that only charged for the live version would let a deep version chain blow the slice's byte
		// ceiling without ever reporting it.
		let storage = MemoryRowStorage::new();
		let key = EncodedKey::new(b"k".to_vec());
		for v in 1..=4u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(vec![v as u8; 512])))],
				)]),
			)
			.unwrap();
		}

		let (_, to_drop, consumed, _) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(4), UNBOUNDED);

		assert_eq!(to_drop.len(), 4, "all four versions leave the buffer");
		let expected: u64 = (1..=4u64).map(|v| entry_bytes(&key, &Some(CowVec::new(vec![v as u8; 512])))).sum();
		assert_eq!(
			consumed,
			ByteSize::from_bytes(expected),
			"the slice must charge itself for every version it evicted, not just the live one"
		);
	}

	fn indexed_oldest(storage: &MemoryRowStorage, table: EntryKind, key: &EncodedKey) -> Option<CommitVersion> {
		let entry = storage.inner.entries.data.get(&table)?;
		let oldest = entry.oldest.read();
		oldest.iter().find(|(_, keys)| keys.contains(key)).map(|(v, _)| *v)
	}

	fn assert_index_consistent(storage: &MemoryRowStorage, table: EntryKind) {
		let entry = storage.inner.entries.data.get(&table).expect("table exists");
		let current = entry.current.read();
		let historical = entry.historical.read();
		let oldest = entry.oldest.read();

		// A key indexed anywhere but its smallest stored version can never be selected, so its old
		// versions leak in the buffer forever.
		let mut resident: HashSet<EncodedKey> = HashSet::new();
		resident.extend(current.keys().cloned());
		resident.extend(historical.keys().cloned());
		for key in &resident {
			let expected = oldest_version(&current, &historical, key);
			let indexed = oldest.iter().find(|(_, keys)| keys.contains(key)).map(|(v, _)| *v);
			assert_eq!(
				indexed, expected,
				"a resident key must sit in the index bucket of its smallest version"
			);
		}

		// A stale index entry (key in neither map) would be re-selected on every sweep and never clear.
		for (bucket, keys) in oldest.iter() {
			for key in keys {
				assert!(
					resident.contains(key),
					"index holds a key that is in neither map (stale entry)"
				);
				assert_eq!(
					oldest_version(&current, &historical, key),
					Some(*bucket),
					"index bucket must equal the key's smallest stored version"
				);
			}
		}
	}

	#[test]
	fn index_stays_consistent_across_new_monotonic_out_of_order_and_drops() {
		// The eviction index is what keeps collect_evictable_below O(evictable) instead of O(table);
		// drift from the maps either strands a key forever or churns a ghost, so every maintenance path
		// is cross-checked against a full walk of both maps.
		let storage = MemoryRowStorage::new();
		let kind = EntryKind::Multi;
		let a = EncodedKey::new(b"a");
		let b = EncodedKey::new(b"b");
		let c = EncodedKey::new(b"c");

		let set = |v: u64, key: &EncodedKey, val: &str| {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					kind,
					vec![(key.clone(), Some(CowVec::new(val.as_bytes().to_vec())))],
				)]),
			)
			.unwrap();
		};

		set(10, &a, "a10");
		set(20, &a, "a20");
		set(3, &a, "a3");
		set(5, &b, "b5");
		set(7, &c, "c7");
		assert_eq!(
			indexed_oldest(&storage, kind, &a),
			Some(CommitVersion(3)),
			"an out-of-order write below the current version must lower a's bucket to 3"
		);
		assert_index_consistent(&storage, kind);

		storage.compact(HashMap::from([(kind, vec![(a.clone(), CommitVersion(3))])])).unwrap();
		assert_eq!(
			indexed_oldest(&storage, kind, &a),
			Some(CommitVersion(10)),
			"dropping the oldest version must raise the bucket to the next-smallest stored version"
		);
		assert_index_consistent(&storage, kind);

		storage.compact(HashMap::from([(kind, vec![(b.clone(), CommitVersion(5))])])).unwrap();
		assert_eq!(
			indexed_oldest(&storage, kind, &b),
			None,
			"a fully dropped key must leave the index entirely"
		);
		assert_index_consistent(&storage, kind);
	}

	#[test]
	fn out_of_order_landing_is_selected_for_eviction() {
		// A late or replayed commit landing below the current version becomes the key's oldest; an index
		// that tracked only first-seen versions would strand it in the buffer forever.
		let storage = MemoryRowStorage::new();
		let key = EncodedKey::new(b"k");
		storage.set(
			CommitVersion(20),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v20".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();

		let (to_persist, to_drop, _consumed, _more) =
			storage.collect_evictable_below(EntryKind::Multi, CommitVersion(5), UNBOUNDED);
		let dropped: HashSet<CommitVersion> = to_drop.iter().map(|e| e.version).collect();
		assert_eq!(
			dropped,
			HashSet::from([CommitVersion(3)]),
			"the out-of-order v3 must be selected; v20 stays resident"
		);
		assert_eq!(to_persist.len(), 1);
		assert_eq!(to_persist[0].1, CommitVersion(3), "the aged-out v3 is the value persisted");
	}

	#[test]
	fn byte_tally_matches_a_full_walk_across_mixed_mutations() {
		// The tally is incremental, so any drift from the true map contents misreports memory forever
		// after; every mutation shape is exercised then compared against an exhaustive walk.
		let storage = MemoryRowStorage::new();
		let k1 = EncodedKey::new(b"key-one");
		let k2 = EncodedKey::new(b"key-two");

		for v in 1..=3u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(k1.clone(), Some(CowVec::new(format!("value-{v}").into_bytes())))],
				)]),
			)
			.unwrap();
		}
		storage.set(
			CommitVersion(5),
			HashMap::from([(EntryKind::Multi, vec![(k2.clone(), Some(CowVec::new(b"x".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(2),
			HashMap::from([(EntryKind::Multi, vec![(k2.clone(), Some(CowVec::new(b"older".to_vec())))])]),
		)
		.unwrap();
		storage.set(CommitVersion(6), HashMap::from([(EntryKind::Multi, vec![(k2.clone(), None)])])).unwrap();

		storage.compact(HashMap::from([(EntryKind::Multi, vec![(k1.clone(), CommitVersion(3))])])).unwrap();
		storage.compact(HashMap::from([(EntryKind::Multi, vec![(k2.clone(), CommitVersion(2))])])).unwrap();

		let entry = storage.inner.entries.data.get(&EntryKind::Multi).unwrap();
		let current = entry.current.read();
		let walked_current: u64 = current.iter().map(|(k, (_, v))| entry_bytes(k, v)).sum();
		drop(current);
		let historical = entry.historical.read();
		let walked_historical: u64 = historical
			.iter()
			.map(|(k, versions)| versions.values().map(|v| entry_bytes(k, v)).sum::<u64>())
			.sum();
		drop(historical);

		assert!(walked_current > 0, "precondition: the scenario must leave current entries behind");
		assert!(walked_historical > 0, "precondition: the scenario must leave historical entries behind");
		assert_eq!(
			storage.current_resident_bytes().as_bytes(),
			walked_current,
			"the incremental current tally must equal an exhaustive walk of the current map"
		);
		assert_eq!(
			storage.historical_resident_bytes().as_bytes(),
			walked_historical,
			"the incremental historical tally must equal an exhaustive walk of the historical map"
		);
	}

	#[test]
	fn byte_tally_nets_to_zero_when_the_buffer_is_fully_drained() {
		// Eviction drains the buffer continuously, so a leak in any release path accumulates into a
		// permanently inflated memory report; the live-version drop is included in the sequence.
		let storage = MemoryRowStorage::new();
		let key = EncodedKey::new(b"k");
		for v in 1..=4u64 {
			storage.set(
				CommitVersion(v),
				HashMap::from([(
					EntryKind::Multi,
					vec![(key.clone(), Some(CowVec::new(format!("v{v}").into_bytes())))],
				)]),
			)
			.unwrap();
		}
		assert!(storage.current_resident_bytes().as_bytes() > 0);
		assert!(storage.historical_resident_bytes().as_bytes() > 0);

		storage.compact(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(4))])])).unwrap();
		storage.compact(HashMap::from([(
			EntryKind::Multi,
			vec![
				(key.clone(), CommitVersion(1)),
				(key.clone(), CommitVersion(2)),
				(key.clone(), CommitVersion(3)),
			],
		)]))
		.unwrap();

		assert_eq!(
			storage.current_resident_bytes(),
			ByteSize::ZERO,
			"draining every entry must return the current tally to zero"
		);
		assert_eq!(
			storage.historical_resident_bytes(),
			ByteSize::ZERO,
			"draining every entry must return the historical tally to zero"
		);
	}

	#[test]
	fn clear_table_resets_the_byte_tally() {
		let storage = MemoryRowStorage::new();
		let key = EncodedKey::new(b"k");
		storage.set(
			CommitVersion(1),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(2),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"w".to_vec())))])]),
		)
		.unwrap();
		assert!(storage.current_resident_bytes().as_bytes() > 0);
		assert!(storage.historical_resident_bytes().as_bytes() > 0);

		storage.clear_table(EntryKind::Multi).unwrap();
		assert_eq!(
			storage.current_resident_bytes(),
			ByteSize::ZERO,
			"clearing a table must zero its byte tally, not leak it"
		);
		assert_eq!(storage.historical_resident_bytes(), ByteSize::ZERO);
	}

	#[test]
	fn an_empty_buffer_has_no_oldest_pending_version() {
		// None means "nothing is waiting to be flushed", which is what lets a retention floor sit at
		// the permitted watermark. Reporting a version here would peg the floor to a write that
		// does not exist.
		let storage = MemoryRowStorage::new();

		assert_eq!(storage.oldest_pending_version(), None);
	}

	#[test]
	fn oldest_pending_version_is_the_minimum_across_every_entry_kind() {
		// The retention floor is global, so a single lagging keyspace has to hold it down. Taking a
		// per-kind minimum, or the newest version instead of the oldest, is what lets the tombstone
		// reaper delete rows an un-flushed write is about to rewrite.
		let storage = MemoryRowStorage::new();
		let key = EncodedKey::new(b"k");

		storage.set(
			CommitVersion(40),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"late".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(7),
			HashMap::from([(
				EntryKind::Source(StorageId::Table(TableId(1))),
				vec![(key.clone(), Some(CowVec::new(b"early".to_vec())))],
			)]),
		)
		.unwrap();
		storage.set(
			CommitVersion(19),
			HashMap::from([(
				EntryKind::Source(StorageId::Table(TableId(2))),
				vec![(key.clone(), Some(CowVec::new(b"mid".to_vec())))],
			)]),
		)
		.unwrap();

		assert_eq!(
			storage.oldest_pending_version(),
			Some(CommitVersion(7)),
			"the oldest un-flushed write in any keyspace is what bounds the durable frontier"
		);
	}

	#[test]
	fn the_oldest_bucket_in_a_keyspace_wins_over_its_newer_ones() {
		// One keyspace holds many keys, each indexed under its own oldest resident version. Reading
		// the newest bucket instead of the oldest reports a frontier above writes that are still
		// buffered - and the tombstone reaper deletes under exactly that frontier.
		let storage = MemoryRowStorage::new();

		storage.set(
			CommitVersion(11),
			HashMap::from([(
				EntryKind::Multi,
				vec![(EncodedKey::new(b"late"), Some(CowVec::new(b"v".to_vec())))],
			)]),
		)
		.unwrap();
		storage.set(
			CommitVersion(4),
			HashMap::from([(
				EntryKind::Multi,
				vec![(EncodedKey::new(b"early"), Some(CowVec::new(b"v".to_vec())))],
			)]),
		)
		.unwrap();

		assert_eq!(storage.oldest_pending_version(), Some(CommitVersion(4)));
	}

	#[test]
	fn a_superseded_version_still_counts_as_pending_until_it_is_compacted_away() {
		// Overwriting a key moves the old version into the historical map; it is still resident and
		// still un-flushed. If the index followed the current version instead, the frontier would
		// jump past a version the flusher has not written yet.
		let storage = MemoryRowStorage::new();
		let key = EncodedKey::new(b"k");

		storage.set(
			CommitVersion(3),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v3".to_vec())))])]),
		)
		.unwrap();
		storage.set(
			CommitVersion(9),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v9".to_vec())))])]),
		)
		.unwrap();

		assert_eq!(storage.oldest_pending_version(), Some(CommitVersion(3)));

		storage.compact(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(3))])])).unwrap();

		assert_eq!(
			storage.oldest_pending_version(),
			Some(CommitVersion(9)),
			"once the sweep drains v3 the frontier may advance to the next un-flushed write"
		);
	}

	#[test]
	fn draining_the_buffer_clears_the_oldest_pending_version() {
		// A sweep that empties a keyspace has to retire its bucket from the index. A stale bucket
		// pins the retention floor at a version that is already durable, and reclamation stalls
		// forever with no failing symptom.
		let storage = MemoryRowStorage::new();
		let key = EncodedKey::new(b"k");

		storage.set(
			CommitVersion(5),
			HashMap::from([(EntryKind::Multi, vec![(key.clone(), Some(CowVec::new(b"v".to_vec())))])]),
		)
		.unwrap();
		storage.compact(HashMap::from([(EntryKind::Multi, vec![(key.clone(), CommitVersion(5))])])).unwrap();

		assert_eq!(storage.oldest_pending_version(), None);
	}
}
