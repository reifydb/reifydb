// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::HashMap,
	iter,
	ops::{Bound, ControlFlow},
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{
	common::CommitVersion,
	interface::store::EntryKind,
	metrics::{collect::MetricsCollector, heap::HeapSize, sample::MetricsSample},
};
use reifydb_runtime::sync::{Arc, map::Map};
use reifydb_value::{byte_size::ByteSize, count::Count, reifydb_assertions, util::cowvec::CowVec};
use tracing::{Span, field, instrument};

use crate::{
	MultiVersionScope, RangeBatch, RangeCursor, RawEntry, TierBatch, VersionedGetResult,
	config::Config,
	entry::{Entry, entry_bytes},
	memory::MemoryRows,
	storage::{Read, Remove, Rows, Write},
};

type EvictablePersist = Vec<(EncodedKey, CommitVersion, Option<CowVec<u8>>)>;
type EvictableDrop = Vec<(EncodedKey, CommitVersion)>;

#[derive(Clone, Debug)]
pub struct EvictedVersion {
	pub key: EncodedKey,
	pub version: CommitVersion,
	pub value_bytes: ByteSize,
	pub current: bool,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MultiCommitMetrics {
	pub current_bytes: ByteSize,
	pub historical_bytes: ByteSize,
	pub table_count: Count,
	pub current_entries: Count,
	pub queued_bytes: ByteSize,
}

pub struct CommitStore<R = MemoryRows>(Arc<Inner<R>>);

struct Inner<R> {
	rows: R,
	entries: Map<EntryKind, Arc<Entry<R>>>,
	config: Config,
}

impl<R> Clone for CommitStore<R> {
	fn clone(&self) -> Self {
		Self(Arc::clone(&self.0))
	}
}

impl<R> CommitStore<R> {
	pub fn list_all_entry_kinds(&self) -> Vec<EntryKind> {
		self.0.entries.keys()
	}
}

impl<R: Rows> CommitStore<R> {
	pub fn open(rows: R, config: Config) -> Self {
		Self(Arc::new(Inner {
			rows,
			entries: Map::new(),
			config,
		}))
	}

	#[instrument(name = "store::commit::ensure_table", level = "trace", skip(self), fields(table = ?table))]
	pub fn ensure_table(&self, table: EntryKind) {
		self.get_or_create_table(table);
	}

	#[inline]
	#[instrument(name = "store::commit::get_or_create_table", level = "trace", skip(self), fields(table = ?table))]
	fn get_or_create_table(&self, table: EntryKind) -> Arc<Entry<R>> {
		self.0.entries.get_or_insert_with(table, || Arc::new(Entry::new(self.0.rows.empty())))
	}
}

impl CommitStore {
	#[instrument(name = "store::commit::new", level = "debug")]
	pub fn new() -> Self {
		Self::open(MemoryRows::default(), Config::default())
	}
}

impl Default for CommitStore {
	fn default() -> Self {
		Self::new()
	}
}

impl<R: Read> CommitStore<R> {
	#[instrument(name = "store::commit::get", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len(), version = version.0))]
	pub fn get(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> VersionedGetResult {
		let Some(entry) = self.0.entries.get(&table) else {
			return VersionedGetResult::NotFound;
		};
		match entry.rows.get(key, version) {
			Some((found, Some(value))) => VersionedGetResult::Value {
				value,
				version: found,
			},
			Some((_, None)) => VersionedGetResult::Tombstone,
			None => VersionedGetResult::NotFound,
		}
	}

	#[instrument(name = "store::commit::contains", level = "trace", skip(self, key), fields(table = ?table, key_len = key.len(), version = version.0), ret)]
	pub fn contains(&self, table: EntryKind, key: &[u8], version: CommitVersion) -> bool {
		matches!(self.get(table, key, version), VersionedGetResult::Value { .. })
	}

	#[instrument(name = "store::commit::range_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size, scope = ?scope))]
	pub fn range_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> RangeBatch {
		let mut entries = Vec::with_capacity(batch_size + 1);
		let has_more = self.range_into(table, cursor, start, end, scope, batch_size, false, &mut entries);
		RangeBatch {
			entries,
			has_more,
		}
	}

	#[allow(clippy::too_many_arguments)]
	pub fn range_next_into(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
		entries: &mut Vec<RawEntry>,
	) -> bool {
		self.range_into(table, cursor, start, end, scope, batch_size, false, entries)
	}

	#[instrument(name = "store::commit::range_rev_next", level = "trace", skip(self, cursor, start, end), fields(table = ?table, batch_size = batch_size, scope = ?scope))]
	pub fn range_rev_next(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
	) -> RangeBatch {
		let mut entries = Vec::with_capacity(batch_size + 1);
		let has_more = self.range_into(table, cursor, start, end, scope, batch_size, true, &mut entries);
		RangeBatch {
			entries,
			has_more,
		}
	}

	#[allow(clippy::too_many_arguments)]
	fn range_into(
		&self,
		table: EntryKind,
		cursor: &mut RangeCursor,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		scope: MultiVersionScope,
		batch_size: usize,
		reverse: bool,
		entries: &mut Vec<RawEntry>,
	) -> bool {
		entries.clear();

		if cursor.is_exhausted() {
			return false;
		}

		let Some(entry) = self.0.entries.get(&table) else {
			cursor.finish();
			return false;
		};

		let last = cursor.last_key().cloned();
		let (start, end) = match &last {
			Some(last) if reverse => (start, Bound::Excluded(last.as_slice())),
			Some(last) => (Bound::Excluded(last.as_slice()), end),
			None => (start, end),
		};

		entries.reserve(batch_size + 1);
		entry.rows.scan(start, end, reverse, |key, versions| {
			if let Some((version, value)) = versions.iter().find(|(version, _)| *version <= scope.read())
				&& scope.contains(*version)
			{
				entries.push(RawEntry {
					key: key.clone(),
					version: *version,
					value: (*value).clone(),
				});
			}
			if entries.len() > batch_size {
				ControlFlow::Break(())
			} else {
				ControlFlow::Continue(())
			}
		});

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

		has_more
	}

	pub fn estimated_current_count(&self, table: EntryKind) -> u64 {
		self.0.entries.get(&table).map_or(0, |entry| entry.rows.stats().keys)
	}

	pub fn list_entry_kinds_by_oldest_pending(&self) -> Vec<EntryKind> {
		let mut kinds: Vec<(EntryKind, CommitVersion)> =
			self.0.entries
				.keys()
				.into_iter()
				.filter_map(|kind| Some((kind, self.0.entries.get(&kind)?.rows.stats().oldest?)))
				.collect();
		kinds.sort_by_key(|(_, oldest)| *oldest);
		kinds.into_iter().map(|(kind, _)| kind).collect()
	}

	pub fn metrics(&self) -> MultiCommitMetrics {
		let kinds = self.0.entries.keys();
		let mut metrics = MultiCommitMetrics {
			table_count: Count::new(kinds.len() as u64),
			..MultiCommitMetrics::default()
		};
		for kind in kinds {
			let Some(entry) = self.0.entries.get(&kind) else {
				continue;
			};
			let stats = entry.rows.stats();
			metrics.current_bytes = metrics.current_bytes.saturating_add(stats.current_bytes);
			metrics.historical_bytes = metrics.historical_bytes.saturating_add(stats.historical_bytes);
			metrics.current_entries = metrics.current_entries.saturating_add(Count::new(stats.keys));
			let pending = entry.pending.lock().heap_size();
			let retained = entry.retained.lock().heap_size();
			metrics.queued_bytes =
				metrics.queued_bytes.saturating_add(ByteSize::from_bytes((pending + retained) as u64));
		}
		metrics
	}
}

impl<R: Write> CommitStore<R> {
	#[instrument(name = "store::commit::set", level = "trace", skip(self, batches), fields(
		table_count = batches.len(),
		total_entry_count = field::Empty,
		version = version.0
	))]
	pub fn set(&self, version: CommitVersion, batches: TierBatch) {
		let total_entries: usize = batches.values().map(|rows| rows.len()).sum();

		for (table, rows) in batches {
			self.process_table(table, version, rows);
		}

		Span::current().record("total_entry_count", total_entries);
	}

	#[inline]
	#[instrument(name = "store::commit::process_table", level = "trace", skip(self, rows), fields(
		table = ?table,
		entry_count = rows.len(),
	))]
	fn process_table(&self, table: EntryKind, version: CommitVersion, rows: Vec<(EncodedKey, Option<CowVec<u8>>)>) {
		let entry = self.get_or_create_table(table);
		let keys: Vec<EncodedKey> = rows.iter().map(|(key, _)| key.clone()).collect();
		let bytes = entry.rows.insert(version, rows);
		entry.pending.lock().extend(keys);
		if bytes >= self.0.config.close_threshold {
			entry.rows.close();
		}
	}
}

impl<R: Remove> CommitStore<R> {
	#[instrument(name = "store::commit::compact", level = "debug", skip(self, batches), fields(
		table_count = batches.len(),
		total_entry_count = field::Empty
	))]
	pub fn compact(&self, batches: HashMap<EntryKind, Vec<(EncodedKey, CommitVersion)>>) -> Vec<EvictedVersion> {
		let total_entries: usize = batches.values().map(|pairs| pairs.len()).sum();
		let mut removed: Vec<EvictedVersion> = Vec::with_capacity(total_entries);

		for (table, pairs) in batches {
			let Some(entry) = self.0.entries.get(&table) else {
				continue;
			};
			let (evicted, emptied) = entry.rows.remove(pairs);
			entry.unqueue(&emptied);
			removed.extend(evicted);
		}

		Span::current().record("total_entry_count", total_entries);
		removed
	}
}

impl<R: Read + Write> CommitStore<R> {
	#[instrument(name = "store::commit::collect_evictable_below", level = "debug", skip_all, fields(table = ?table, cutoff = cutoff.0))]
	pub fn collect_evictable_below(
		&self,
		table: EntryKind,
		cutoff: CommitVersion,
		budget: ByteSize,
		start: Option<&EncodedKey>,
	) -> (EvictablePersist, EvictableDrop, ByteSize, Option<EncodedKey>) {
		let Some(entry) = self.0.entries.get(&table) else {
			return (Vec::new(), Vec::new(), ByteSize::ZERO, None);
		};
		if entry.rows.stats().active_oldest.is_some_and(|oldest| oldest <= cutoff) {
			entry.rows.close();
		}

		let budget = budget.as_bytes();
		let mut consumed = 0u64;
		let mut to_persist: EvictablePersist = Vec::new();
		let mut to_drop: EvictableDrop = Vec::new();
		let mut next = None;
		let from = start.map_or(Bound::Unbounded, |key| Bound::Included(key.as_slice()));

		entry.rows.scan_closed(from, Bound::Unbounded, cutoff, |key, versions| {
			let Some(visible) = versions.iter().position(|(version, _)| *version <= cutoff) else {
				return ControlFlow::Continue(());
			};
			if !to_persist.is_empty() && consumed >= budget {
				next = Some(key.clone());
				return ControlFlow::Break(());
			}
			let (version, value) = versions[visible];
			to_persist.push((key.clone(), version, value.clone()));
			for (version, value) in &versions[visible..] {
				consumed += entry_bytes(key, value);
				to_drop.push((key.clone(), *version));
			}
			ControlFlow::Continue(())
		});

		(to_persist, to_drop, ByteSize::from_bytes(consumed), next)
	}
}

impl<R: Read + Remove> CommitStore<R> {
	#[instrument(name = "store::commit::gc", level = "trace", skip(self), fields(kind = ?kind, cutoff = cutoff.0, batch = batch))]
	pub fn gc(&self, kind: EntryKind, cutoff: CommitVersion, batch: usize) -> (Vec<EvictedVersion>, u64) {
		let Some(entry) = self.0.entries.get(&kind) else {
			return (Vec::new(), 0);
		};

		let retained_len = entry.retained.lock().len();
		let pending_room = batch - retained_len.min(batch - batch / 2);
		let mut pending = entry.pending.lock();
		let mut keys: Vec<EncodedKey> = iter::from_fn(|| pending.pop_first()).take(pending_room).collect();
		let remaining = pending.len() as u64;
		drop(pending);

		let retained_room = batch - keys.len();
		let cursor = entry.retained_cursor.lock().clone();
		let mut retained = entry.retained.lock();
		let taken: Vec<EncodedKey> = match &cursor {
			Some(cursor) => retained
				.range::<EncodedKey, _>((Bound::Excluded(cursor), Bound::Unbounded))
				.chain(retained.range::<EncodedKey, _>(..=cursor))
				.take(retained_room)
				.cloned()
				.collect(),
			None => retained.iter().take(retained_room).cloned().collect(),
		};
		for key in &taken {
			retained.remove(key);
		}
		drop(retained);
		if let Some(last) = taken.last() {
			*entry.retained_cursor.lock() = Some(last.clone());
		}
		keys.extend(taken);

		let mut pairs: Vec<(EncodedKey, CommitVersion)> = Vec::new();
		let mut retained: Vec<EncodedKey> = Vec::new();
		for key in keys {
			let versions = entry.rows.versions(&key);
			let listed = versions.partition_point(|version| *version <= cutoff).saturating_sub(1);
			let requeue = versions.len() - listed >= 2;

			reifydb_assertions! {
				let visible = versions.iter().rev().find(|version| **version <= cutoff);
				assert!(
					versions[..listed].iter().all(|version| visible.is_some_and(|visible| version < visible))
						&& versions[listed..].iter().all(|version| visible.is_none_or(|visible| version >= visible)),
					"gc must list exactly the versions below the one a reader at the cutoff sees; listing \
					 that version or a newer one loses a visible row (key={key:?}, cutoff={cutoff:?}, \
					 versions={versions:?}, listed={listed})"
				);
				let kept = versions.iter().filter(|version| visible.is_none_or(|visible| *version >= visible)).count();
				assert!(
					requeue == (kept >= 2),
					"gc must requeue a key exactly when it keeps two or more versions, or a kept visible \
					 version lingers until the key is written again (key={key:?}, kept={kept}, \
					 requeue={requeue})"
				);
			}

			pairs.extend(versions[..listed].iter().map(|version| (key.clone(), *version)));
			if requeue {
				retained.push(key);
			}
		}

		entry.retained.lock().extend(retained);

		if pairs.is_empty() {
			return (Vec::new(), remaining);
		}
		let (evicted, emptied) = entry.rows.remove(pairs);
		entry.unqueue(&emptied);
		(evicted, remaining)
	}
}

impl<R: Read> MetricsCollector for CommitStore<R> {
	fn collect(&self, out: &mut Vec<MetricsSample>) {
		let metrics = self.metrics();
		out.push(MetricsSample::heap("commit_buffer", "current_bytes", metrics.current_bytes));
		out.push(MetricsSample::heap("commit_buffer", "historical_bytes", metrics.historical_bytes));
		out.push(MetricsSample::count("commit_buffer", "table_count", metrics.table_count.as_u64()));
		out.push(MetricsSample::count("commit_buffer", "current_entries", metrics.current_entries.as_u64()));
		out.push(MetricsSample::heap("commit_buffer", "queued_bytes", metrics.queued_bytes));
	}
}
