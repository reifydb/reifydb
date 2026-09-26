// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Reverse,
	collections::{HashMap, HashSet, VecDeque},
	iter, mem,
	ops::{Bound, ControlFlow},
	sync::Arc,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, metrics::heap::HeapSize};
use reifydb_runtime::sync::{
	mutex::Mutex,
	rwlock::{RwLock, RwLockWriteGuard},
};
use reifydb_value::{byte_size::ByteSize, reifydb_assertions, util::cowvec::CowVec};
use tracing::instrument;

use crate::{
	entry::{Value, entry_bytes, entry_bytes_with, value_bytes_of},
	rows::{ActiveRows, ClosedRows, MergedRows, Removed, RowMap, VersionMap, lookup},
	storage::{Read, Remove, RowStats, Rows, Write},
	store::EvictedVersion,
};

#[derive(Default)]
pub struct MemoryRows {
	active: RwLock<ActiveRows>,
	closed: RwLock<VecDeque<Arc<ClosedRows>>>,
	counts: Mutex<Counts>,
}

#[derive(Default)]
struct Counts {
	keys: u64,
	current_bytes: u64,
	historical_bytes: u64,
}

impl MemoryRows {
	#[instrument(name = "store::commit::active_write", level = "debug", skip_all)]
	fn active_write(&self) -> RwLockWriteGuard<'_, ActiveRows> {
		self.active.write()
	}
}

impl Rows for MemoryRows {
	fn empty(&self) -> Self {
		Self::default()
	}
}

impl Read for MemoryRows {
	fn get(&self, key: &[u8], version: CommitVersion) -> Option<(CommitVersion, Option<CowVec<u8>>)> {
		let active = self.active.read();
		let closed = self.closed.read();
		lookup(iter::once(active.rows()).chain(closed.iter().rev().map(|map| map.rows())), key, version)
			.map(|(found, value)| (found, value.clone()))
	}

	fn scan(
		&self,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		reverse: bool,
		visit: impl FnMut(&EncodedKey, &[(CommitVersion, &Option<CowVec<u8>>)]) -> ControlFlow<()>,
	) {
		let active = self.active.read();
		let closed = self.closed.read();
		walk(
			iter::once(active.rows()).chain(closed.iter().rev().map(|map| map.rows())),
			start,
			end,
			reverse,
			visit,
		);
	}

	fn scan_closed(
		&self,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
		cutoff: CommitVersion,
		visit: impl FnMut(&EncodedKey, &[(CommitVersion, &Option<CowVec<u8>>)]) -> ControlFlow<()>,
	) {
		let closed: Vec<Arc<ClosedRows>> =
			self.closed.read().iter().rev().filter(|map| map.min_version() <= cutoff).cloned().collect();
		walk(closed.iter().map(|map| map.rows()), start, end, false, visit);
	}

	fn versions(&self, key: &[u8]) -> Vec<CommitVersion> {
		let active = self.active.read();
		let closed = self.closed.read();
		let mut versions: Vec<CommitVersion> = iter::once(active.rows())
			.chain(closed.iter().map(|map| map.rows()))
			.filter_map(|rows| rows.versions_for(key))
			.flat_map(|found| found.keys().map(|Reverse(version)| *version))
			.collect();
		versions.sort_unstable();
		versions.dedup();
		versions
	}

	fn stats(&self) -> RowStats {
		let active = self.active.read();
		let closed = self.closed.read();
		let counts = self.counts.lock();
		let maps = || iter::once(active.rows()).chain(closed.iter().map(|map| map.rows()));
		RowStats {
			current_bytes: ByteSize::from_bytes(counts.current_bytes),
			historical_bytes: ByteSize::from_bytes(counts.historical_bytes),
			keys: counts.keys,
			oldest: maps().filter_map(RowMap::min_version).min(),
			active_oldest: active.min_version(),
		}
	}
}

impl Write for MemoryRows {
	fn insert(&self, version: CommitVersion, rows: Vec<(EncodedKey, Option<CowVec<u8>>)>) -> ByteSize {
		let mut active = self.active_write();
		let closed = self.closed.read();
		let mut counts = self.counts.lock();
		for (key, value) in rows {
			let key_heap = key.heap_size();
			let bytes = entry_bytes_with(key_heap, &value);
			let newest = lookup(
				iter::once(active.rows()).chain(closed.iter().rev().map(|map| map.rows())),
				&key,
				CommitVersion(u64::MAX),
			)
			.map(|(found, held)| (found, entry_bytes_with(key_heap, held)));
			let replaced = active
				.rows()
				.versions_for(&key)
				.and_then(|found| found.get(&Reverse(version)))
				.map_or(0, |held| entry_bytes_with(key_heap, held));
			active.insert(key, version, value);
			match newest {
				None => {
					counts.keys = counts.keys.saturating_add(1);
					counts.current_bytes = counts.current_bytes.saturating_add(bytes);
				}
				Some((found, newest_bytes)) if version >= found => {
					counts.current_bytes =
						counts.current_bytes.saturating_add(bytes).saturating_sub(newest_bytes);
					counts.historical_bytes = counts
						.historical_bytes
						.saturating_add(newest_bytes)
						.saturating_sub(replaced);
				}
				Some(_) => {
					counts.historical_bytes =
						counts.historical_bytes.saturating_add(bytes).saturating_sub(replaced);
				}
			}
		}
		ByteSize::from_bytes(active.bytes())
	}

	fn close(&self) {
		let mut active = self.active_write();
		if active.is_empty() {
			return;
		}
		let closing = mem::take(&mut *active);
		self.closed.write().push_back(Arc::new(closing.close()));
	}
}

impl Remove for MemoryRows {
	fn remove(&self, pairs: Vec<(EncodedKey, CommitVersion)>) -> (Vec<EvictedVersion>, Vec<EncodedKey>) {
		let mut dropped: HashMap<EncodedKey, HashSet<CommitVersion>> = HashMap::new();
		let mut below = CommitVersion(0);
		for (key, version) in pairs {
			below = below.max(version);
			dropped.entry(key).or_default().insert(version);
		}

		let mut active = self.active_write();
		let mut closed = self.closed.write();
		let newest: HashMap<&EncodedKey, (CommitVersion, u64)> = dropped
			.keys()
			.filter_map(|key| {
				lookup(
					iter::once(active.rows()).chain(closed.iter().rev().map(|map| map.rows())),
					key,
					CommitVersion(u64::MAX),
				)
				.map(|(version, value)| (key, (version, entry_bytes(key, value))))
			})
			.collect();
		let record = |removed: &Removed| EvictedVersion {
			key: removed.key.clone(),
			version: removed.version,
			value_bytes: value_bytes_of(&removed.value),
			current: newest.get(&removed.key).map(|(version, _)| version) == Some(&removed.version),
		};

		let mut removed = Vec::new();
		if active.min_version().is_some_and(|min| min <= below) {
			removed.extend(active.compact(&dropped));
		}
		for map in closed.iter_mut() {
			if map.min_version() > below {
				continue;
			}
			removed.extend(Arc::make_mut(map).compact(&dropped));
		}
		closed.retain(|map| !map.rows().is_empty());
		let evicted: Vec<EvictedVersion> = removed.iter().map(&record).collect();

		let mut emptied = Vec::new();
		let mut counts = self.counts.lock();
		for key in dropped.keys() {
			let before = newest.get(key).map(|(_, bytes)| *bytes);
			let after = lookup(
				iter::once(active.rows()).chain(closed.iter().rev().map(|map| map.rows())),
				key,
				CommitVersion(u64::MAX),
			)
			.map(|(_, value)| entry_bytes(key, value));
			if after.is_none() {
				emptied.push(key.clone());
			}
			if before.is_some() && after.is_none() {
				counts.keys = counts.keys.saturating_sub(1);
			}
			let (before, after) = (before.unwrap_or(0), after.unwrap_or(0));
			counts.current_bytes = counts.current_bytes.saturating_add(after).saturating_sub(before);
			counts.historical_bytes = counts.historical_bytes.saturating_add(before).saturating_sub(after);
		}
		counts.historical_bytes = counts
			.historical_bytes
			.saturating_sub(removed.iter().map(|removed| entry_bytes(&removed.key, &removed.value)).sum());

		reifydb_assertions! {
			for removed in &evicted {
				assert!(
					dropped.get(&removed.key).is_some_and(|versions| versions.contains(&removed.version)),
					"remove dropped a pair it was not given, so a version some reader still sees is gone \
					 (key={:?}, version={:?})",
					removed.key,
					removed.version
				);
			}
			let reported: HashSet<&EncodedKey> = emptied.iter().collect();
			for key in dropped.keys() {
				let held = iter::once(active.rows())
					.chain(closed.iter().map(|map| map.rows()))
					.any(|rows| rows.versions_for(key).is_some());
				assert!(
					reported.contains(key) != held,
					"remove must report a key as emptied exactly when no map holds it, or the gc queues keep \
					 a gone key or drop a held one (key={key:?}, held={held})"
				);
			}
		}

		(evicted, emptied)
	}
}

fn walk<'a>(
	maps: impl Iterator<Item = &'a RowMap>,
	start: Bound<&[u8]>,
	end: Bound<&[u8]>,
	reverse: bool,
	visit: impl FnMut(&EncodedKey, &[(CommitVersion, &Value)]) -> ControlFlow<()>,
) {
	let ranges = maps.map(|rows| rows.range((start, end)));
	if reverse {
		visit_merged(MergedRows::new(ranges.map(Iterator::rev).collect(), true), visit);
	} else {
		visit_merged(MergedRows::new(ranges.collect(), false), visit);
	}
}

fn visit_merged<'a, I>(
	mut merged: MergedRows<'a, I>,
	mut visit: impl FnMut(&EncodedKey, &[(CommitVersion, &Value)]) -> ControlFlow<()>,
) where
	I: Iterator<Item = (&'a EncodedKey, &'a VersionMap)>,
{
	let mut versions: Vec<(CommitVersion, &'a Value)> = Vec::new();
	while let Some((key, group)) = merged.next_group() {
		versions.clear();
		versions.extend(group
			.iter()
			.copied()
			.flat_map(|found| found.iter().map(|(Reverse(version), value)| (*version, value))));
		versions.sort_by_key(|(version, _)| Reverse(*version));
		if visit(key, &versions).is_break() {
			return;
		}
	}
}
