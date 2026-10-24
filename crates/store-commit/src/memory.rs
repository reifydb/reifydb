// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::Reverse,
	collections::{BTreeMap, HashMap, HashSet, VecDeque},
	iter, mem,
	ops::{Bound, ControlFlow, Range},
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
	rows::{ActiveRows, ClosedRows, MergedRows, Removed, RowMap, Versions, lookup, tier},
	storage::{Read, Remove, RowStats, RowTallies, Rows, Write},
	store::EvictedVersion,
};

#[derive(Default)]
pub struct MemoryRows {
	active: RwLock<ActiveRows>,
	closed: RwLock<VecDeque<Arc<ClosedRows>>>,
	counts: Mutex<Counts>,
	merging: Mutex<()>,
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

	fn merge_run(&self, fan_in: usize, cap: ByteSize, floor: ByteSize) -> bool {
		let snapshot: Vec<Arc<ClosedRows>> = self.closed.read().iter().cloned().collect();
		let Some(run) = mergeable_run(&snapshot, fan_in, cap.as_bytes(), floor.as_bytes()) else {
			return false;
		};
		let inputs = &snapshot[run];
		let merged = Arc::new(ClosedRows::merged(inputs));
		let mut closed = self.closed.write();
		let start = closed
			.iter()
			.position(|map| Arc::ptr_eq(map, &inputs[0]))
			.expect("only remove drops closed maps, and it waits on the merge lock");
		reifydb_assertions! {
			assert!(
				inputs.iter().enumerate().all(|(offset, input)| closed
					.get(start + offset)
					.is_some_and(|held| Arc::ptr_eq(held, input))),
				"the merge inputs must still be neighbours in the closed maps when the merged map replaces them"
			);
		}
		closed.drain(start..start + inputs.len());
		closed.insert(start, merged);
		true
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
			.flat_map(|found| found.iter().map(|(version, _)| version))
			.collect();
		versions.sort_unstable();
		versions.dedup();
		versions
	}

	fn stats(&self) -> RowStats {
		let active = self.active.read();
		let closed = self.closed.read();
		let maps = || iter::once(active.rows()).chain(closed.iter().map(|map| map.rows()));
		RowStats {
			oldest: maps().filter_map(RowMap::min_version).min(),
			active_oldest: active.min_version(),
			closed_maps: closed.len() as u64,
		}
	}

	fn tallies(&self) -> RowTallies {
		let counts = self.counts.lock();
		RowTallies {
			current_bytes: ByteSize::from_bytes(counts.current_bytes),
			historical_bytes: ByteSize::from_bytes(counts.historical_bytes),
			keys: counts.keys,
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
				.and_then(|found| found.exact(version))
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

	fn merge(&self, fan_in: usize, cap: ByteSize, floor: ByteSize, backlog: usize) {
		if backlog == 0 {
			let _merging = self.merging.lock();
			while self.merge_run(fan_in, cap, floor) {}
			return;
		}
		let cap_tier = tier(cap.as_bytes(), fan_in);
		let mergeable =
			self.closed.read().iter().filter(|map| tier(map.rows().bytes(), fan_in) < cap_tier).count();
		if mergeable <= backlog {
			return;
		}
		let Some(_merging) = self.merging.try_lock() else {
			return;
		};
		self.merge_run(fan_in, cap, floor);
	}
}

const REMOVE_CHUNK: usize = 4096;

fn chunks_by_key(pairs: Vec<(EncodedKey, CommitVersion)>, limit: usize) -> Vec<Vec<(EncodedKey, CommitVersion)>> {
	let mut by_key: BTreeMap<EncodedKey, Vec<CommitVersion>> = BTreeMap::new();
	for (key, version) in pairs {
		by_key.entry(key).or_default().push(version);
	}
	let mut chunks = Vec::new();
	let mut chunk = Vec::new();
	for (key, versions) in by_key {
		if !chunk.is_empty() && chunk.len() + versions.len() > limit {
			chunks.push(mem::take(&mut chunk));
		}
		chunk.extend(versions.into_iter().map(|version| (key.clone(), version)));
	}
	if !chunk.is_empty() {
		chunks.push(chunk);
	}
	chunks
}

fn mergeable_run(maps: &[Arc<ClosedRows>], fan_in: usize, cap: u64, floor: u64) -> Option<Range<usize>> {
	let cap_tier = tier(cap, fan_in);
	let floor_tier = tier(floor, fan_in);
	let tiers: Vec<u32> = maps.iter().map(|map| tier(map.rows().bytes(), fan_in)).collect();
	let bytes = |run: &Range<usize>| -> u64 { maps[run.clone()].iter().map(|map| map.rows().bytes()).sum() };
	let runs = || (fan_in..=maps.len()).map(|end| end - fan_in..end);
	runs().find(|run| {
		tiers[run.clone()].iter().all(|tier| *tier == tiers[run.start]) && tier(bytes(run), fan_in) <= cap_tier
	})
	.or_else(|| {
		runs().filter(|run| {
			let window = &tiers[run.clone()];
			window.iter().all(|tier| *tier <= floor_tier)
				&& window.iter().any(|tier| *tier < floor_tier)
				&& tier(bytes(run), fan_in) <= cap_tier
		})
		.min_by_key(|run| bytes(run))
	})
}

impl Remove for MemoryRows {
	fn remove(&self, pairs: Vec<(EncodedKey, CommitVersion)>) -> (Vec<EvictedVersion>, Vec<EncodedKey>) {
		let mut evicted = Vec::new();
		let mut emptied = Vec::new();
		for chunk in chunks_by_key(pairs, REMOVE_CHUNK) {
			let (chunk_evicted, chunk_emptied) = self.remove_chunk(chunk);
			evicted.extend(chunk_evicted);
			emptied.extend(chunk_emptied);
		}
		(evicted, emptied)
	}
}

impl MemoryRows {
	fn remove_chunk(&self, pairs: Vec<(EncodedKey, CommitVersion)>) -> (Vec<EvictedVersion>, Vec<EncodedKey>) {
		let mut dropped: HashMap<EncodedKey, HashSet<CommitVersion>> = HashMap::new();
		let mut below = CommitVersion(0);
		for (key, version) in pairs {
			below = below.max(version);
			dropped.entry(key).or_default().insert(version);
		}

		let _merging = self.merging.lock();
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
	I: Iterator<Item = (&'a EncodedKey, &'a Versions)>,
{
	let mut versions: Vec<(CommitVersion, &'a Value)> = Vec::new();
	while let Some((key, group)) = merged.next_group() {
		versions.clear();
		versions.extend(group.iter().copied().flat_map(Versions::iter));
		versions.sort_by_key(|(version, _)| Reverse(*version));
		if visit(key, &versions).is_break() {
			return;
		}
	}
}

#[cfg(test)]
mod tests {
	use std::{
		sync::{Arc, mpsc},
		thread,
		time::Duration,
	};

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::common::CommitVersion;
	use reifydb_value::{byte_size::ByteSize, util::cowvec::CowVec};

	use super::{MemoryRows, chunks_by_key};
	use crate::storage::{Read, Remove, Write};

	fn key(index: u64) -> EncodedKey {
		EncodedKey::new(index.to_be_bytes().to_vec())
	}

	fn value(version: u64, len: usize) -> Option<CowVec<u8>> {
		let mut bytes = version.to_be_bytes().to_vec();
		bytes.resize(len.max(8), 0xAB);
		Some(CowVec::new(bytes))
	}

	fn closed_maps_of_one_tier(rows: &MemoryRows, maps: u64) {
		for version in 1..=maps {
			rows.insert(CommitVersion(version), vec![(key(version), value(version, 40))]);
			rows.close();
		}
	}

	#[test]
	fn a_commit_path_merge_never_waits_for_a_merge_already_running() {
		// a commit that waits for the merge task or a flush merge holds the oracle lock for that whole merge.
		let rows = Arc::new(MemoryRows::default());
		closed_maps_of_one_tier(&rows, 20);
		let held = rows.merging.lock();
		let (done_tx, done_rx) = mpsc::channel();
		let committer = {
			let rows = Arc::clone(&rows);
			thread::spawn(move || {
				rows.merge(4, ByteSize::from_mib(256), ByteSize::ZERO, 16);
				done_tx.send(()).unwrap();
			})
		};

		let returned = done_rx.recv_timeout(Duration::from_millis(500)).is_ok();
		drop(held);
		committer.join().unwrap();

		assert!(
			returned,
			"a commit-path merge must leave a running merge to finish the work instead of waiting for it"
		);
	}

	#[test]
	fn a_commit_path_merge_folds_one_run_at_most() {
		// folding every run inside one commit trades many small stalls for one long oracle hold.
		let rows = MemoryRows::default();
		closed_maps_of_one_tier(&rows, 20);

		rows.merge(4, ByteSize::from_mib(256), ByteSize::ZERO, 16);

		assert_eq!(
			rows.stats().closed_maps,
			17,
			"one run of four maps folds into one, the rest waits for the task"
		);
	}

	fn closed_maps_alternating(rows: &MemoryRows, maps: u64, small: usize, large: usize) {
		for version in 1..=maps {
			let len = if version % 2 == 0 {
				large
			} else {
				small
			};
			rows.insert(CommitVersion(version), vec![(key(version), value(version, len))]);
			rows.close();
		}
	}

	#[test]
	fn small_closed_maps_of_mixed_tiers_merge_down_below_the_close_size() {
		// flush leftovers of alternating sizes never form four equal tiers, so without a fallback they pile up
		// forever.
		let rows = MemoryRows::default();
		closed_maps_alternating(&rows, 20, 40, 3000);

		rows.merge(4, ByteSize::from_mib(256), ByteSize::from_kib(64), 0);

		let closed = rows.stats().closed_maps;
		assert!(
			closed < 4,
			"small maps must keep merging until fewer than a fan-in of them remain, got {closed}"
		);
	}

	#[test]
	fn closed_maps_above_the_close_size_never_join_a_small_map_merge() {
		// pulling full-size maps into the fallback recopies them on every merge and inflates merge work on the
		// memory path.
		let rows = MemoryRows::default();
		closed_maps_alternating(&rows, 20, 8 * 1024, 64 * 1024);

		rows.merge(4, ByteSize::from_mib(256), ByteSize::from_kib(4), 0);

		assert_eq!(rows.stats().closed_maps, 20, "maps above the close size merge only by the equal-tier rule");
	}

	#[test]
	fn a_remove_chunk_never_splits_a_key_and_keeps_every_pair() {
		// a key split across chunks would compute its newest version from a half-removed state and skew the
		// tallies.
		let pairs: Vec<(EncodedKey, CommitVersion)> = (0..300u64)
			.flat_map(|index| {
				(1..=(index % 7 + 1)).map(move |version| (key(index), CommitVersion(version)))
			})
			.collect();

		let chunks = chunks_by_key(pairs.clone(), 64);

		let mut seen: Vec<(EncodedKey, CommitVersion)> = chunks.iter().flatten().cloned().collect();
		let mut expected = pairs;
		seen.sort();
		expected.sort();
		assert_eq!(seen, expected, "chunking must keep every pair exactly once");
		for (index, chunk) in chunks.iter().enumerate() {
			assert!(chunk.len() <= 64, "chunk {index} holds {} pairs, over the limit", chunk.len());
			for later in &chunks[index + 1..] {
				assert!(
					later.iter().all(|(key, _)| chunk.iter().all(|(held, _)| held != key)),
					"a key must sit in exactly one chunk"
				);
			}
		}
	}

	#[test]
	fn a_chunked_remove_leaves_the_same_rows_and_tallies_as_one_pass() {
		// chunking only bounds lock holds, so any difference from a single pass is a lost or double-counted
		// removal.
		let chunked = MemoryRows::default();
		let single = MemoryRows::default();
		for rows in [&chunked, &single] {
			for version in 1..=2u64 {
				let batch = (0..10_000u64).map(|index| (key(index), value(version, 24))).collect();
				rows.insert(CommitVersion(version), batch);
				rows.close();
			}
		}
		let pairs: Vec<(EncodedKey, CommitVersion)> =
			(0..10_000u64).map(|index| (key(index), CommitVersion(1))).collect();

		let (mut chunked_evicted, chunked_emptied) = chunked.remove(pairs.clone());
		let (mut single_evicted, single_emptied) = single.remove_chunk(pairs);

		chunked_evicted.sort_by(|left, right| left.key.cmp(&right.key));
		single_evicted.sort_by(|left, right| left.key.cmp(&right.key));
		assert_eq!(chunked_evicted.len(), single_evicted.len());
		assert!(chunked_evicted.iter().zip(&single_evicted).all(|(left, right)| left.key == right.key
			&& left.version == right.version
			&& left.current == right.current));
		assert_eq!(chunked_emptied, single_emptied);
		assert_eq!(
			chunked.tallies(),
			single.tallies(),
			"chunked removal must leave the same byte and key tallies"
		);
		assert_eq!(chunked.get(&key(42), CommitVersion(9)), single.get(&key(42), CommitVersion(9)));
	}
}
