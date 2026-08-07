// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

#[cfg(reifydb_assertions)]
use std::sync::atomic::AtomicBool;
use std::{
	collections::{BTreeMap, btree_map::Entry},
	iter::once,
	mem::{size_of, take},
	ops::Bound,
	sync::atomic::{AtomicU64, Ordering},
};

use reifydb_codec::{
	encoded::row::EncodedRow,
	key::encoded::{EncodedKey, EncodedKeyRange},
};
use reifydb_core::common::CommitVersion;
use reifydb_runtime::sync::rwlock::RwLock;
use reifydb_value::reifydb_assertions;

use crate::{
	config::OperatorStoreConfig,
	floor::{FloorSpec, floor_expired},
};

enum PointEntry {
	Row(EncodedRow),
	Tombstone,
}

const NODE_FILL_DIVISOR: usize = 2;

const ENTRY_OVERHEAD: usize = NODE_FILL_DIVISOR * (size_of::<EncodedKey>() + size_of::<PointEntry>());

fn point_bytes(key: &EncodedKey, entry: &PointEntry) -> u64 {
	let value_len = match entry {
		PointEntry::Row(row) => row.as_slice().len(),
		PointEntry::Tombstone => 0,
	};
	(ENTRY_OVERHEAD + key.heap_bytes() + value_len) as u64
}

fn bound_heap_bytes(bound: &Bound<EncodedKey>) -> usize {
	match bound {
		Bound::Included(key) | Bound::Excluded(key) => key.heap_bytes(),
		Bound::Unbounded => 0,
	}
}

fn range_entry_bytes(range: &EncodedKeyRange) -> u64 {
	(ENTRY_OVERHEAD + bound_heap_bytes(&range.start) + bound_heap_bytes(&range.end)) as u64
}

fn range_contains(range: &EncodedKeyRange, key: &EncodedKey) -> bool {
	let after_start = match &range.start {
		Bound::Included(start) => key >= start,
		Bound::Excluded(start) => key > start,
		Bound::Unbounded => true,
	};
	let before_end = match &range.end {
		Bound::Included(end) => key <= end,
		Bound::Excluded(end) => key < end,
		Bound::Unbounded => true,
	};
	after_start && before_end
}

fn range_is_empty(range: &EncodedKeyRange) -> bool {
	match (&range.start, &range.end) {
		(Bound::Included(start), Bound::Included(end)) => start > end,
		(Bound::Included(start), Bound::Excluded(end)) => start >= end,
		(Bound::Excluded(start), Bound::Included(end)) => start >= end,
		(Bound::Excluded(start), Bound::Excluded(end)) => start >= end,
		_ => false,
	}
}

#[derive(Default)]
struct Batch {
	entries: BTreeMap<EncodedKey, PointEntry>,
	ranges: Vec<EncodedKeyRange>,
	bytes: u64,
}

impl Batch {
	fn is_empty(&self) -> bool {
		self.entries.is_empty() && self.ranges.is_empty()
	}

	fn put(&mut self, key: EncodedKey, entry: PointEntry) {
		match self.entries.entry(key) {
			Entry::Occupied(mut slot) => {
				self.bytes -= point_bytes(slot.key(), slot.get());
				self.bytes += point_bytes(slot.key(), &entry);
				slot.insert(entry);
			}
			Entry::Vacant(slot) => {
				self.bytes += point_bytes(slot.key(), &entry);
				slot.insert(entry);
			}
		}
	}

	fn delete_point(&mut self, key: &EncodedKey) {
		if let Some((key, entry)) = self.entries.remove_entry(key) {
			self.bytes -= point_bytes(&key, &entry);
		}
	}

	fn delete_covered(&mut self, range: &EncodedKeyRange) {
		let covered: Vec<EncodedKey> = self
			.entries
			.range::<EncodedKey, _>((range.start.as_ref(), range.end.as_ref()))
			.map(|(key, _)| key.clone())
			.collect();
		for key in covered {
			self.delete_point(&key);
		}
	}

	fn push_range(&mut self, range: EncodedKeyRange) {
		self.delete_covered(&range);
		self.bytes += range_entry_bytes(&range);
		self.ranges.push(range);
	}

	fn covers(&self, key: &EncodedKey) -> bool {
		self.ranges.iter().any(|range| range_contains(range, key))
	}
}

#[derive(Default)]
pub(crate) struct ArenaInner {
	active: Batch,
	frozen: Vec<Batch>,
}

impl ArenaInner {
	fn total_bytes(&self) -> u64 {
		self.active.bytes + self.frozen.iter().map(|batch| batch.bytes).sum::<u64>()
	}

	pub(crate) fn set(&mut self, key: EncodedKey, value: EncodedRow, config: &OperatorStoreConfig) {
		self.active.put(key, PointEntry::Row(value));
		self.roll(config);
	}

	pub(crate) fn remove(&mut self, key: &EncodedKey, config: &OperatorStoreConfig) {
		if self.frozen.is_empty() {
			self.active.delete_point(key);
		} else {
			self.active.put(key.clone(), PointEntry::Tombstone);
			self.roll(config);
		}
	}

	pub(crate) fn remove_range(&mut self, range: EncodedKeyRange, config: &OperatorStoreConfig) {
		if range_is_empty(&range) {
			return;
		}
		if self.frozen.is_empty() {
			self.active.delete_covered(&range);
		} else {
			self.active.push_range(range);
			self.roll(config);
		}
	}

	pub(crate) fn clear(&mut self) {
		self.active = Batch::default();
		self.frozen.clear();
	}

	pub(crate) fn freeze(&mut self) {
		if self.active.is_empty() {
			return;
		}
		self.frozen.push(take(&mut self.active));
	}

	pub(crate) fn compact(&mut self, floor: &FloorSpec) -> u64 {
		self.freeze();
		if self.frozen.is_empty() {
			return 0;
		}
		self.merge_from(0, floor)
	}

	fn roll(&mut self, config: &OperatorStoreConfig) {
		if self.active.bytes >= config.freeze_bytes {
			self.freeze();
		}
		let cap = config.max_frozen.max(1);
		while self.frozen.len() > cap {
			let start = self.pick_merge_start();
			self.merge_from(start, &FloorSpec::default());
		}
	}

	fn pick_merge_start(&self) -> usize {
		let newest = self.frozen.len() - 1;
		let mut start = newest - 1;
		let mut merged = self.frozen[newest].bytes + self.frozen[start].bytes;
		while start > 0 && self.frozen[start - 1].bytes <= merged.saturating_mul(2) {
			start -= 1;
			merged += self.frozen[start].bytes;
		}
		start
	}

	fn merge_from(&mut self, start: usize, floor: &FloorSpec) -> u64 {
		let inputs = self.frozen.split_off(start);
		let merging_oldest = self.frozen.is_empty();
		let (merged, dropped) = merge(inputs, merging_oldest, floor);
		if !merged.is_empty() {
			self.frozen.push(merged);
		}
		dropped
	}

	fn batches_newest_first(&self) -> impl Iterator<Item = &Batch> {
		once(&self.active).chain(self.frozen.iter().rev())
	}

	pub(crate) fn get(&self, key: &EncodedKey) -> Option<EncodedRow> {
		for batch in self.batches_newest_first() {
			if let Some(entry) = batch.entries.get(key) {
				return match entry {
					PointEntry::Row(row) => Some(row.clone()),
					PointEntry::Tombstone => None,
				};
			}
			if batch.covers(key) {
				return None;
			}
		}
		None
	}

	pub(crate) fn contains(&self, key: &EncodedKey) -> bool {
		for batch in self.batches_newest_first() {
			if let Some(entry) = batch.entries.get(key) {
				return matches!(entry, PointEntry::Row(_));
			}
			if batch.covers(key) {
				return false;
			}
		}
		false
	}

	pub(crate) fn scan(&self, range: &EncodedKeyRange, limit: usize) -> (Vec<(EncodedKey, EncodedRow)>, bool) {
		let mut items = Vec::new();
		if range_is_empty(range) {
			return (items, false);
		}
		let stack: Vec<&Batch> = self.batches_newest_first().collect();
		let mut cursors: Vec<_> = stack
			.iter()
			.map(|batch| {
				batch.entries
					.range::<EncodedKey, _>((range.start.as_ref(), range.end.as_ref()))
					.peekable()
			})
			.collect();
		loop {
			let mut min_key: Option<&EncodedKey> = None;
			for cursor in cursors.iter_mut() {
				if let Some(key) = cursor.peek().map(|(key, _)| *key)
					&& min_key.is_none_or(|current| key < current)
				{
					min_key = Some(key);
				}
			}
			let Some(key) = min_key else {
				break;
			};
			let mut winner: Option<(usize, &PointEntry)> = None;
			for (index, cursor) in cursors.iter_mut().enumerate() {
				if cursor.peek().is_some_and(|(peeked, _)| *peeked == key) {
					let (_, entry) = cursor.next().unwrap();
					if winner.is_none() {
						winner = Some((index, entry));
					}
				}
			}
			let (winner_index, entry) = winner.unwrap();
			if stack[..winner_index].iter().any(|batch| batch.covers(key)) {
				continue;
			}
			if let PointEntry::Row(row) = entry {
				if items.len() == limit {
					return (items, true);
				}
				items.push((key.clone(), row.clone()));
			}
		}
		(items, false)
	}
}

fn merge(inputs: Vec<Batch>, merging_oldest: bool, floor: &FloorSpec) -> (Batch, u64) {
	let mut ranges_by_batch = Vec::with_capacity(inputs.len());
	let mut cursors = Vec::with_capacity(inputs.len());
	for batch in inputs {
		ranges_by_batch.push(batch.ranges);
		cursors.push(batch.entries.into_iter().peekable());
	}
	let mut merged = Batch::default();
	let mut dropped = 0u64;
	loop {
		let mut min_key: Option<EncodedKey> = None;
		for cursor in cursors.iter_mut() {
			if let Some((key, _)) = cursor.peek()
				&& min_key.as_ref().is_none_or(|current| key < current)
			{
				min_key = Some(key.clone());
			}
		}
		let Some(key) = min_key else {
			break;
		};
		let mut winner: Option<(usize, PointEntry)> = None;
		for (index, cursor) in cursors.iter_mut().enumerate() {
			if cursor.peek().is_some_and(|(peeked, _)| *peeked == key) {
				let (_, entry) = cursor.next().unwrap();
				winner = Some((index, entry));
			}
		}
		let (winner_index, entry) = winner.unwrap();
		let masked =
			ranges_by_batch[winner_index + 1..].iter().flatten().any(|range| range_contains(range, &key));
		if masked {
			continue;
		}
		match entry {
			PointEntry::Tombstone => {
				if !merging_oldest {
					merged.put(key, PointEntry::Tombstone);
				}
			}
			PointEntry::Row(row) => {
				if floor_expired(floor, &key, &row) {
					dropped += 1;
				} else {
					merged.put(key, PointEntry::Row(row));
				}
			}
		}
	}
	if !merging_oldest {
		for ranges in ranges_by_batch {
			for range in ranges {
				merged.bytes += range_entry_bytes(&range);
				merged.ranges.push(range);
			}
		}
	}
	(merged, dropped)
}

pub(crate) struct Arena {
	inner: RwLock<ArenaInner>,
	bytes: AtomicU64,
	upper: AtomicU64,
	#[cfg(reifydb_assertions)]
	writing: AtomicBool,
}

impl Arena {
	pub(crate) fn new() -> Self {
		Self {
			inner: RwLock::new(ArenaInner::default()),
			bytes: AtomicU64::new(0),
			upper: AtomicU64::new(0),
			#[cfg(reifydb_assertions)]
			writing: AtomicBool::new(false),
		}
	}

	pub(crate) fn mutate<R>(&self, total: &AtomicU64, f: impl FnOnce(&mut ArenaInner) -> R) -> R {
		reifydb_assertions! {
			assert!(
				!self.writing.swap(true, Ordering::Acquire),
				"operator arenas are single-writer: a second thread entered a mutating operation while another write was in flight"
			);
		}
		let mut inner = self.inner.write();
		let result = f(&mut inner);
		let new_bytes = inner.total_bytes();
		drop(inner);
		let old_bytes = self.bytes.swap(new_bytes, Ordering::Relaxed);
		adjust(total, old_bytes, new_bytes);
		reifydb_assertions! {
			self.writing.store(false, Ordering::Release);
		}
		result
	}

	pub(crate) fn read<R>(&self, f: impl FnOnce(&ArenaInner) -> R) -> R {
		let inner = self.inner.read();
		f(&inner)
	}

	pub(crate) fn bytes(&self) -> u64 {
		self.bytes.load(Ordering::Relaxed)
	}

	pub(crate) fn set_upper(&self, version: CommitVersion) {
		self.upper.store(version.0, Ordering::Relaxed);
	}

	pub(crate) fn upper(&self) -> CommitVersion {
		CommitVersion(self.upper.load(Ordering::Relaxed))
	}
}

fn adjust(counter: &AtomicU64, old: u64, new: u64) {
	if new >= old {
		counter.fetch_add(new - old, Ordering::Relaxed);
	} else {
		saturating_sub(counter, old - new);
	}
}

pub(crate) fn saturating_sub(counter: &AtomicU64, amount: u64) {
	let mut observed = counter.load(Ordering::Relaxed);
	loop {
		let next = observed.saturating_sub(amount);
		match counter.compare_exchange_weak(observed, next, Ordering::Relaxed, Ordering::Relaxed) {
			Ok(_) => return,
			Err(actual) => observed = actual,
		}
	}
}
