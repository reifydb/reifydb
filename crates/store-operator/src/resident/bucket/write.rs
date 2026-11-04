// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	collections::{BTreeMap, btree_map::Entry},
	iter::Peekable,
	mem::{self, size_of},
	ops::{Bound, RangeBounds},
	sync::atomic::{AtomicBool, Ordering},
};

use reifydb_codec::row::pod::EncodedPodRow;
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			keyspace::columns_width,
			state::{GroupId, GroupStateKey, OperatorStateKey},
			traits::Keyspace,
		},
		typed::layout::KeyLayout,
	},
	state::typed::SuffixBytes,
};
use reifydb_value::{byte_size::ByteSize, reifydb_assertions};

use crate::{
	resident::bucket::{Bucket, FrozenRows, GroupIds},
	types::Scan,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Staged {
	Never,
	Flushing,
	Dirty,
	Clean,
}

impl Staged {
	pub fn is_dirty(self) -> bool {
		matches!(self, Self::Never | Self::Dirty)
	}

	pub fn is_collapsible(self) -> bool {
		matches!(self, Self::Never)
	}
}

#[derive(Debug, Clone, Copy)]
struct Generations {
	settled: u32,
	frozen: u32,
}

impl Generations {
	fn staged(self, entry: &WriteEntry) -> Staged {
		if entry.generation <= self.settled {
			Staged::Clean
		} else if entry.generation <= self.frozen {
			Staged::Flushing
		} else if entry.never {
			Staged::Never
		} else {
			Staged::Dirty
		}
	}
}

#[derive(Debug)]
pub struct WriteEntry {
	pub post: Option<EncodedPodRow>,
	generation: u32,
	never: bool,
	referenced: AtomicBool,
}

impl Clone for WriteEntry {
	fn clone(&self) -> Self {
		Self {
			post: self.post.clone(),
			generation: self.generation,
			never: self.never,
			referenced: AtomicBool::new(self.referenced.load(Ordering::Relaxed)),
		}
	}
}

impl PartialEq for WriteEntry {
	fn eq(&self, other: &Self) -> bool {
		self.post == other.post && self.generation == other.generation && self.never == other.never
	}
}

impl Eq for WriteEntry {}

impl WriteEntry {
	fn new(post: Option<EncodedPodRow>, generation: u32, never: bool) -> Self {
		Self {
			post,
			generation,
			never,
			referenced: AtomicBool::new(true),
		}
	}

	pub fn touch(&self) {
		self.referenced.store(true, Ordering::Relaxed);
	}

	fn take_reference(&self) -> bool {
		self.referenced.swap(false, Ordering::Relaxed)
	}

	fn row_bytes(&self) -> ByteSize {
		Self::bytes_of(&self.post)
	}

	fn bytes_of(post: &Option<EncodedPodRow>) -> ByteSize {
		post.as_ref().map_or(ByteSize::ZERO, |row| ByteSize::from_bytes(row.len() as u64))
	}
}

struct Partition<K: Keyspace> {
	live: BTreeMap<K::Suffix, WriteEntry>,
	deleted: BTreeMap<K::Suffix, WriteEntry>,
}

impl<K: Keyspace> Partition<K> {
	fn new() -> Self {
		Self {
			live: BTreeMap::new(),
			deleted: BTreeMap::new(),
		}
	}

	fn is_empty(&self) -> bool {
		self.live.is_empty() && self.deleted.is_empty()
	}

	fn get(&self, suffix: &K::Suffix) -> Option<&WriteEntry> {
		match self.live.get(suffix) {
			Some(entry) => Some(entry),
			None => self.deleted.get(suffix),
		}
	}

	fn merged(&self) -> Merged<impl Iterator<Item = (&K::Suffix, &WriteEntry)>> {
		Merged {
			live: self.live.iter().peekable(),
			deleted: self.deleted.iter().peekable(),
			scan: Scan::Forward,
		}
	}

	fn merged_range(
		&self,
		bounds: (Bound<K::Suffix>, Bound<K::Suffix>),
		scan: Scan,
		limit: usize,
	) -> Vec<(&K::Suffix, &WriteEntry)> {
		match scan {
			Scan::Forward => take_rows(
				Merged {
					live: self.live.range(bounds.clone()).peekable(),
					deleted: self.deleted.range(bounds).peekable(),
					scan,
				},
				limit,
			),
			Scan::Backward => {
				let mut out = take_rows(
					Merged {
						live: self.live.range(bounds.clone()).rev().peekable(),
						deleted: self.deleted.range(bounds).rev().peekable(),
						scan,
					},
					limit,
				);
				out.reverse();
				out
			}
		}
	}
}

fn take_rows<'a, S: 'a>(
	entries: impl Iterator<Item = (&'a S, &'a WriteEntry)>,
	limit: usize,
) -> Vec<(&'a S, &'a WriteEntry)> {
	let mut out = Vec::new();
	let mut rows = 0usize;
	for (suffix, entry) in entries {
		if entry.post.is_some() {
			rows += 1;
		}
		out.push((suffix, entry));
		if rows == limit {
			break;
		}
	}
	out
}

struct Merged<I: Iterator> {
	live: Peekable<I>,
	deleted: Peekable<I>,
	scan: Scan,
}

impl<'a, S: Ord + 'a, I: Iterator<Item = (&'a S, &'a WriteEntry)>> Iterator for Merged<I> {
	type Item = (&'a S, &'a WriteEntry);

	fn next(&mut self) -> Option<Self::Item> {
		match (self.live.peek(), self.deleted.peek()) {
			(None, None) => None,
			(Some(_), None) => self.live.next(),
			(None, Some(_)) => self.deleted.next(),
			(Some((live, _)), Some((deleted, _))) => {
				let ahead = match self.scan {
					Scan::Forward => live <= deleted,
					Scan::Backward => live >= deleted,
				};
				match ahead {
					true => self.live.next(),
					false => self.deleted.next(),
				}
			}
		}
	}
}

type PendingRows<K> = BTreeMap<(GroupId, <K as Keyspace>::Suffix), Option<EncodedPodRow>>;

struct FrozenBucket<K: Keyspace> {
	rows: PendingRows<K>,
	_groups: BTreeMap<GroupId, usize>,
}

impl<K: Keyspace> FrozenRows for FrozenBucket<K> {
	fn for_each(&self, visit: &mut dyn FnMut(GroupId, &[u8], &Option<EncodedPodRow>)) {
		for ((group, suffix), post) in &self.rows {
			visit(*group, &suffix.to_suffix_bytes(), post);
		}
	}
}

pub struct StandardBucket<K: Keyspace> {
	operator: OperatorId,
	partitions: BTreeMap<GroupId, Partition<K>>,
	bytes: ByteSize,
	entries: usize,
	tombstones: usize,
	pending: PendingRows<K>,
	pending_tombstones: usize,
	frozen_tombstones: usize,
	dirty_bytes: ByteSize,
	dirty_groups: BTreeMap<GroupId, usize>,
	active_generation: u32,
	frozen_generation: u32,
	settled_generation: u32,
}

impl<K: Keyspace> StandardBucket<K> {
	pub fn new(operator: OperatorId) -> Self {
		Self {
			operator,
			partitions: BTreeMap::new(),
			bytes: ByteSize::ZERO,
			entries: 0,
			tombstones: 0,
			pending: BTreeMap::new(),
			pending_tombstones: 0,
			frozen_tombstones: 0,
			dirty_bytes: ByteSize::ZERO,
			dirty_groups: BTreeMap::new(),
			active_generation: 1,
			frozen_generation: 0,
			settled_generation: 0,
		}
	}

	fn generations(&self) -> Generations {
		Generations {
			settled: self.settled_generation,
			frozen: self.frozen_generation,
		}
	}

	pub fn staged_of(&self, entry: &WriteEntry) -> Staged {
		self.generations().staged(entry)
	}

	pub fn operator(&self) -> OperatorId {
		self.operator
	}

	fn suffix_bytes() -> ByteSize {
		ByteSize::from_bytes(size_of::<K::Suffix>() as u64)
	}

	fn group_bytes() -> ByteSize {
		ByteSize::from_bytes(size_of::<GroupId>() as u64)
	}

	pub fn len(&self) -> usize {
		self.entries
	}

	pub fn tombstone_len(&self) -> usize {
		self.tombstones
	}

	pub fn is_empty(&self) -> bool {
		self.entries == 0
	}

	pub fn footprint(&self) -> ByteSize {
		self.bytes
	}

	pub fn dirty_len(&self) -> usize {
		self.pending.len()
	}

	pub fn dirty_footprint(&self) -> ByteSize {
		self.dirty_bytes.saturating_add(Self::group_bytes() * self.dirty_groups.len() as u64)
	}

	pub fn freeze(&mut self) -> Option<(Box<dyn FrozenRows>, usize, ByteSize)> {
		if self.pending.is_empty() {
			return None;
		}
		reifydb_assertions! {
			assert_eq!(
				self.frozen_generation, self.settled_generation,
				"operator {} froze a second generation before the previous one settled, so the rows of generation {} would read as clean without reaching the device",
				self.operator.0, self.frozen_generation
			);
		}
		let count = self.pending.len();
		let bytes = self.dirty_footprint();
		let rows = mem::take(&mut self.pending);
		let groups = mem::take(&mut self.dirty_groups);
		self.frozen_tombstones = mem::take(&mut self.pending_tombstones);
		self.dirty_bytes = ByteSize::ZERO;
		self.frozen_generation = self.active_generation;
		self.active_generation += 1;
		Some((
			Box::new(FrozenBucket::<K> {
				rows,
				_groups: groups,
			}),
			count,
			bytes,
		))
	}

	pub fn evict_clean(&mut self, bytes: &mut ByteSize, tombstones: &mut usize) -> (usize, ByteSize) {
		let mut evicted = 0usize;
		let mut evicted_tombstones = 0usize;
		let mut freed = ByteSize::ZERO;
		let suffix_cost = Self::suffix_bytes();
		let group_cost = Self::group_bytes();
		let generations = self.generations();
		self.partitions.retain(|_, partition| {
			{
				let mut sweep = |entry: &mut WriteEntry, tombstone: bool| {
					if !matches!(generations.staged(entry), Staged::Clean) {
						return true;
					}
					if bytes.as_bytes() == 0 && (!tombstone || *tombstones == 0) {
						return true;
					}
					if entry.take_reference() {
						return true;
					}
					let cost = entry.row_bytes().saturating_add(suffix_cost);
					freed = freed.saturating_add(cost);
					*bytes = bytes.saturating_sub(cost);
					if tombstone {
						*tombstones = tombstones.saturating_sub(1);
						evicted_tombstones += 1;
					}
					evicted += 1;
					false
				};
				partition.live.retain(|_: &K::Suffix, entry: &mut WriteEntry| sweep(entry, false));
				partition.deleted.retain(|_: &K::Suffix, entry: &mut WriteEntry| sweep(entry, true));
			}
			if partition.is_empty() {
				freed = freed.saturating_add(group_cost);
				*bytes = bytes.saturating_sub(group_cost);
				return false;
			}
			true
		});
		self.entries -= evicted;
		self.tombstones -= evicted_tombstones;
		self.bytes = self.bytes.saturating_sub(freed);
		(evicted, freed)
	}

	pub fn settle_flushing(&mut self) {
		self.settled_generation = self.frozen_generation;
		self.tombstones += mem::take(&mut self.frozen_tombstones);
	}

	pub fn record(&mut self, group: GroupId, suffix: K::Suffix, post: Option<EncodedPodRow>) {
		self.write(group, suffix, post, false);
	}

	pub fn record_fresh(&mut self, group: GroupId, suffix: K::Suffix, post: Option<EncodedPodRow>) {
		self.write(group, suffix, post, true);
	}

	fn write(&mut self, group: GroupId, suffix: K::Suffix, post: Option<EncodedPodRow>, fresh: bool) {
		if let Entry::Vacant(entry) = self.partitions.entry(group) {
			entry.insert(Partition::new());
			self.bytes = self.bytes.saturating_add(Self::group_bytes());
		}
		let generations = self.generations();
		let active = self.active_generation;
		let partition = self.partitions.get_mut(&group).expect("the partition was just inserted");
		let incoming = WriteEntry::bytes_of(&post);
		let tombstone = post.is_none();
		let (target, other) = match post.is_some() {
			true => (&mut partition.live, &mut partition.deleted),
			false => (&mut partition.deleted, &mut partition.live),
		};
		let mut dirtied = false;
		let mut uncharge = ByteSize::ZERO;
		let outgoing = match target.get_mut(&suffix) {
			Some(entry) => {
				let outgoing = entry.row_bytes();
				let staged = generations.staged(entry);
				match staged.is_dirty() {
					true => {
						uncharge = outgoing.saturating_add(Self::suffix_bytes());
					}
					false => dirtied = true,
				}
				if tombstone {
					match staged {
						Staged::Clean => self.tombstones -= 1,
						Staged::Flushing => self.frozen_tombstones -= 1,
						Staged::Never | Staged::Dirty => {}
					}
				}
				entry.post = post.clone();
				entry.generation = active;
				entry.never = staged.is_collapsible();
				entry.touch();
				outgoing
			}
			None => match other.remove(&suffix) {
				Some(moved) => {
					let staged = generations.staged(&moved);
					match staged.is_dirty() {
						true => {
							uncharge =
								moved.row_bytes().saturating_add(Self::suffix_bytes());
						}
						false => dirtied = true,
					}
					if !tombstone {
						match staged {
							Staged::Clean => self.tombstones -= 1,
							Staged::Flushing => self.frozen_tombstones -= 1,
							Staged::Never | Staged::Dirty => {}
						}
					}
					target.insert(
						suffix.clone(),
						WriteEntry::new(post.clone(), active, staged.is_collapsible()),
					);
					moved.row_bytes()
				}
				None => {
					target.insert(suffix.clone(), WriteEntry::new(post.clone(), active, fresh));
					self.bytes = self.bytes.saturating_add(Self::suffix_bytes());
					self.entries += 1;
					dirtied = true;
					ByteSize::ZERO
				}
			},
		};
		if dirtied {
			*self.dirty_groups.entry(group).or_insert(0) += 1;
		}
		let replaced = self.pending.insert((group, suffix), post);
		match (tombstone, matches!(replaced, Some(None))) {
			(true, false) => self.pending_tombstones += 1,
			(false, true) => self.pending_tombstones -= 1,
			(true, true) | (false, false) => {}
		}
		self.dirty_bytes = self
			.dirty_bytes
			.saturating_sub(uncharge)
			.saturating_add(incoming.saturating_add(Self::suffix_bytes()));
		self.bytes = self.bytes.saturating_sub(outgoing).saturating_add(incoming);
	}

	pub fn erase(&mut self, group: GroupId, suffix: &K::Suffix) -> bool {
		let generations = self.generations();
		let Some(partition) = self.partitions.get_mut(&group) else {
			return false;
		};
		if !partition.get(suffix).is_some_and(|entry| generations.staged(entry).is_collapsible()) {
			return false;
		}
		let entry = match partition.live.remove(suffix) {
			Some(entry) => entry,
			None => partition.deleted.remove(suffix).expect("the entry was just observed"),
		};
		self.entries -= 1;
		if matches!(self.pending.remove(&(group, suffix.clone())), Some(None)) {
			self.pending_tombstones -= 1;
		}
		let remaining = self.dirty_groups.get_mut(&group).expect("a dirty row keeps its group counted");
		*remaining -= 1;
		if *remaining == 0 {
			self.dirty_groups.remove(&group);
		}
		self.dirty_bytes =
			self.dirty_bytes.saturating_sub(Self::suffix_bytes()).saturating_sub(entry.row_bytes());
		self.bytes = self.bytes.saturating_sub(Self::suffix_bytes()).saturating_sub(entry.row_bytes());
		if partition.is_empty() {
			self.partitions.remove(&group);
			self.bytes = self.bytes.saturating_sub(Self::group_bytes());
		}
		true
	}

	pub fn get(&self, group: GroupId, suffix: &K::Suffix) -> Option<&WriteEntry> {
		self.partitions.get(&group)?.get(suffix)
	}

	pub fn range<R: RangeBounds<K::Suffix>>(
		&self,
		group: GroupId,
		bounds: R,
	) -> impl DoubleEndedIterator<Item = (&K::Suffix, &WriteEntry)> {
		self.partitions.get(&group).map(|partition| partition.live.range(bounds)).into_iter().flatten()
	}

	pub fn last(&self, group: GroupId) -> Option<(&K::Suffix, &WriteEntry)> {
		let partition = self.partitions.get(&group)?;
		match (partition.live.last_key_value(), partition.deleted.last_key_value()) {
			(None, None) => None,
			(Some(live), None) => Some(live),
			(None, Some(deleted)) => Some(deleted),
			(Some(live), Some(deleted)) => match live.0 >= deleted.0 {
				true => Some(live),
				false => Some(deleted),
			},
		}
	}

	pub fn entries(&self) -> impl Iterator<Item = (GroupId, &K::Suffix, &WriteEntry)> {
		self.partitions.iter().rev().flat_map(|(group, partition)| {
			partition.merged().map(move |(suffix, entry)| (*group, suffix, entry))
		})
	}
}

impl<K: Keyspace> Bucket for StandardBucket<K> {
	fn footprint(&self) -> ByteSize {
		self.bytes
	}

	fn len(&self) -> usize {
		StandardBucket::len(self)
	}

	fn tombstone_len(&self) -> usize {
		StandardBucket::tombstone_len(self)
	}

	fn dirty_len(&self) -> usize {
		StandardBucket::dirty_len(self)
	}

	fn dirty_footprint(&self) -> ByteSize {
		StandardBucket::dirty_footprint(self)
	}

	fn freeze(&mut self) -> Option<(Box<dyn FrozenRows>, usize, ByteSize)> {
		StandardBucket::freeze(self)
	}

	#[cfg(test)]
	fn flushing_len(&self) -> usize {
		self.entries().filter(|(_, _, entry)| matches!(self.staged_of(entry), Staged::Flushing)).count()
	}

	fn evict_clean(&mut self, bytes: &mut ByteSize, tombstones: &mut usize) -> (usize, ByteSize) {
		StandardBucket::evict_clean(self, bytes, tombstones)
	}

	fn settle_flushing(&mut self) {
		StandardBucket::settle_flushing(self)
	}

	fn for_each(&self, visit: &mut dyn FnMut(GroupId, &[u8], &WriteEntry)) {
		for (group, suffix, entry) in self.entries() {
			visit(group, &suffix.to_suffix_bytes(), entry);
		}
	}

	fn groups_in_range(&self, lower: &Bound<GroupId>, upper: &Bound<GroupId>) -> GroupIds {
		self.partitions.range((*lower, *upper)).map(|(group, _)| *group).collect()
	}

	fn encoded_range_in(
		&self,
		group: GroupId,
		start: &Bound<Vec<u8>>,
		end: &Bound<Vec<u8>>,
		scan: Scan,
		limit: usize,
		tombstones: bool,
	) -> Vec<(GroupStateKey, WriteEntry)> {
		let bounds = (suffix_bound::<K>(start, 0x00), suffix_bound::<K>(end, 0xFF));
		let encode = |suffix: &K::Suffix, entry: &WriteEntry| {
			if entry.post.is_some() {
				entry.touch();
			}
			(OperatorStateKey::inner_encoded(group, K::ID, suffix.to_suffix_bytes()), entry.clone())
		};
		if tombstones {
			let Some(partition) = self.partitions.get(&group) else {
				return Vec::new();
			};
			return partition
				.merged_range(bounds, scan, limit)
				.into_iter()
				.map(|(suffix, entry)| encode(suffix, entry))
				.collect();
		}
		match scan {
			Scan::Forward => self
				.range(group, bounds)
				.take(limit)
				.map(|(suffix, entry)| encode(suffix, entry))
				.collect(),
			Scan::Backward => {
				let mut out: Vec<(GroupStateKey, WriteEntry)> = self
					.range(group, bounds)
					.rev()
					.take(limit)
					.map(|(suffix, entry)| encode(suffix, entry))
					.collect();
				out.reverse();
				out
			}
		}
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn as_any_mut(&mut self) -> &mut dyn Any {
		self
	}
}

fn suffix_bound<K: Keyspace>(bound: &Bound<Vec<u8>>, fill: u8) -> Bound<K::Suffix> {
	match bound {
		Bound::Unbounded => Bound::Unbounded,
		Bound::Included(bytes) => Bound::Included(padded::<K>(bytes, fill)),
		Bound::Excluded(bytes) => Bound::Excluded(padded::<K>(bytes, fill)),
	}
}

fn padded<K: Keyspace>(bytes: &[u8], fill: u8) -> K::Suffix {
	let mut out = bytes.to_vec();
	out.resize(columns_width(<K::Suffix as KeyLayout>::COLUMNS), fill);
	K::Suffix::from_suffix_bytes(&out).expect("a padded suffix must decode as its own key type")
}
