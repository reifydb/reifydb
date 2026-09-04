// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	collections::{BTreeMap, btree_map::Entry},
	iter::Peekable,
	mem::{replace, size_of, take},
	ops::{Bound, RangeBounds},
	sync::atomic::{AtomicBool, Ordering},
};

use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
use reifydb_core::{
	interface::catalog::flow::OperatorId,
	key::{
		operator::{
			keyspace::columns_width,
			state::{GroupId, KeyspaceId, OperatorStateKey},
			traits::Keyspace,
		},
		typed::layout::KeyLayout,
	},
	state::typed::SuffixBytes,
	util::sorted::SortedVecMap,
};
use reifydb_value::{Result, byte_size::ByteSize};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use rusqlite::{Connection, Transaction};

use crate::tier::bucket::{AnyBucket, Budget, GroupIds, Resume, Scan};
#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
use crate::tier::persistent::sqlite::typed;

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

	fn dirtied(self) -> Self {
		match self {
			Self::Never => Self::Never,
			Self::Flushing | Self::Dirty | Self::Clean => Self::Dirty,
		}
	}
}

#[derive(Debug)]
pub struct WriteEntry {
	pub post: Option<EncodedPodRow>,
	pub staged: Staged,
	referenced: AtomicBool,
}

impl Clone for WriteEntry {
	fn clone(&self) -> Self {
		Self {
			post: self.post.clone(),
			staged: self.staged,
			referenced: AtomicBool::new(self.referenced.load(Ordering::Relaxed)),
		}
	}
}

impl PartialEq for WriteEntry {
	fn eq(&self, other: &Self) -> bool {
		self.post == other.post && self.staged == other.staged
	}
}

impl Eq for WriteEntry {}

impl WriteEntry {
	fn new(post: Option<EncodedPodRow>, staged: Staged) -> Self {
		Self {
			post,
			staged,
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
	live: SortedVecMap<K::Suffix, WriteEntry>,
	deleted: SortedVecMap<K::Suffix, WriteEntry>,
	dirty: usize,
}

impl<K: Keyspace> Partition<K> {
	fn new() -> Self {
		Self {
			live: SortedVecMap::new(),
			deleted: SortedVecMap::new(),
			dirty: 0,
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
		}
	}
}

struct Merged<I: Iterator> {
	live: Peekable<I>,
	deleted: Peekable<I>,
}

impl<'a, S: Ord + 'a, I: Iterator<Item = (&'a S, &'a WriteEntry)>> Iterator for Merged<I> {
	type Item = (&'a S, &'a WriteEntry);

	fn next(&mut self) -> Option<Self::Item> {
		match (self.live.peek(), self.deleted.peek()) {
			(None, None) => None,
			(Some(_), None) => self.live.next(),
			(None, Some(_)) => self.deleted.next(),
			(Some((live, _)), Some((deleted, _))) => match live <= deleted {
				true => self.live.next(),
				false => self.deleted.next(),
			},
		}
	}
}

pub struct TypedBucket<K: Keyspace> {
	operator: OperatorId,
	partitions: BTreeMap<GroupId, Partition<K>>,
	bytes: ByteSize,
	entries: usize,
	dirty: usize,
	dirty_bytes: ByteSize,
	dirty_groups: usize,
}

impl<K: Keyspace> TypedBucket<K> {
	pub fn new(operator: OperatorId) -> Self {
		Self {
			operator,
			partitions: BTreeMap::new(),
			bytes: ByteSize::ZERO,
			entries: 0,
			dirty: 0,
			dirty_bytes: ByteSize::ZERO,
			dirty_groups: 0,
		}
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

	pub fn is_empty(&self) -> bool {
		self.entries == 0
	}

	pub fn footprint(&self) -> ByteSize {
		self.bytes
	}

	pub fn dirty_len(&self) -> usize {
		self.dirty
	}

	pub fn dirty_footprint(&self) -> ByteSize {
		self.dirty_bytes.saturating_add(Self::group_bytes() * self.dirty_groups as u64)
	}

	pub fn stage_dirty(&mut self, visit: &mut dyn FnMut(GroupId, &[u8], &WriteEntry)) -> ByteSize {
		let mut staged = ByteSize::ZERO;
		for (group, partition) in self.partitions.iter_mut() {
			let mut charged_group = false;
			for (suffix, entry) in partition.live.iter_mut().chain(partition.deleted.iter_mut()) {
				if !entry.staged.is_dirty() {
					continue;
				}
				visit(*group, &suffix.to_suffix_bytes(), entry);
				entry.staged = Staged::Flushing;
				self.dirty -= 1;
				self.dirty_bytes = self
					.dirty_bytes
					.saturating_sub(entry.row_bytes())
					.saturating_sub(Self::suffix_bytes());
				staged = staged.saturating_add(entry.row_bytes()).saturating_add(Self::suffix_bytes());
				if !charged_group {
					staged = staged.saturating_add(Self::group_bytes());
					charged_group = true;
				}
			}
			if charged_group {
				partition.dirty = 0;
				self.dirty_groups -= 1;
			}
		}
		staged
	}

	pub fn revert_flushing(&mut self) -> usize {
		let mut reverted = 0usize;
		let mut restored = ByteSize::ZERO;
		for partition in self.partitions.values_mut() {
			let mut restored_here = 0usize;
			for (_, entry) in partition.live.iter_mut().chain(partition.deleted.iter_mut()) {
				if matches!(entry.staged, Staged::Flushing) {
					entry.staged = Staged::Dirty;
					restored_here += 1;
					restored = restored
						.saturating_add(entry.row_bytes())
						.saturating_add(Self::suffix_bytes());
				}
			}
			if restored_here > 0 && partition.dirty == 0 {
				self.dirty_groups += 1;
			}
			partition.dirty += restored_here;
			reverted += restored_here;
		}
		self.dirty += reverted;
		self.dirty_bytes = self.dirty_bytes.saturating_add(restored);
		reverted
	}

	pub fn evict_clean(&mut self, bytes: &mut ByteSize, entries: &mut usize) -> (usize, ByteSize) {
		let mut evicted = 0usize;
		let mut freed = ByteSize::ZERO;
		let suffix_cost = Self::suffix_bytes();
		let group_cost = Self::group_bytes();
		self.partitions.retain(|_, partition| {
			{
				let mut sweep = |_: &K::Suffix, entry: &mut WriteEntry| {
					if !matches!(entry.staged, Staged::Clean) {
						return true;
					}
					if bytes.as_bytes() == 0 && *entries == 0 {
						return true;
					}
					if entry.take_reference() {
						return true;
					}
					let cost = entry.row_bytes().saturating_add(suffix_cost);
					freed = freed.saturating_add(cost);
					*bytes = bytes.saturating_sub(cost);
					*entries = entries.saturating_sub(1);
					evicted += 1;
					false
				};
				partition.live.retain(&mut sweep);
				partition.deleted.retain(&mut sweep);
			}
			if partition.is_empty() {
				freed = freed.saturating_add(group_cost);
				*bytes = bytes.saturating_sub(group_cost);
				return false;
			}
			true
		});
		self.entries -= evicted;
		self.bytes = self.bytes.saturating_sub(freed);
		(evicted, freed)
	}

	pub fn settle_flushing(&mut self) {
		for partition in self.partitions.values_mut() {
			for (_, entry) in partition.live.iter_mut().chain(partition.deleted.iter_mut()) {
				if matches!(entry.staged, Staged::Flushing) {
					entry.staged = Staged::Clean;
				}
			}
		}
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
		let partition = self.partitions.get_mut(&group).expect("the partition was just inserted");
		let incoming = WriteEntry::bytes_of(&post);
		let (target, other) = match post.is_some() {
			true => (&mut partition.live, &mut partition.deleted),
			false => (&mut partition.deleted, &mut partition.live),
		};
		let mut dirtied = 0usize;
		let mut uncharge = ByteSize::ZERO;
		let outgoing = match target.get_mut(&suffix) {
			Some(entry) => {
				let outgoing = entry.row_bytes();
				match entry.staged.is_dirty() {
					true => {
						uncharge = outgoing.saturating_add(Self::suffix_bytes());
					}
					false => dirtied += 1,
				}
				entry.post = post;
				entry.staged = entry.staged.dirtied();
				entry.touch();
				outgoing
			}
			None => match other.remove(&suffix) {
				Some(moved) => {
					match moved.staged.is_dirty() {
						true => {
							uncharge =
								moved.row_bytes().saturating_add(Self::suffix_bytes());
						}
						false => dirtied += 1,
					}
					target.insert(suffix, WriteEntry::new(post, moved.staged.dirtied()));
					moved.row_bytes()
				}
				None => {
					target.insert(
						suffix,
						WriteEntry::new(
							post,
							match fresh {
								true => Staged::Never,
								false => Staged::Dirty,
							},
						),
					);
					self.bytes = self.bytes.saturating_add(Self::suffix_bytes());
					self.entries += 1;
					dirtied += 1;
					ByteSize::ZERO
				}
			},
		};
		if dirtied > 0 {
			let partition = self.partitions.get_mut(&group).expect("the partition was just inserted");
			partition.dirty += dirtied;
			if partition.dirty == dirtied {
				self.dirty_groups += 1;
			}
		}
		self.dirty += dirtied;
		self.dirty_bytes = self
			.dirty_bytes
			.saturating_sub(uncharge)
			.saturating_add(incoming.saturating_add(Self::suffix_bytes()));
		self.bytes = self.bytes.saturating_sub(outgoing).saturating_add(incoming);
	}

	pub fn erase(&mut self, group: GroupId, suffix: &K::Suffix) -> bool {
		let Some(partition) = self.partitions.get_mut(&group) else {
			return false;
		};
		if !partition.get(suffix).is_some_and(|entry| entry.staged.is_collapsible()) {
			return false;
		}
		let entry = match partition.live.remove(suffix) {
			Some(entry) => entry,
			None => partition.deleted.remove(suffix).expect("the entry was just observed"),
		};
		self.entries -= 1;
		self.dirty -= 1;
		partition.dirty -= 1;
		if partition.dirty == 0 {
			self.dirty_groups -= 1;
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

	pub fn absorb(&mut self, other: Self) {
		for (group, partition) in other.partitions {
			for (suffix, entry) in partition.live {
				self.record(group, suffix, entry.post);
			}
			for (suffix, entry) in partition.deleted {
				self.record(group, suffix, entry.post);
			}
		}
	}

	pub fn clear(&mut self) {
		self.partitions.clear();
		self.bytes = ByteSize::ZERO;
		self.entries = 0;
		self.dirty = 0;
		self.dirty_bytes = ByteSize::ZERO;
		self.dirty_groups = 0;
	}
}

impl<K: Keyspace> AnyBucket for TypedBucket<K> {
	fn keyspace(&self) -> KeyspaceId {
		K::ID
	}

	fn footprint(&self) -> ByteSize {
		self.bytes
	}

	fn len(&self) -> usize {
		TypedBucket::len(self)
	}

	fn dirty_len(&self) -> usize {
		TypedBucket::dirty_len(self)
	}

	fn dirty_footprint(&self) -> ByteSize {
		TypedBucket::dirty_footprint(self)
	}

	fn stage_dirty(&mut self, visit: &mut dyn FnMut(GroupId, &[u8], &WriteEntry)) -> ByteSize {
		TypedBucket::stage_dirty(self, visit)
	}

	fn revert_flushing(&mut self) -> usize {
		TypedBucket::revert_flushing(self)
	}

	fn evict_clean(&mut self, bytes: &mut ByteSize, entries: &mut usize) -> (usize, ByteSize) {
		TypedBucket::evict_clean(self, bytes, entries)
	}

	fn settle_flushing(&mut self) {
		TypedBucket::settle_flushing(self)
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	fn write_into(&self, txn: &Transaction) {
		let mut sets: Vec<(OperatorId, K::GroupedKey, Vec<u8>)> = Vec::new();
		let mut removes: Vec<(OperatorId, K::GroupedKey)> = Vec::new();
		for (group, suffix, entry) in self.entries() {
			let key = K::join(group, suffix.clone());
			match &entry.post {
				Some(row) => sets.push((self.operator, key, row.as_slice().to_vec())),
				None => removes.push((self.operator, key)),
			}
		}
		typed::set_chunked::<K>(txn, &sets);
		typed::remove_chunked::<K>(txn, &removes);
	}

	#[cfg(all(feature = "sqlite", not(target_arch = "wasm32")))]
	fn flush(&mut self, conn: &Connection) -> Result<()> {
		for (group, partition) in take(&mut self.partitions) {
			for (suffix, entry) in partition.live {
				let key = K::join(group, suffix);
				let row = entry.post.expect("a live entry carries a row");
				typed::set::<K>(conn, self.operator, &key, row.as_slice());
			}
			for (suffix, _) in partition.deleted {
				typed::remove::<K>(conn, self.operator, &K::join(group, suffix));
			}
		}
		self.bytes = ByteSize::ZERO;
		self.entries = 0;
		Ok(())
	}

	fn reap_group(&mut self, group: GroupId, budget: &mut Budget) -> Result<Resume> {
		let Some(partition) = self.partitions.get_mut(&group) else {
			return Ok(Resume::Done);
		};
		let mut released = ByteSize::ZERO;
		let had_dirty = partition.dirty > 0;
		while budget.rows > 0 {
			let next = match (partition.live.first_key_value(), partition.deleted.first_key_value()) {
				(None, None) => None,
				(Some(_), None) => partition.live.pop_first(),
				(None, Some(_)) => partition.deleted.pop_first(),
				(Some((live, _)), Some((deleted, _))) => match live <= deleted {
					true => partition.live.pop_first(),
					false => partition.deleted.pop_first(),
				},
			};
			let Some((_, entry)) = next else {
				break;
			};
			budget.rows -= 1;
			self.entries -= 1;
			if entry.staged.is_dirty() {
				self.dirty -= 1;
				partition.dirty -= 1;
				self.dirty_bytes = self
					.dirty_bytes
					.saturating_sub(Self::suffix_bytes())
					.saturating_sub(entry.row_bytes());
			}
			released = released.saturating_add(Self::suffix_bytes()).saturating_add(entry.row_bytes());
		}
		if had_dirty && partition.dirty == 0 {
			self.dirty_groups -= 1;
		}
		let drained = partition.is_empty();
		self.bytes = self.bytes.saturating_sub(released);
		if drained {
			self.partitions.remove(&group);
			self.bytes = self.bytes.saturating_sub(Self::group_bytes());
			return Ok(Resume::Done);
		}
		Ok(Resume::More)
	}

	fn for_each(&self, visit: &mut dyn FnMut(GroupId, &[u8], &WriteEntry)) {
		for (group, suffix, entry) in self.entries() {
			visit(group, &suffix.to_suffix_bytes(), entry);
		}
	}

	fn encoded_entries(&self) -> Vec<(EncodedKey, WriteEntry)> {
		self.entries()
			.map(|(group, suffix, entry)| {
				(
					OperatorStateKey::inner_encoded(group, K::ID, suffix.to_suffix_bytes())
						.into_encoded(),
					entry.clone(),
				)
			})
			.collect()
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
	) -> Vec<(EncodedKey, WriteEntry)> {
		let bounds = (suffix_bound::<K>(start, 0x00), suffix_bound::<K>(end, 0xFF));
		let encode = |suffix: &K::Suffix, entry: &WriteEntry| {
			entry.touch();
			(
				OperatorStateKey::inner_encoded(group, K::ID, suffix.to_suffix_bytes()).into_encoded(),
				entry.clone(),
			)
		};
		match scan {
			Scan::Forward => self
				.range(group, bounds)
				.take(limit)
				.map(|(suffix, entry)| encode(suffix, entry))
				.collect(),
			Scan::Backward => {
				let mut out: Vec<(EncodedKey, WriteEntry)> = self
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

	fn absorb_any(&mut self, other: &mut dyn AnyBucket) {
		let other = other
			.as_any_mut()
			.downcast_mut::<Self>()
			.expect("a keyspace id must map to exactly one key type");
		self.absorb(Self {
			operator: other.operator,
			partitions: take(&mut other.partitions),
			bytes: replace(&mut other.bytes, ByteSize::ZERO),
			entries: replace(&mut other.entries, 0),
			dirty: replace(&mut other.dirty, 0),
			dirty_bytes: replace(&mut other.dirty_bytes, ByteSize::ZERO),
			dirty_groups: replace(&mut other.dirty_groups, 0),
		});
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
