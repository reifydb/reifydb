// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	any::Any,
	collections::{BTreeMap, btree_map::Entry},
	iter::Peekable,
	mem::{replace, size_of, take},
	ops::{Bound, RangeBounds},
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteEntry {
	pub post: Option<EncodedPodRow>,
	pub never_staged: bool,
}

impl WriteEntry {
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
}

impl<K: Keyspace> Partition<K> {
	fn new() -> Self {
		Self {
			live: SortedVecMap::new(),
			deleted: SortedVecMap::new(),
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
}

impl<K: Keyspace> TypedBucket<K> {
	pub fn new(operator: OperatorId) -> Self {
		Self {
			operator,
			partitions: BTreeMap::new(),
			bytes: ByteSize::ZERO,
			entries: 0,
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
		let outgoing = match target.get_mut(&suffix) {
			Some(entry) => {
				let outgoing = entry.row_bytes();
				entry.post = post;
				outgoing
			}
			None => match other.remove(&suffix) {
				Some(moved) => {
					target.insert(
						suffix,
						WriteEntry {
							post,
							never_staged: moved.never_staged,
						},
					);
					moved.row_bytes()
				}
				None => {
					target.insert(
						suffix,
						WriteEntry {
							post,
							never_staged: fresh,
						},
					);
					self.bytes = self.bytes.saturating_add(Self::suffix_bytes());
					self.entries += 1;
					ByteSize::ZERO
				}
			},
		};
		self.bytes = self.bytes.saturating_sub(outgoing).saturating_add(incoming);
	}

	pub fn erase(&mut self, group: GroupId, suffix: &K::Suffix) -> bool {
		let Some(partition) = self.partitions.get_mut(&group) else {
			return false;
		};
		if !partition.get(suffix).is_some_and(|entry| entry.never_staged) {
			return false;
		}
		let entry = match partition.live.remove(suffix) {
			Some(entry) => entry,
			None => partition.deleted.remove(suffix).expect("the entry was just observed"),
		};
		self.entries -= 1;
		self.bytes = self
			.bytes
			.saturating_sub(Self::suffix_bytes())
			.saturating_sub(entry.row_bytes());
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
			released = released.saturating_add(Self::suffix_bytes()).saturating_add(entry.row_bytes());
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
