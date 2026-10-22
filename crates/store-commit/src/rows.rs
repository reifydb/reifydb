// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	cmp::{Ordering, Reverse},
	collections::{BTreeMap, BinaryHeap, HashMap, HashSet, btree_map},
	mem,
	ops::RangeBounds,
};

use reifydb_codec::key::encoded::EncodedKey;
use reifydb_core::{common::CommitVersion, metrics::heap::HeapSize};
use reifydb_value::reifydb_assertions;

use crate::entry::{Value, entry_bytes_with};

#[derive(Clone)]
pub(super) enum Versions {
	One(CommitVersion, Value),
	Many(BTreeMap<Reverse<CommitVersion>, Value>),
}

impl Versions {
	fn insert(&mut self, version: CommitVersion, value: Value) -> Option<Value> {
		match self {
			Versions::One(held, current) if *held == version => Some(mem::replace(current, value)),
			Versions::One(held, current) => {
				let current = current.take();
				*self = Versions::Many(BTreeMap::from([
					(Reverse(*held), current),
					(Reverse(version), value),
				]));
				None
			}
			Versions::Many(versions) => versions.insert(Reverse(version), value),
		}
	}

	fn remove(&mut self, version: CommitVersion) -> Option<Value> {
		let removed = match self {
			Versions::One(held, value) if *held == version => {
				let value = value.take();
				*self = Versions::Many(BTreeMap::new());
				return Some(value);
			}
			Versions::One(..) => return None,
			Versions::Many(versions) => versions.remove(&Reverse(version)),
		};
		if let Versions::Many(versions) = self
			&& versions.len() == 1
		{
			let (Reverse(held), value) = versions.pop_first().expect("one version is left");
			*self = Versions::One(held, value);
		}
		removed
	}

	fn is_empty(&self) -> bool {
		matches!(self, Versions::Many(versions) if versions.is_empty())
	}

	fn visible(&self, version: CommitVersion) -> Option<(CommitVersion, &Value)> {
		match self {
			Versions::One(held, value) => (*held <= version).then_some((*held, value)),
			Versions::Many(versions) => {
				versions.range(Reverse(version)..).next().map(|(Reverse(found), value)| (*found, value))
			}
		}
	}

	pub fn exact(&self, version: CommitVersion) -> Option<&Value> {
		match self {
			Versions::One(held, value) => (*held == version).then_some(value),
			Versions::Many(versions) => versions.get(&Reverse(version)),
		}
	}

	pub fn iter(&self) -> impl Iterator<Item = (CommitVersion, &Value)> {
		let (one, many) = match self {
			Versions::One(held, value) => (Some((*held, value)), None),
			Versions::Many(versions) => {
				(None, Some(versions.iter().map(|(Reverse(version), value)| (*version, value))))
			}
		};
		one.into_iter().chain(many.into_iter().flatten())
	}
}

#[derive(Clone, Default)]
pub(super) struct RowMap {
	entries: BTreeMap<EncodedKey, Versions>,
	bytes: u64,
	versions: BTreeMap<CommitVersion, u32>,
}

impl RowMap {
	pub fn insert(&mut self, key: EncodedKey, version: CommitVersion, value: Value) {
		let key_heap = key.heap_size();
		let bytes = entry_bytes_with(key_heap, &value);

		let replaced = match self.entries.entry(key) {
			btree_map::Entry::Vacant(slot) => {
				slot.insert(Versions::One(version, value));
				None
			}
			btree_map::Entry::Occupied(mut slot) => slot.get_mut().insert(version, value),
		};
		match replaced {
			Some(replaced) => self.bytes = self.bytes.saturating_sub(entry_bytes_with(key_heap, &replaced)),
			None => *self.versions.entry(version).or_insert(0) += 1,
		}
		self.bytes = self.bytes.saturating_add(bytes);
	}

	pub fn remove(&mut self, dropped: &HashMap<EncodedKey, HashSet<CommitVersion>>) -> Vec<Removed> {
		let mut removed = Vec::new();
		for (key, versions) in dropped {
			let Some(held) = self.entries.get_mut(key) else {
				continue;
			};
			let key_heap = key.heap_size();
			for version in versions {
				let Some(value) = held.remove(*version) else {
					continue;
				};
				self.bytes = self.bytes.saturating_sub(entry_bytes_with(key_heap, &value));
				Self::forget(&mut self.versions, *version);
				removed.push(Removed {
					key: key.clone(),
					version: *version,
					value,
				});
			}
			if held.is_empty() {
				self.entries.remove(key);
			}
		}
		removed
	}

	fn forget(versions: &mut BTreeMap<CommitVersion, u32>, version: CommitVersion) {
		let count = versions.get_mut(&version).expect("every stored version is counted");
		*count -= 1;
		if *count == 0 {
			versions.remove(&version);
		}
	}

	pub fn get(&self, key: &[u8], version: CommitVersion) -> Option<(CommitVersion, &Value)> {
		self.entries.get(key).and_then(|versions| versions.visible(version))
	}

	pub fn versions_for(&self, key: &[u8]) -> Option<&Versions> {
		self.entries.get(key)
	}

	pub fn range<R>(&self, bounds: R) -> btree_map::Range<'_, EncodedKey, Versions>
	where
		R: RangeBounds<[u8]>,
	{
		self.entries.range::<[u8], R>(bounds)
	}

	pub fn bytes(&self) -> u64 {
		self.bytes
	}

	pub fn is_empty(&self) -> bool {
		self.entries.is_empty()
	}

	pub fn min_version(&self) -> Option<CommitVersion> {
		self.versions.keys().next().copied()
	}

	pub fn max_version(&self) -> Option<CommitVersion> {
		self.versions.keys().next_back().copied()
	}

	fn packed(self) -> Self {
		Self {
			entries: self.entries.into_iter().collect(),
			bytes: self.bytes,
			versions: self.versions,
		}
	}
}

pub(super) struct Removed {
	pub key: EncodedKey,
	pub version: CommitVersion,
	pub value: Value,
}

pub(super) fn lookup<'a>(
	maps: impl Iterator<Item = &'a RowMap>,
	key: &[u8],
	version: CommitVersion,
) -> Option<(CommitVersion, &'a Value)> {
	let mut best: Option<(CommitVersion, &'a Value)> = None;
	for rows in maps {
		if rows.min_version().is_none_or(|min| min > version) {
			continue;
		}
		if let Some((found, _)) = best
			&& rows.max_version().is_some_and(|max| max <= found)
		{
			continue;
		}
		if let Some((found, value)) = rows.get(key, version)
			&& best.is_none_or(|(best, _)| found > best)
		{
			best = Some((found, value));
		}
	}
	best
}

struct Head<'a> {
	key: &'a EncodedKey,
	versions: &'a Versions,
	source: usize,
	reverse: bool,
}

impl PartialEq for Head<'_> {
	fn eq(&self, other: &Self) -> bool {
		self.key == other.key
	}
}

impl Eq for Head<'_> {}

impl PartialOrd for Head<'_> {
	fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
		Some(self.cmp(other))
	}
}

impl Ord for Head<'_> {
	fn cmp(&self, other: &Self) -> Ordering {
		let order = self.key.cmp(other.key);
		if self.reverse {
			order
		} else {
			order.reverse()
		}
	}
}

pub(super) struct MergedRows<'a, I>
where
	I: Iterator<Item = (&'a EncodedKey, &'a Versions)>,
{
	iters: Vec<I>,
	heads: BinaryHeap<Head<'a>>,
	reverse: bool,
	group: Vec<&'a Versions>,
}

impl<'a, I> MergedRows<'a, I>
where
	I: Iterator<Item = (&'a EncodedKey, &'a Versions)>,
{
	pub fn new(mut iters: Vec<I>, reverse: bool) -> Self {
		let mut heads = BinaryHeap::with_capacity(iters.len());
		for (source, iter) in iters.iter_mut().enumerate() {
			if let Some((key, versions)) = iter.next() {
				heads.push(Head {
					key,
					versions,
					source,
					reverse,
				});
			}
		}
		Self {
			iters,
			heads,
			reverse,
			group: Vec::new(),
		}
	}

	fn advance(&mut self, source: usize) {
		if let Some((key, versions)) = self.iters[source].next() {
			self.heads.push(Head {
				key,
				versions,
				source,
				reverse: self.reverse,
			});
		}
	}

	pub fn next_group(&mut self) -> Option<(&'a EncodedKey, &[&'a Versions])> {
		let first = self.heads.pop()?;
		let target = first.key;
		self.group.clear();
		self.group.push(first.versions);
		self.advance(first.source);
		while self.heads.peek().is_some_and(|head| head.key == target) {
			let head = self.heads.pop().expect("a peeked heap yields");
			self.group.push(head.versions);
			self.advance(head.source);
		}
		Some((target, &self.group))
	}
}

#[derive(Default)]
pub(super) struct ActiveRows {
	rows: RowMap,
}

impl ActiveRows {
	pub fn rows(&self) -> &RowMap {
		&self.rows
	}

	pub fn insert(&mut self, key: EncodedKey, version: CommitVersion, value: Value) {
		self.rows.insert(key, version, value);
	}

	pub fn min_version(&self) -> Option<CommitVersion> {
		self.rows.min_version()
	}

	pub fn compact(&mut self, dropped: &HashMap<EncodedKey, HashSet<CommitVersion>>) -> Vec<Removed> {
		self.rows.remove(dropped)
	}

	pub fn bytes(&self) -> u64 {
		self.rows.bytes()
	}

	pub fn is_empty(&self) -> bool {
		self.rows.is_empty()
	}

	pub fn close(self) -> ClosedRows {
		reifydb_assertions! {
			assert!(
				!self.rows.is_empty(),
				"closing an empty active map mints a closed map with no version range, and the flush \
				 orders closed maps by that range"
			);
		}
		ClosedRows {
			rows: self.rows.packed(),
		}
	}
}

#[derive(Clone)]
pub(super) struct ClosedRows {
	rows: RowMap,
}

impl ClosedRows {
	pub fn rows(&self) -> &RowMap {
		&self.rows
	}

	pub fn min_version(&self) -> CommitVersion {
		self.rows.min_version().expect("a closed map holds at least one row")
	}

	pub fn compact(&mut self, dropped: &HashMap<EncodedKey, HashSet<CommitVersion>>) -> Vec<Removed> {
		self.rows.remove(dropped)
	}
}
