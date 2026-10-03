// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, btree_map},
	ops::{Bound, RangeBounds},
};

use reifydb_codec::{
	key::{encode_u8, encoded::EncodedKey},
	row::bytes::EncodedBytes,
};
use reifydb_value::byte_size::ByteSize;

use crate::{delta::RemoveVisibility, key::tag::KeyTag};

#[derive(Debug, Clone)]
pub enum PendingWrite {
	Set(EncodedBytes),
	Remove {
		announce: RemoveVisibility,
	},
}

#[derive(Debug, Default, Clone)]
pub struct Pending {
	state: BTreeMap<EncodedKey, PendingWrite>,
	entries: Vec<(EncodedKey, PendingWrite)>,
	index: BTreeMap<EncodedKey, usize>,
	pre: BTreeMap<EncodedKey, Option<ByteSize>>,
}

impl Pending {
	pub fn new() -> Self {
		Self {
			state: BTreeMap::new(),
			entries: Vec::new(),
			index: BTreeMap::new(),
			pre: BTreeMap::new(),
		}
	}

	pub fn classify(&mut self, key: EncodedKey, pre: Option<ByteSize>) {
		self.pre.entry(key).or_insert(pre);
	}

	pub fn is_classified(&self, key: &EncodedKey) -> bool {
		self.pre.contains_key(key)
	}

	pub fn pre_at(&self, key: &EncodedKey) -> Option<Option<ByteSize>> {
		self.pre.get(key).copied()
	}

	fn put(&mut self, key: EncodedKey, write: PendingWrite) {
		if is_state_key(&key) {
			self.state.insert(key, write);
			return;
		}
		if let Some(&slot) = self.index.get(&key) {
			self.entries[slot].1 = write;
			return;
		}
		self.index.insert(key.clone(), self.entries.len());
		self.entries.push((key, write));
	}

	pub fn write_at(&self, key: &EncodedKey) -> Option<&PendingWrite> {
		if is_state_key(key) {
			self.state.get(key)
		} else {
			self.index.get(key).map(|slot| &self.entries[*slot].1)
		}
	}

	pub fn insert(&mut self, key: EncodedKey, value: EncodedBytes) {
		self.put(key, PendingWrite::Set(value));
	}

	pub fn remove(&mut self, key: EncodedKey) {
		self.put(
			key,
			PendingWrite::Remove {
				announce: RemoveVisibility::Announced,
			},
		);
	}

	pub fn insert_batch(&mut self, keys: Vec<EncodedKey>, values: Vec<EncodedBytes>) {
		assert_eq!(keys.len(), values.len(), "Pending::insert_batch keys/values length mismatch");
		for (k, v) in keys.into_iter().zip(values) {
			self.put(k, PendingWrite::Set(v));
		}
	}

	pub fn remove_batch(&mut self, keys: Vec<EncodedKey>) {
		for k in keys {
			self.put(
				k,
				PendingWrite::Remove {
					announce: RemoveVisibility::Announced,
				},
			);
		}
	}

	pub fn remove_silent(&mut self, key: EncodedKey) {
		self.put(
			key,
			PendingWrite::Remove {
				announce: RemoveVisibility::Silent,
			},
		);
	}

	pub fn remove_unobserved(&mut self, key: EncodedKey) {
		self.put(
			key,
			PendingWrite::Remove {
				announce: RemoveVisibility::Unobserved,
			},
		);
	}

	pub fn contains_key(&self, key: &EncodedKey) -> bool {
		self.write_at(key).is_some()
	}

	pub fn is_empty(&self) -> bool {
		self.state.is_empty() && self.entries.is_empty()
	}

	pub fn len(&self) -> usize {
		self.state.len() + self.entries.len()
	}

	pub fn rows_ordered(&self) -> impl DoubleEndedIterator<Item = (&EncodedKey, &PendingWrite)> + '_ {
		self.entries.iter().map(|(k, w)| (k, w))
	}

	pub fn state_sorted(&self) -> impl DoubleEndedIterator<Item = (&EncodedKey, &PendingWrite)> + '_ {
		self.state.iter()
	}

	pub fn iter_sorted(&self) -> impl DoubleEndedIterator<Item = (&EncodedKey, &PendingWrite)> + '_ {
		self.range(..)
	}

	pub fn range<R>(&self, range: R) -> impl DoubleEndedIterator<Item = (&EncodedKey, &PendingWrite)> + '_
	where
		R: RangeBounds<EncodedKey>,
	{
		let block_start = [encode_u8(KeyTag::OperatorState as u8)];
		let block_end = [block_start[0] + 1];
		let start = range.start_bound().map(EncodedKey::as_slice);
		let end = range.end_bound().map(EncodedKey::as_slice);
		let below = self.row_segment(start, end_before(end, &block_start));
		let above = self.row_segment(start_from(start, &block_end), end);
		let state = self.state.range::<EncodedKey, _>((range.start_bound(), range.end_bound()));
		below.into_iter()
			.flatten()
			.map(|(k, slot)| (k, &self.entries[*slot].1))
			.chain(state)
			.chain(above.into_iter().flatten().map(|(k, slot)| (k, &self.entries[*slot].1)))
	}

	pub fn collect_range<R>(&self, range: R, out: &mut BTreeMap<EncodedKey, PendingWrite>)
	where
		R: RangeBounds<EncodedKey>,
	{
		for (key, write) in self.range(range) {
			out.insert(key.clone(), write.clone());
		}
	}

	pub fn collect_range_back<R>(&self, range: R, limit: usize, out: &mut BTreeMap<EncodedKey, PendingWrite>)
	where
		R: RangeBounds<EncodedKey>,
	{
		for (key, write) in self.range(range).rev().take(limit) {
			out.insert(key.clone(), write.clone());
		}
	}

	fn row_segment(
		&self,
		start: Bound<&[u8]>,
		end: Bound<&[u8]>,
	) -> Option<btree_map::Range<'_, EncodedKey, usize>> {
		holds_keys(start, end).then(|| self.index.range::<[u8], _>((start, end)))
	}
}

fn is_state_key(key: &[u8]) -> bool {
	KeyTag::of(key) == Some(KeyTag::OperatorState)
}

fn end_before<'a>(end: Bound<&'a [u8]>, limit: &'a [u8]) -> Bound<&'a [u8]> {
	match end {
		Bound::Included(key) if key < limit => Bound::Included(key),
		Bound::Excluded(key) if key <= limit => Bound::Excluded(key),
		_ => Bound::Excluded(limit),
	}
}

fn start_from<'a>(start: Bound<&'a [u8]>, limit: &'a [u8]) -> Bound<&'a [u8]> {
	match start {
		Bound::Included(key) if key >= limit => Bound::Included(key),
		Bound::Excluded(key) if key >= limit => Bound::Excluded(key),
		_ => Bound::Included(limit),
	}
}

fn holds_keys(start: Bound<&[u8]>, end: Bound<&[u8]>) -> bool {
	match (start, end) {
		(Bound::Included(start), Bound::Included(end)) => start <= end,
		(Bound::Included(start) | Bound::Excluded(start), Bound::Included(end) | Bound::Excluded(end)) => {
			start < end
		}
		_ => true,
	}
}

#[cfg(test)]
pub mod tests {
	use std::vec;

	use reifydb_value::util::cowvec::CowVec;

	use super::*;

	fn make_key(s: &str) -> EncodedKey {
		EncodedKey::new(s.as_bytes())
	}

	fn make_value(s: &str) -> EncodedBytes {
		EncodedBytes(CowVec::new(s.as_bytes().to_vec()))
	}

	fn set_value<'a>(pending: &'a Pending, key: &EncodedKey) -> Option<&'a EncodedBytes> {
		match pending.write_at(key) {
			Some(PendingWrite::Set(value)) => Some(value),
			_ => None,
		}
	}

	fn removed(pending: &Pending, key: &EncodedKey) -> bool {
		matches!(pending.write_at(key), Some(PendingWrite::Remove { .. }))
	}

	fn back_page(pending: &Pending, limit: usize) -> Vec<(EncodedKey, PendingWrite)> {
		// Every back-page assertion below wants the whole keyspace, so the range is fixed here and only the
		// limit varies.
		let mut out = BTreeMap::new();
		pending.collect_range_back(.., limit, &mut out);
		out.into_iter().collect()
	}

	#[test]
	fn a_back_page_returns_the_greatest_keys_of_the_range() {
		// The caller walks down from the greatest key looking for the last live row. A page taken from the
		// low end hands it keys it has already passed, so it reports a last row that is not the last.
		let mut pending = Pending::new();
		for n in 1..=5 {
			pending.insert(make_key(&format!("k{n:02}")), make_value("v"));
		}

		let keys: Vec<EncodedKey> = back_page(&pending, 2).into_iter().map(|(key, _)| key).collect();

		assert_eq!(keys, vec![make_key("k04"), make_key("k05")]);
	}

	#[test]
	fn a_back_page_wider_than_the_range_returns_every_key() {
		// The caller stops paging when a page comes back short. A page that drops a key at a limit it never
		// reached ends the walk early and reports no last row at all.
		let mut pending = Pending::new();
		for n in 1..=3 {
			pending.insert(make_key(&format!("k{n:02}")), make_value("v"));
		}

		let keys: Vec<EncodedKey> = back_page(&pending, 99).into_iter().map(|(key, _)| key).collect();

		assert_eq!(keys, vec![make_key("k01"), make_key("k02"), make_key("k03")]);
	}

	#[test]
	fn test_insert_single_write() {
		let mut pending = Pending::new();
		let key = make_key("key1");
		let value = make_value("value1");

		pending.insert(key.clone(), value.clone());

		assert_eq!(set_value(&pending, &key), Some(&value));
		assert!(!removed(&pending, &key));
		assert!(pending.contains_key(&key));
	}

	#[test]
	fn test_insert_multiple_writes() {
		let mut pending = Pending::new();

		pending.insert(make_key("key1"), make_value("value1"));
		pending.insert(make_key("key2"), make_value("value2"));
		pending.insert(make_key("key3"), make_value("value3"));

		assert_eq!(set_value(&pending, &make_key("key1")), Some(&make_value("value1")));
		assert_eq!(set_value(&pending, &make_key("key2")), Some(&make_value("value2")));
		assert_eq!(set_value(&pending, &make_key("key3")), Some(&make_value("value3")));
	}

	#[test]
	fn test_insert_overwrites_existing_key() {
		let mut pending = Pending::new();
		let key = make_key("key1");

		pending.insert(key.clone(), make_value("value1"));
		pending.insert(key.clone(), make_value("value2"));

		assert_eq!(set_value(&pending, &key), Some(&make_value("value2")));
	}

	#[test]
	fn test_remove_operation() {
		let mut pending = Pending::new();
		let key = make_key("key1");

		pending.remove(key.clone());

		assert!(removed(&pending, &key));
		assert!(pending.contains_key(&key));
		assert_eq!(set_value(&pending, &key), None);
	}

	#[test]
	fn test_write_then_remove() {
		let mut pending = Pending::new();
		let key = make_key("key1");

		pending.insert(key.clone(), make_value("value1"));
		assert_eq!(set_value(&pending, &key), Some(&make_value("value1")));

		pending.remove(key.clone());
		assert!(removed(&pending, &key));
		assert_eq!(set_value(&pending, &key), None);
	}

	#[test]
	fn test_remove_then_write() {
		let mut pending = Pending::new();
		let key = make_key("key1");

		pending.remove(key.clone());
		assert!(removed(&pending, &key));

		pending.insert(key.clone(), make_value("value1"));
		assert!(!removed(&pending, &key));
		assert_eq!(set_value(&pending, &key), Some(&make_value("value1")));
	}

	#[test]
	fn test_iter_sorted_order() {
		let mut pending = Pending::new();

		pending.insert(make_key("zebra"), make_value("z"));
		pending.insert(make_key("apple"), make_value("a"));
		pending.insert(make_key("mango"), make_value("m"));

		let keys: Vec<_> = pending.iter_sorted().map(|(k, _)| k.clone()).collect();

		assert_eq!(keys, vec![make_key("apple"), make_key("mango"), make_key("zebra")]);
	}

	#[test]
	fn test_range_query() {
		let mut pending = Pending::new();

		pending.insert(make_key("a"), make_value("1"));
		pending.insert(make_key("b"), make_value("2"));
		pending.insert(make_key("c"), make_value("3"));
		pending.insert(make_key("d"), make_value("4"));

		let range_keys: Vec<_> = pending.range(make_key("b")..make_key("d")).map(|(k, _)| k.clone()).collect();

		assert_eq!(range_keys, vec![make_key("b"), make_key("c")]);
	}

	#[test]
	fn test_range_query_inclusive() {
		let mut pending = Pending::new();

		pending.insert(make_key("a"), make_value("1"));
		pending.insert(make_key("b"), make_value("2"));
		pending.insert(make_key("c"), make_value("3"));

		let range_keys: Vec<_> = pending.range(make_key("a")..=make_key("c")).map(|(k, _)| k.clone()).collect();

		assert_eq!(range_keys, vec![make_key("a"), make_key("b"), make_key("c")]);
	}

	#[test]
	fn test_range_query_empty() {
		let mut pending = Pending::new();

		pending.insert(make_key("a"), make_value("1"));
		pending.insert(make_key("z"), make_value("2"));

		let range_keys: Vec<_> = pending.range(make_key("m")..make_key("n")).map(|(k, _)| k.clone()).collect();

		assert!(range_keys.is_empty());
	}

	#[test]
	fn test_contains_key() {
		let mut pending = Pending::new();

		pending.insert(make_key("key1"), make_value("value1"));
		pending.remove(make_key("key2"));

		assert!(pending.contains_key(&make_key("key1")));
		assert!(pending.contains_key(&make_key("key2"))); // Remove is also "contained"
		assert!(!pending.contains_key(&make_key("key3")));
	}

	#[test]
	fn test_mixed_writes_and_removes() {
		let mut pending = Pending::new();

		pending.insert(make_key("write1"), make_value("v1"));
		pending.remove(make_key("remove1"));
		pending.insert(make_key("write2"), make_value("v2"));
		pending.remove(make_key("remove2"));

		assert_eq!(set_value(&pending, &make_key("write1")), Some(&make_value("v1")));
		assert_eq!(set_value(&pending, &make_key("write2")), Some(&make_value("v2")));
		assert!(removed(&pending, &make_key("remove1")));
		assert!(removed(&pending, &make_key("remove2")));
		assert_eq!(set_value(&pending, &make_key("remove1")), None);
		assert_eq!(set_value(&pending, &make_key("remove2")), None);
	}

	#[test]
	fn test_is_empty() {
		let mut pending = Pending::new();
		assert!(pending.is_empty());

		pending.insert(make_key("key1"), make_value("value1"));
		assert!(!pending.is_empty());

		let mut tombstones = Pending::new();
		tombstones.remove(make_key("key1"));
		assert!(!tombstones.is_empty());
	}

	#[test]
	fn test_iter_sorted_includes_removes() {
		let mut pending = Pending::new();

		pending.insert(make_key("b"), make_value("2"));
		pending.remove(make_key("a"));
		pending.insert(make_key("c"), make_value("3"));

		let items: Vec<_> = pending.iter_sorted().collect();
		assert_eq!(items.len(), 3);

		assert_eq!(items[0].0, &make_key("a"));
		assert!(matches!(items[0].1, PendingWrite::Remove { .. }));

		assert_eq!(items[1].0, &make_key("b"));
		assert!(matches!(items[1].1, PendingWrite::Set(_)));

		assert_eq!(items[2].0, &make_key("c"));
		assert!(matches!(items[2].1, PendingWrite::Set(_)));
	}
}
