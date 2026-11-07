// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, btree_map},
	iter::once,
	mem::take,
	ops::{Bound, RangeBounds},
};

use reifydb_codec::{
	key::{encode_u8, encoded::EncodedKey},
	row::bytes::EncodedBytes,
};
use reifydb_value::{
	byte_size::ByteSize,
	value::{partition::Partition, row_number::RowNumber},
};

use crate::{delta::RemoveVisibility, interface::catalog::id::QueueId, key::tag::KeyTag};

#[derive(Debug, Clone)]
pub enum PendingWrite {
	Set(EncodedBytes),
	Remove {
		announce: RemoveVisibility,
	},
}

#[derive(Debug, Clone, PartialEq)]
pub struct PendingQueueItem {
	pub queue: QueueId,
	pub partition: Partition,
	pub key_hash: Option<u64>,
	pub row_number: RowNumber,
	pub encoded: EncodedBytes,
}

#[derive(Debug, Default, Clone)]
pub struct Pending {
	state: BTreeMap<EncodedKey, PendingWrite>,
	entries: Vec<(EncodedKey, PendingWrite)>,
	index: BTreeMap<EncodedKey, usize>,
	pre: BTreeMap<EncodedKey, Option<ByteSize>>,
	queue_items: Vec<PendingQueueItem>,
	below: Vec<Pending>,
}

impl Pending {
	pub fn new() -> Self {
		Self {
			state: BTreeMap::new(),
			entries: Vec::new(),
			index: BTreeMap::new(),
			pre: BTreeMap::new(),
			queue_items: Vec::new(),
			below: Vec::new(),
		}
	}

	pub fn push_layer(&mut self) {
		let mut below = take(&mut self.below);
		below.push(take(self));
		self.below = below;
	}

	pub fn into_layers(mut self) -> Vec<Pending> {
		let mut layers = take(&mut self.below);
		layers.push(self);
		layers
	}

	pub fn merged(layers: Vec<Pending>) -> Pending {
		let mut merged = Pending::new();
		for layer in layers.into_iter().flat_map(Pending::into_layers) {
			merged.state.extend(layer.state);
			merged.put(layer.entries);
			for (key, pre) in layer.pre {
				merged.pre.entry(key).or_insert(pre);
			}
			merged.queue_items.extend(layer.queue_items);
		}
		merged
	}

	fn layers(&self) -> impl DoubleEndedIterator<Item = &Pending> + '_ {
		self.below.iter().chain(once(self))
	}

	pub fn admit_queue_items(&mut self, items: impl IntoIterator<Item = PendingQueueItem>) {
		self.queue_items.extend(items);
	}

	pub fn queue_items(&self) -> &[PendingQueueItem] {
		&self.queue_items
	}

	pub fn classify(&mut self, key: EncodedKey, pre: Option<ByteSize>) {
		self.pre.entry(key).or_insert(pre);
	}

	pub fn pre_at(&self, key: &EncodedKey) -> Option<Option<ByteSize>> {
		self.pre.get(key).copied()
	}

	pub fn put(&mut self, writes: Vec<(EncodedKey, PendingWrite)>) {
		for (key, write) in writes {
			if is_state_key(&key) {
				self.state.insert(key, write);
				continue;
			}
			if let Some(&slot) = self.index.get(&key) {
				self.entries[slot].1 = write;
				continue;
			}
			self.index.insert(key.clone(), self.entries.len());
			self.entries.push((key, write));
		}
	}

	pub fn write_at(&self, key: &EncodedKey) -> Option<&PendingWrite> {
		self.layers().rev().find_map(|layer| layer.own_write_at(key))
	}

	fn own_write_at(&self, key: &EncodedKey) -> Option<&PendingWrite> {
		if is_state_key(key) {
			self.state.get(key)
		} else {
			self.index.get(key).map(|slot| &self.entries[*slot].1)
		}
	}

	pub fn insert(&mut self, keys: Vec<EncodedKey>, values: Vec<EncodedBytes>) {
		assert_eq!(keys.len(), values.len(), "Pending::insert keys/values length mismatch");
		self.put(keys.into_iter().zip(values).map(|(k, v)| (k, PendingWrite::Set(v))).collect());
	}

	pub fn remove(&mut self, keys: Vec<EncodedKey>) {
		self.put(keys
			.into_iter()
			.map(|k| {
				(
					k,
					PendingWrite::Remove {
						announce: RemoveVisibility::Announced,
					},
				)
			})
			.collect());
	}

	pub fn remove_silent(&mut self, key: EncodedKey) {
		self.put(vec![(
			key,
			PendingWrite::Remove {
				announce: RemoveVisibility::Silent,
			},
		)]);
	}

	pub fn remove_unobserved(&mut self, key: EncodedKey) {
		self.put(vec![(
			key,
			PendingWrite::Remove {
				announce: RemoveVisibility::Unobserved,
			},
		)]);
	}

	pub fn contains_key(&self, key: &EncodedKey) -> bool {
		self.write_at(key).is_some()
	}

	pub fn is_empty(&self) -> bool {
		self.layers().all(|layer| layer.state.is_empty() && layer.entries.is_empty())
	}

	pub fn len(&self) -> usize {
		self.layers().map(|layer| layer.state.len() + layer.entries.len()).sum()
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
		for layer in self.layers() {
			for (key, write) in layer.range((range.start_bound(), range.end_bound())) {
				out.insert(key.clone(), write.clone());
			}
		}
	}

	pub fn collect_range_back<R>(&self, range: R, limit: usize, out: &mut BTreeMap<EncodedKey, PendingWrite>)
	where
		R: RangeBounds<EncodedKey>,
	{
		let mut merged = BTreeMap::new();
		for layer in self.layers() {
			for (key, write) in layer.range((range.start_bound(), range.end_bound())).rev().take(limit) {
				merged.insert(key.clone(), write.clone());
			}
		}
		out.extend(merged.into_iter().rev().take(limit));
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
	use std::{mem::take, vec};

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
			pending.insert(vec![make_key(&format!("k{n:02}"))], vec![make_value("v")]);
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
			pending.insert(vec![make_key(&format!("k{n:02}"))], vec![make_value("v")]);
		}

		let keys: Vec<EncodedKey> = back_page(&pending, 99).into_iter().map(|(key, _)| key).collect();

		assert_eq!(keys, vec![make_key("k01"), make_key("k02"), make_key("k03")]);
	}

	#[test]
	fn test_insert_single_write() {
		let mut pending = Pending::new();
		let key = make_key("key1");
		let value = make_value("value1");

		pending.insert(vec![key.clone()], vec![value.clone()]);

		assert_eq!(set_value(&pending, &key), Some(&value));
		assert!(!removed(&pending, &key));
		assert!(pending.contains_key(&key));
	}

	#[test]
	fn test_insert_multiple_writes() {
		let mut pending = Pending::new();

		pending.insert(vec![make_key("key1")], vec![make_value("value1")]);
		pending.insert(vec![make_key("key2")], vec![make_value("value2")]);
		pending.insert(vec![make_key("key3")], vec![make_value("value3")]);

		assert_eq!(set_value(&pending, &make_key("key1")), Some(&make_value("value1")));
		assert_eq!(set_value(&pending, &make_key("key2")), Some(&make_value("value2")));
		assert_eq!(set_value(&pending, &make_key("key3")), Some(&make_value("value3")));
	}

	#[test]
	fn test_insert_overwrites_existing_key() {
		let mut pending = Pending::new();
		let key = make_key("key1");

		pending.insert(vec![key.clone()], vec![make_value("value1")]);
		pending.insert(vec![key.clone()], vec![make_value("value2")]);

		assert_eq!(set_value(&pending, &key), Some(&make_value("value2")));
	}

	#[test]
	fn test_remove_operation() {
		let mut pending = Pending::new();
		let key = make_key("key1");

		pending.remove(vec![key.clone()]);

		assert!(removed(&pending, &key));
		assert!(pending.contains_key(&key));
		assert_eq!(set_value(&pending, &key), None);
	}

	#[test]
	fn test_write_then_remove() {
		let mut pending = Pending::new();
		let key = make_key("key1");

		pending.insert(vec![key.clone()], vec![make_value("value1")]);
		assert_eq!(set_value(&pending, &key), Some(&make_value("value1")));

		pending.remove(vec![key.clone()]);
		assert!(removed(&pending, &key));
		assert_eq!(set_value(&pending, &key), None);
	}

	#[test]
	fn test_remove_then_write() {
		let mut pending = Pending::new();
		let key = make_key("key1");

		pending.remove(vec![key.clone()]);
		assert!(removed(&pending, &key));

		pending.insert(vec![key.clone()], vec![make_value("value1")]);
		assert!(!removed(&pending, &key));
		assert_eq!(set_value(&pending, &key), Some(&make_value("value1")));
	}

	#[test]
	fn test_iter_sorted_order() {
		let mut pending = Pending::new();

		pending.insert(vec![make_key("zebra")], vec![make_value("z")]);
		pending.insert(vec![make_key("apple")], vec![make_value("a")]);
		pending.insert(vec![make_key("mango")], vec![make_value("m")]);

		let keys: Vec<_> = pending.iter_sorted().map(|(k, _)| k.clone()).collect();

		assert_eq!(keys, vec![make_key("apple"), make_key("mango"), make_key("zebra")]);
	}

	#[test]
	fn test_range_query() {
		let mut pending = Pending::new();

		pending.insert(vec![make_key("a")], vec![make_value("1")]);
		pending.insert(vec![make_key("b")], vec![make_value("2")]);
		pending.insert(vec![make_key("c")], vec![make_value("3")]);
		pending.insert(vec![make_key("d")], vec![make_value("4")]);

		let range_keys: Vec<_> = pending.range(make_key("b")..make_key("d")).map(|(k, _)| k.clone()).collect();

		assert_eq!(range_keys, vec![make_key("b"), make_key("c")]);
	}

	#[test]
	fn test_range_query_inclusive() {
		let mut pending = Pending::new();

		pending.insert(vec![make_key("a")], vec![make_value("1")]);
		pending.insert(vec![make_key("b")], vec![make_value("2")]);
		pending.insert(vec![make_key("c")], vec![make_value("3")]);

		let range_keys: Vec<_> = pending.range(make_key("a")..=make_key("c")).map(|(k, _)| k.clone()).collect();

		assert_eq!(range_keys, vec![make_key("a"), make_key("b"), make_key("c")]);
	}

	#[test]
	fn test_range_query_empty() {
		let mut pending = Pending::new();

		pending.insert(vec![make_key("a")], vec![make_value("1")]);
		pending.insert(vec![make_key("z")], vec![make_value("2")]);

		let range_keys: Vec<_> = pending.range(make_key("m")..make_key("n")).map(|(k, _)| k.clone()).collect();

		assert!(range_keys.is_empty());
	}

	#[test]
	fn test_contains_key() {
		let mut pending = Pending::new();

		pending.insert(vec![make_key("key1")], vec![make_value("value1")]);
		pending.remove(vec![make_key("key2")]);

		assert!(pending.contains_key(&make_key("key1")));
		assert!(pending.contains_key(&make_key("key2"))); // Remove is also "contained"
		assert!(!pending.contains_key(&make_key("key3")));
	}

	#[test]
	fn test_mixed_writes_and_removes() {
		let mut pending = Pending::new();

		pending.insert(vec![make_key("write1")], vec![make_value("v1")]);
		pending.remove(vec![make_key("remove1")]);
		pending.insert(vec![make_key("write2")], vec![make_value("v2")]);
		pending.remove(vec![make_key("remove2")]);

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

		pending.insert(vec![make_key("key1")], vec![make_value("value1")]);
		assert!(!pending.is_empty());

		let mut tombstones = Pending::new();
		tombstones.remove(vec![make_key("key1")]);
		assert!(!tombstones.is_empty());
	}

	#[test]
	fn test_queue_items_survive_taking_the_pending_and_keep_their_order() {
		// The committer reads items from the taken pending, so an item lost here is a row workers never see.
		let item = |row: u64| PendingQueueItem {
			queue: QueueId(1),
			partition: Partition(0),
			key_hash: None,
			row_number: RowNumber(row),
			encoded: make_value("item"),
		};
		let mut pending = Pending::new();
		pending.admit_queue_items([item(1), item(2)]);
		pending.admit_queue_items([item(3)]);
		let taken = take(&mut pending);
		let rows: Vec<u64> = taken.queue_items().iter().map(|item| item.row_number.0).collect();
		assert_eq!(rows, vec![1, 2, 3]);
		assert!(pending.queue_items().is_empty());
	}

	#[test]
	fn test_iter_sorted_includes_removes() {
		let mut pending = Pending::new();

		pending.insert(vec![make_key("b")], vec![make_value("2")]);
		pending.remove(vec![make_key("a")]);
		pending.insert(vec![make_key("c")], vec![make_value("3")]);

		let items: Vec<_> = pending.iter_sorted().collect();
		assert_eq!(items.len(), 3);

		assert_eq!(items[0].0, &make_key("a"));
		assert!(matches!(items[0].1, PendingWrite::Remove { .. }));

		assert_eq!(items[1].0, &make_key("b"));
		assert!(matches!(items[1].1, PendingWrite::Set(_)));

		assert_eq!(items[2].0, &make_key("c"));
		assert!(matches!(items[2].1, PendingWrite::Set(_)));
	}

	fn range_of(pending: &Pending) -> Vec<(EncodedKey, Option<EncodedBytes>)> {
		// Removes show as none so a test can tell a hidden key from a key that was never written.
		let mut out = BTreeMap::new();
		pending.collect_range(.., &mut out);
		out.into_iter()
			.map(|(key, write)| match write {
				PendingWrite::Set(value) => (key, Some(value)),
				PendingWrite::Remove {
					..
				} => (key, None),
			})
			.collect()
	}

	#[test]
	fn a_read_sees_every_layer_and_the_newest_layer_wins() {
		// A later source in one step must read what an earlier source wrote, and its own newer write over it;
		// otherwise an operator recomputes from stale state and its output for that source is wrong.
		let mut pending = Pending::new();
		pending.insert(vec![make_key("a"), make_key("b")], vec![make_value("a1"), make_value("b1")]);
		pending.push_layer();
		pending.insert(vec![make_key("a")], vec![make_value("a2")]);
		pending.remove(vec![make_key("b")]);
		pending.push_layer();
		pending.insert(vec![make_key("c")], vec![make_value("c3")]);

		assert_eq!(set_value(&pending, &make_key("a")), Some(&make_value("a2")));
		assert!(removed(&pending, &make_key("b")), "a remove in a newer layer must hide the older set");
		assert_eq!(set_value(&pending, &make_key("c")), Some(&make_value("c3")));
		assert!(pending.contains_key(&make_key("a")));
		assert!(!pending.is_empty());
		assert_eq!(
			range_of(&pending),
			vec![
				(make_key("a"), Some(make_value("a2"))),
				(make_key("b"), None),
				(make_key("c"), Some(make_value("c3"))),
			]
		);
	}

	#[test]
	fn a_back_page_across_layers_returns_the_greatest_keys_once() {
		// The last-row walk pages down from the greatest key; a layered page must equal the page of the same
		// writes made in one layer, otherwise the walk skips a key or sees a shadowed write as live.
		let writes: Vec<Vec<(&str, Option<&str>)>> = vec![
			vec![("k01", Some("1")), ("k05", Some("1")), ("k09", Some("1"))],
			vec![("k02", Some("2")), ("k09", None), ("k07", Some("2"))],
			vec![("k08", Some("3")), ("k05", Some("3"))],
		];
		let mut layered = Pending::new();
		let mut flat = Pending::new();
		for (index, layer) in writes.iter().enumerate() {
			if index > 0 {
				layered.push_layer();
			}
			for (key, value) in layer {
				for pending in [&mut layered, &mut flat] {
					match value {
						Some(value) => {
							pending.insert(vec![make_key(key)], vec![make_value(value)])
						}
						None => pending.remove(vec![make_key(key)]),
					}
				}
			}
		}

		for limit in [1, 2, 3, 4, 99] {
			let layered_page: Vec<(EncodedKey, bool)> = back_page(&layered, limit)
				.into_iter()
				.map(|(key, write)| (key, matches!(write, PendingWrite::Set(_))))
				.collect();
			let flat_page: Vec<(EncodedKey, bool)> = back_page(&flat, limit)
				.into_iter()
				.map(|(key, write)| (key, matches!(write, PendingWrite::Set(_))))
				.collect();
			assert_eq!(layered_page, flat_page, "limit {limit}");
		}
		let top: Vec<(EncodedKey, PendingWrite)> = back_page(&layered, 3);
		assert_eq!(
			top.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>(),
			vec![make_key("k07"), make_key("k08"), make_key("k09")]
		);
		assert!(matches!(top[2].1, PendingWrite::Remove { .. }), "k09 was removed in layer 2");
	}

	#[test]
	fn merged_keeps_the_last_write_of_each_key_and_the_first_pre() {
		// A merge that kept an earlier layer's write or a later layer's pre would commit a value or size the
		// fold never had.
		let first = {
			let mut layer = Pending::new();
			layer.insert(vec![make_key("k"), make_key("a")], vec![make_value("v1"), make_value("a1")]);
			layer.classify(make_key("k"), None);
			layer
		};
		let second = {
			let mut layer = Pending::new();
			layer.insert(vec![make_key("k")], vec![make_value("v2")]);
			layer.classify(make_key("k"), Some(ByteSize::from_bytes(9)));
			layer
		};

		let merged = Pending::merged(vec![first, second]);

		let rows: Vec<(EncodedKey, Option<EncodedBytes>)> = merged
			.rows_ordered()
			.map(|(key, write)| {
				let value = match write {
					PendingWrite::Set(value) => Some(value.clone()),
					PendingWrite::Remove {
						..
					} => None,
				};
				(key.clone(), value)
			})
			.collect();
		assert_eq!(
			rows,
			vec![(make_key("k"), Some(make_value("v2"))), (make_key("a"), Some(make_value("a1")))],
			"the last layer's value must win and the first write keeps its place"
		);
		assert_eq!(merged.pre_at(&make_key("k")), Some(None), "the first layer saw the committed pre");
	}

	#[test]
	fn into_layers_gives_each_layer_only_its_own_writes() {
		// Each layer becomes one commit stamped with its own source; a write leaking into a neighbour layer
		// lands at the wrong source and an exact reader pairs a row with a value it never had.
		let mut pending = Pending::new();
		pending.insert(vec![make_key("k")], vec![make_value("v1")]);
		pending.push_layer();
		pending.insert(vec![make_key("other")], vec![make_value("o2")]);
		pending.push_layer();
		pending.insert(vec![make_key("k")], vec![make_value("v3")]);

		let layers = pending.into_layers();

		assert_eq!(layers.len(), 3);
		assert_eq!(range_of(&layers[0]), vec![(make_key("k"), Some(make_value("v1")))]);
		assert_eq!(range_of(&layers[1]), vec![(make_key("other"), Some(make_value("o2")))]);
		assert_eq!(range_of(&layers[2]), vec![(make_key("k"), Some(make_value("v3")))]);
	}
}
