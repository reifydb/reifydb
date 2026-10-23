// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{hash::Hash, marker::PhantomData, slice::from_ref};

use reifydb_codec::row::operator::state::{OperatorState, decode};
use reifydb_core::{
	internal_err,
	key::operator::state::{GroupStateKey, IntoGroupStateKey, row_number_counter_key},
	metrics::heap::HeapSize,
	state::{batch::StateBatch, timer::StateStore},
};
use reifydb_value::{Result, value::row_number::RowNumber};

pub fn mint_row_numbers<S>(store: &mut S, count: u64) -> Result<RowNumber>
where
	S: StateStore + ?Sized,
{
	let seed = row_number_seed(store)?;
	store.state_set(vec![(row_number_counter_key(), (seed + count).encode_state()?)])?;
	Ok(RowNumber(seed))
}

#[derive(Debug, Default)]
pub struct RowNumberReserve {
	next: Option<u64>,
}

impl RowNumberReserve {
	pub fn take<S>(&mut self, store: &mut S) -> Result<RowNumber>
	where
		S: StateStore + ?Sized,
	{
		let next = match self.next {
			Some(next) => next,
			None => row_number_seed(store)?,
		};
		self.next = Some(next + 1);
		Ok(RowNumber(next))
	}

	pub fn commit<S>(self, store: &mut S) -> Result<()>
	where
		S: StateStore + ?Sized,
	{
		match self.next {
			Some(next) => store.state_set(vec![(row_number_counter_key(), next.encode_state()?)]),
			None => Ok(()),
		}
	}
}

fn row_number_seed<S>(store: &mut S) -> Result<u64>
where
	S: StateStore + ?Sized,
{
	match store.state_get(from_ref(&row_number_counter_key()))?.pop().flatten() {
		Some(row) => Ok(decode::<u64>(&row)?),
		None => Ok(1),
	}
}

pub fn get<K, V>(store: &mut dyn StateStore, keys: &[K]) -> Result<Vec<Option<V>>>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
	V: Clone + OperatorState + HeapSize,
{
	let encoded: Vec<GroupStateKey> = keys.iter().map(|key| key.into_group_state_key()).collect();
	let rows = store.state_get(&encoded)?;
	if rows.len() != encoded.len() {
		return internal_err!("a batch read answered {} slots for {} keys", rows.len(), encoded.len());
	}
	let mut values = Vec::with_capacity(rows.len());
	for row in rows {
		values.push(match row {
			Some(row) => Some(decode::<V>(&row)?),
			None => None,
		});
	}
	Ok(values)
}

pub fn get_classified<K, V>(store: &mut dyn StateStore, keys: &[K]) -> Result<Vec<Option<V>>>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
	V: Clone + OperatorState + HeapSize,
{
	let encoded: Vec<GroupStateKey> = keys.iter().map(|key| key.into_group_state_key()).collect();
	let rows = store.state_get(&encoded)?;
	if rows.len() != encoded.len() {
		return internal_err!("a batch read answered {} slots for {} keys", rows.len(), encoded.len());
	}
	let mut values = Vec::with_capacity(rows.len());
	for (key, row) in encoded.iter().zip(rows) {
		let (value, pre) = match row {
			Some(row) => (Some(decode::<V>(&row)?), Some(row.byte_size())),
			None => (None, None),
		};
		store.state_classify(key, pre);
		values.push(value);
	}
	Ok(values)
}

pub fn batch<K, V>(store: &mut dyn StateStore, keys: &[K]) -> Result<TypedBatch<V>>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
	V: Clone + OperatorState + HeapSize,
{
	let keys = keys.iter().map(|key| key.into_group_state_key()).collect();
	Ok(TypedBatch {
		batch: store.state_batch(keys)?,
		value: PhantomData,
	})
}

pub struct TypedBatch<V> {
	batch: StateBatch,
	value: PhantomData<V>,
}

impl<V: OperatorState> TypedBatch<V> {
	pub fn set(&mut self, slot: usize, value: &V) -> Result<()> {
		self.batch.set(slot, value.encode_state()?);
		Ok(())
	}

	pub fn value(&self, slot: usize) -> Result<Option<V>> {
		match self.batch.value(slot) {
			Some(row) => Ok(Some(decode::<V>(row)?)),
			None => Ok(None),
		}
	}

	pub fn remove(&mut self, slot: usize) {
		self.batch.remove(slot);
	}

	pub fn commit(self, store: &mut dyn StateStore) -> Result<()> {
		store.state_write_batch(self.batch)
	}
}

pub fn set<K, V>(store: &mut dyn StateStore, entries: Vec<(K, V)>) -> Result<()>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
	V: Clone + OperatorState + HeapSize,
{
	let mut rows = Vec::with_capacity(entries.len());
	for (key, value) in entries {
		rows.push((key.into_group_state_key(), value.encode_state()?));
	}
	store.state_set(rows)
}

pub fn remove<K>(store: &mut dyn StateStore, keys: &[K]) -> Result<()>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
{
	let encoded: Vec<GroupStateKey> = keys.iter().map(|key| key.into_group_state_key()).collect();
	store.state_remove(&encoded)
}

#[cfg(test)]
mod tests {
	use std::{collections::HashMap, ops::Bound};

	use reifydb_codec::{key::encoded::EncodedKey, row::pod::EncodedPodRow};
	use reifydb_core::{
		key::operator::state::{GroupId, GroupStateKey, GroupStateRange, KeyspaceMask, unmanaged_key},
		state::{
			batch::StateBatch,
			per_key,
			timer::{GroupSweep, TimerKind, TimerStore},
		},
	};
	use reifydb_macro::operator_state;
	use reifydb_value::{
		byte_size::ByteSize,
		value::{datetime::DateTime, row_number::RowNumber},
	};

	use super::*;
	use crate::operator::state::test_utils::test::inner_bytes;

	/// A bare `String` would read as some other group's prefix; this frames the tests' string keys
	/// the way an operator does.
	#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
	struct Key(String);

	impl Key {
		fn new(key: impl Into<String>) -> Self {
			Self(key.into())
		}
	}

	impl HeapSize for Key {
		fn heap_size(&self) -> usize {
			self.0.capacity()
		}
	}

	impl IntoGroupStateKey for &Key {
		fn into_group_state_key(self) -> GroupStateKey {
			unmanaged_key(self.0.as_bytes())
				.expect("a custom state key must be at most sixteen bytes")
				.into()
		}
	}

	#[operator_state]
	#[derive(Debug, Clone, Copy, Default, PartialEq)]
	struct Cell {
		value: i32,
	}

	impl HeapSize for Cell {
		fn heap_size(&self) -> usize {
			0
		}
	}

	fn cell(value: i32) -> Cell {
		Cell {
			value,
		}
	}

	#[derive(Default)]
	struct MockStore {
		data: HashMap<Vec<u8>, EncodedPodRow>,
		removes: usize,
		sets: usize,
		gets: usize,
		classifications: Vec<(Vec<u8>, Option<ByteSize>)>,
		// Settable clock so the persisted timestamp is observable; defaults to epoch.
		now: DateTime,
	}

	impl TimerStore for MockStore {
		fn arm_timer(&mut self, _due: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			unreachable!("the window engine never arms timers; only the shell above it does")
		}

		fn disarm_timer(&mut self, _due: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			unreachable!("the window engine never disarms timers; only the shell above it does")
		}

		fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
			Ok(None)
		}
	}

	impl StateStore for MockStore {
		fn state_get(&mut self, keys: &[GroupStateKey]) -> Result<Vec<Option<EncodedPodRow>>> {
			self.gets += 1;
			Ok(keys.iter().map(|key| self.data.get(key.as_slice()).cloned()).collect())
		}

		fn state_classify(&mut self, key: &GroupStateKey, pre: Option<ByteSize>) {
			self.classifications.push((key.as_slice().to_vec(), pre));
		}

		fn state_page_inner(
			&mut self,
			range: GroupStateRange,
			limit: Option<usize>,
		) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
			let range = inner_bytes(range);
			let after_start = |k: &[u8]| match &range.start {
				Bound::Included(s) => k >= s.as_bytes(),
				Bound::Excluded(s) => k > s.as_bytes(),
				Bound::Unbounded => true,
			};
			let before_end = |k: &[u8]| match &range.end {
				Bound::Included(e) => k <= e.as_bytes(),
				Bound::Excluded(e) => k < e.as_bytes(),
				Bound::Unbounded => true,
			};
			let mut matched: Vec<(Vec<u8>, EncodedPodRow)> = self
				.data
				.iter()
				.filter(|(k, _)| after_start(k) && before_end(k))
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect();
			matched.sort_by(|a, b| a.0.cmp(&b.0));
			if let Some(limit) = limit {
				matched.truncate(limit);
			}
			Ok(matched
				.into_iter()
				.map(|(k, b)| {
					let k = GroupStateKey::from_framed(EncodedKey::new(k))
						.expect("fake store holds an unframed state key");
					(k, b)
				})
				.collect())
		}

		fn get_or_create_row_numbers(
			&mut self,
			_group: GroupId,
			keys: &[EncodedKey],
		) -> Result<Vec<(RowNumber, bool)>> {
			Ok(keys.iter().enumerate().map(|(i, _)| (RowNumber(i as u64 + 1), true)).collect())
		}

		fn get_or_create_row_numbers_for_groups(
			&mut self,
			groups: &[GroupId],
		) -> Result<Vec<(RowNumber, bool)>> {
			Ok(groups.iter().enumerate().map(|(i, _)| (RowNumber(i as u64 + 1), true)).collect())
		}

		fn get_row_numbers(&mut self, _group: GroupId, keys: &[EncodedKey]) -> Result<Vec<Option<RowNumber>>> {
			Ok(keys.iter().enumerate().map(|(i, _)| Some(RowNumber(i as u64 + 1))).collect())
		}

		fn state_remove(&mut self, keys: &[GroupStateKey]) -> Result<()> {
			self.removes += 1;
			for key in keys {
				self.data.remove(key.as_slice());
			}
			Ok(())
		}

		fn state_batch(&mut self, keys: Vec<GroupStateKey>) -> Result<StateBatch> {
			per_key::read_batch(self, keys)
		}

		fn state_write_batch(&mut self, batch: StateBatch) -> Result<()> {
			per_key::write_batch(self, batch)
		}

		fn state_set(&mut self, rows: Vec<(GroupStateKey, EncodedPodRow)>) -> Result<()> {
			self.sets += 1;
			for (key, payload) in rows {
				self.data.insert(key.as_slice().to_vec(), payload);
			}
			Ok(())
		}

		fn group_sweep(
			&mut self,
			groups: &[GroupId],
			limit: usize,
			keyspaces: KeyspaceMask,
		) -> Result<GroupSweep> {
			per_key::group_sweep(self, groups, limit, keyspaces)
		}

		fn remove_row_number(&mut self, _group: GroupId, _keys: &[EncodedKey]) -> Result<()> {
			Ok(())
		}

		fn remove_row_number_for_group(&mut self, _groups: &[GroupId]) -> Result<()> {
			Ok(())
		}

		fn written_at(&self) -> DateTime {
			self.now
		}
	}

	#[test]
	fn set_reaches_the_store_without_waiting_for_flush() {
		// Nothing buffers a pending write, so the value must be durable the moment set returns.
		let mut store = MockStore::default();

		set(&mut store, vec![(Key::new("a"), cell(7))]).unwrap();

		assert_eq!(store.sets, 1, "set must issue exactly one state_set");
		assert!(!store.data.is_empty(), "the value must be in the store before any flush");
		assert_eq!(get::<_, Cell>(&mut store, from_ref(&Key::new("a"))).unwrap().remove(0), Some(cell(7)));
	}

	#[test]
	fn get_reads_through_to_the_store_every_time() {
		// A cached second read would let another writer's value go unseen.
		let mut store = MockStore::default();
		set(&mut store, vec![(Key::new("a"), cell(1))]).unwrap();

		get::<_, Cell>(&mut store, from_ref(&Key::new("a"))).unwrap().remove(0);
		get::<_, Cell>(&mut store, from_ref(&Key::new("a"))).unwrap().remove(0);

		assert_eq!(store.gets, 2, "each get must be served by the store, never from residency");
	}

	#[test]
	fn a_miss_consults_the_store_rather_than_proving_absence() {
		// Absence is no longer answerable from a filter; every miss must be a real store read.
		let mut store = MockStore::default();

		assert_eq!(get::<_, Cell>(&mut store, from_ref(&Key::new("absent"))).unwrap().remove(0), None);
		assert_eq!(store.gets, 1, "the miss must have consulted the store");
	}

	#[test]
	fn a_read_leaves_the_row_in_the_store_for_the_next_reader() {
		// The read half of load-mutate-persist must not consume the row, or the base value is lost.
		let mut store = MockStore::default();
		set(&mut store, vec![(Key::new("a"), cell(3))]).unwrap();

		assert_eq!(get::<_, Cell>(&mut store, from_ref(&Key::new("a"))).unwrap().remove(0), Some(cell(3)));
		assert_eq!(store.removes, 0, "a read must not issue a state_remove");
		assert_eq!(get::<_, Cell>(&mut store, from_ref(&Key::new("a"))).unwrap().remove(0), Some(cell(3)));
	}

	#[test]
	fn read_then_persist_round_trips_a_mutation() {
		// Without an in-place overwrite on the persist half the mutation is silently dropped.
		let mut store = MockStore::default();
		set(&mut store, vec![(Key::new("a"), cell(1))]).unwrap();

		let mut value = get::<_, Cell>(&mut store, from_ref(&Key::new("a"))).unwrap().remove(0).unwrap();
		value.value += 41;
		set(&mut store, vec![(Key::new("a"), value)]).unwrap();

		assert_eq!(get::<_, Cell>(&mut store, from_ref(&Key::new("a"))).unwrap().remove(0), Some(cell(42)));
	}

	#[test]
	fn remove_issues_a_state_remove_and_the_key_reads_back_absent() {
		let mut store = MockStore::default();
		set(&mut store, vec![(Key::new("a"), cell(5))]).unwrap();

		remove(&mut store, from_ref(&Key::new("a"))).unwrap();

		assert_eq!(store.removes, 1);
		assert_eq!(get::<_, Cell>(&mut store, from_ref(&Key::new("a"))).unwrap().remove(0), None);
	}

	#[test]
	fn a_typed_session_hands_the_write_the_size_it_already_read() {
		// A session must hand each write the durable size from its own batch read, never read the key again.
		let mut store = MockStore::default();
		set(&mut store, vec![(Key::new("a"), cell(123_456_789))]).unwrap();
		let durable = store.data.values().next().expect("the seed write is in the store").bytes().len();
		assert!(
			durable > 1,
			"the seed must encode to more than one byte or the size assertion cannot discriminate"
		);
		store.classifications.clear();

		let mut session = batch::<_, Cell>(&mut store, &[Key::new("a")]).unwrap();
		session.set(0, &cell(1)).unwrap();
		session.commit(&mut store).unwrap();

		assert_eq!(store.gets, 1, "the session must read its keys in exactly one batch read");
		assert_eq!(
			store.classifications.len(),
			1,
			"the write must be handed exactly one pre-image, or it falls back to reading the key again"
		);
		assert_eq!(
			store.classifications[0].1,
			Some(ByteSize::from_bytes(durable as u64)),
			"the size handed down must be what a durable read of the row would measure"
		);
	}

	#[test]
	fn a_typed_session_over_an_absent_key_hands_down_an_absence() {
		// Absence is a classification too: an insert billed as a replace debits a row the census never held.
		let mut store = MockStore::default();

		let mut session = batch::<_, Cell>(&mut store, &[Key::new("missing")]).unwrap();
		session.set(0, &cell(3)).unwrap();
		session.commit(&mut store).unwrap();

		assert_eq!(
			store.classifications,
			vec![(Key::new("missing").into_group_state_key().as_slice().to_vec(), None)],
			"a key the read did not find must be handed down as absent, not left for the write to discover"
		);
	}

	#[test]
	fn a_typed_multi_get_hands_the_write_the_size_it_already_read() {
		// A multi-get must hand each write the durable size from its own batch read, never read the key again.
		let mut store = MockStore::default();
		set(&mut store, vec![(Key::new("a"), cell(123_456_789))]).unwrap();
		let durable = store.data.values().next().expect("the seed write is in the store").bytes().len();
		assert!(
			durable > 1,
			"the seed must encode to more than one byte or the size assertion cannot discriminate"
		);
		store.classifications.clear();

		let values = get_classified::<_, Cell>(&mut store, &[Key::new("a")]).unwrap();

		assert_eq!(values, vec![Some(cell(123_456_789))], "classifying must not disturb the value it returns");
		assert_eq!(store.gets, 1, "the multi-get must read its keys in exactly one batch read");
		assert_eq!(
			store.classifications[0].1,
			Some(ByteSize::from_bytes(durable as u64)),
			"the size handed down must be what a durable read of the row would measure"
		);
	}

	#[test]
	fn a_typed_multi_get_of_a_key_that_is_not_there_hands_down_an_absence() {
		// An absent key must come back as an absence, never as a default that reads like a stored zero.
		let mut store = MockStore::default();

		get_classified::<_, Cell>(&mut store, &[Key::new("missing")]).unwrap();

		assert_eq!(
			store.classifications,
			vec![(Key::new("missing").into_group_state_key().as_slice().to_vec(), None)],
			"a key the read did not find must be handed down as absent, not left for the write to discover"
		);
	}
}
