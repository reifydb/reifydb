// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::hash::Hash;

use reifydb_codec::row::operator::state::{OperatorState, decode};
use reifydb_core::{key::operator_state::IntoGroupStateKey, metrics::heap::HeapSize, state::timer::StateStore};
use reifydb_value::Result;

pub fn get<K, V>(store: &mut dyn StateStore, key: &K) -> Result<Option<V>>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
	V: Clone + OperatorState + HeapSize,
{
	let encoded_key = key.into_group_state_key();
	match store.state_get(&encoded_key)? {
		Some(bytes) => Ok(Some(decode::<V>(&bytes)?)),
		None => Ok(None),
	}
}

pub fn set<K, V>(store: &mut dyn StateStore, key: &K, value: &V) -> Result<()>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
	V: Clone + OperatorState + HeapSize,
{
	let encoded_key = key.into_group_state_key();
	let payload = value.encode_state()?;
	store.state_set(&encoded_key, payload)
}

pub fn put<K, V>(store: &mut dyn StateStore, key: &K, value: V) -> Result<()>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
	V: Clone + OperatorState + HeapSize,
{
	set(store, key, &value)
}

pub fn modify<K, V, R>(store: &mut dyn StateStore, key: &K, f: impl FnOnce(&mut V) -> R) -> Result<R>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
	V: Clone + Default + OperatorState + HeapSize,
{
	let encoded_key = key.into_group_state_key();
	let mut value = match store.state_get(&encoded_key)? {
		Some(bytes) => decode::<V>(&bytes)?,
		None => V::default(),
	};
	let result = f(&mut value);
	store.state_set(&encoded_key, value.encode_state()?)?;
	Ok(result)
}

pub fn remove<K>(store: &mut dyn StateStore, key: &K) -> Result<()>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
{
	let encoded_key = key.into_group_state_key();
	store.state_remove(&encoded_key)
}

pub fn get_or_default<K, V>(store: &mut dyn StateStore, key: &K) -> Result<V>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
	V: Clone + Default + OperatorState + HeapSize,
{
	match get(store, key)? {
		Some(value) => Ok(value),
		None => Ok(V::default()),
	}
}

pub fn update<K, V, U>(store: &mut dyn StateStore, key: &K, updater: U) -> Result<V>
where
	K: Hash + Eq + Clone + HeapSize,
	for<'a> &'a K: IntoGroupStateKey,
	V: Clone + Default + OperatorState + HeapSize,
	U: FnOnce(&mut V) -> Result<()>,
{
	let mut value = get_or_default(store, key)?;
	updater(&mut value)?;
	set(store, key, &value)?;
	Ok(value)
}

#[cfg(test)]
mod tests {
	use std::{collections::HashMap, ops::Bound};

	use reifydb_codec::{
		key::encoded::{EncodedKey, EncodedKeyRange},
		row::pod::EncodedPodRow,
	};
	use reifydb_core::{
		key::operator_state::{GroupId, GroupStateKey, Keyspace},
		state::timer::{TimerKind, TimerStore},
	};
	use reifydb_macro::operator_state;
	use reifydb_value::value::{datetime::DateTime, row_number::RowNumber};

	use super::*;

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
			GroupStateKey::root(Keyspace::CUSTOM_NOT_CACHED, self.0.as_bytes())
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
		groups: HashMap<Vec<u8>, GroupId>,
		removes: usize,
		sets: usize,
		gets: usize,
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
		fn intern_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<(GroupId, bool)>> {
			let mut interned = Vec::with_capacity(groups.len());
			for group in groups {
				let bytes = group.as_bytes().to_vec();
				match self.groups.get(&bytes) {
					Some(id) => interned.push((*id, false)),
					None => {
						let next = GroupId(self.groups.len() as u64 + GroupId::FIRST.0);
						self.groups.insert(bytes, next);
						interned.push((next, true));
					}
				}
			}
			Ok(interned)
		}

		fn lookup_groups(&mut self, groups: &[EncodedKey]) -> Result<Vec<Option<GroupId>>> {
			Ok(groups.iter().map(|group| self.groups.get(group.as_bytes()).copied()).collect())
		}

		fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
			self.gets += 1;
			Ok(self.data.get(key.as_slice()).cloned())
		}

		fn state_get_many_visit(
			&mut self,
			keys: &[GroupStateKey],
			visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
		) -> Result<()> {
			for key in keys {
				if let Some(b) = self.data.get(key.as_slice()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}

		fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
			self.sets += 1;
			self.data.insert(key.as_slice().to_vec(), payload);
			Ok(())
		}

		fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
			self.removes += 1;
			self.data.remove(key.as_slice());
			Ok(())
		}

		fn state_range_visit(
			&mut self,
			range: EncodedKeyRange,
			limit: Option<usize>,
			visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
		) -> Result<()> {
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
			for (k, b) in matched {
				let k = GroupStateKey::from_framed(EncodedKey::new(k))
					.expect("fake store holds an unframed state key");
				visit(k, b)?;
			}
			Ok(())
		}

		fn get_or_create_row_numbers(
			&mut self,
			_group: GroupId,
			keys: &[EncodedKey],
		) -> Result<Vec<(RowNumber, bool)>> {
			Ok(keys.iter().enumerate().map(|(i, _)| (RowNumber(i as u64 + 1), true)).collect())
		}

		fn get_or_create_row_numbers_for_pairs(
			&mut self,
			pairs: &[(GroupId, EncodedKey)],
		) -> Result<Vec<(RowNumber, bool)>> {
			Ok(pairs.iter().enumerate().map(|(i, _)| (RowNumber(i as u64 + 1), true)).collect())
		}

		fn remove_row_number(&mut self, _group: GroupId, _key: &EncodedKey) -> Result<()> {
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

		set(&mut store, &Key::new("a"), &cell(7)).unwrap();

		assert_eq!(store.sets, 1, "set must issue exactly one state_set");
		assert!(!store.data.is_empty(), "the value must be in the store before any flush");
		assert_eq!(get::<_, Cell>(&mut store, &Key::new("a")).unwrap(), Some(cell(7)));
	}

	#[test]
	fn get_reads_through_to_the_store_every_time() {
		// A cached second read would let another writer's value go unseen.
		let mut store = MockStore::default();
		set(&mut store, &Key::new("a"), &cell(1)).unwrap();

		get::<_, Cell>(&mut store, &Key::new("a")).unwrap();
		get::<_, Cell>(&mut store, &Key::new("a")).unwrap();

		assert_eq!(store.gets, 2, "each get must be served by the store, never from residency");
	}

	#[test]
	fn a_miss_consults_the_store_rather_than_proving_absence() {
		// Absence is no longer answerable from a filter; every miss must be a real store read.
		let mut store = MockStore::default();

		assert_eq!(get::<_, Cell>(&mut store, &Key::new("absent")).unwrap(), None);
		assert_eq!(store.gets, 1, "the miss must have consulted the store");
	}

	#[test]
	fn a_read_leaves_the_row_in_the_store_for_the_next_reader() {
		// The read half of load-mutate-persist must not consume the row, or the base value is lost.
		let mut store = MockStore::default();
		set(&mut store, &Key::new("a"), &cell(3)).unwrap();

		assert_eq!(get::<_, Cell>(&mut store, &Key::new("a")).unwrap(), Some(cell(3)));
		assert_eq!(store.removes, 0, "a read must not issue a state_remove");
		assert_eq!(get::<_, Cell>(&mut store, &Key::new("a")).unwrap(), Some(cell(3)));
	}

	#[test]
	fn read_then_persist_round_trips_a_mutation() {
		// Without an in-place overwrite on the persist half the mutation is silently dropped.
		let mut store = MockStore::default();
		set(&mut store, &Key::new("a"), &cell(1)).unwrap();

		let mut value = get::<_, Cell>(&mut store, &Key::new("a")).unwrap().unwrap();
		value.value += 41;
		set(&mut store, &Key::new("a"), &value).unwrap();

		assert_eq!(get::<_, Cell>(&mut store, &Key::new("a")).unwrap(), Some(cell(42)));
	}

	#[test]
	fn remove_issues_a_state_remove_and_the_key_reads_back_absent() {
		let mut store = MockStore::default();
		set(&mut store, &Key::new("a"), &cell(5)).unwrap();

		remove(&mut store, &Key::new("a")).unwrap();

		assert_eq!(store.removes, 1);
		assert_eq!(get::<_, Cell>(&mut store, &Key::new("a")).unwrap(), None);
	}

	#[test]
	fn modify_mutates_the_stored_value_and_persists_it() {
		// modify is load-mutate-persist in one call; skipping the persist half would strand the mutation.
		let mut store = MockStore::default();
		set(&mut store, &Key::new("a"), &cell(1)).unwrap();

		let returned = modify(&mut store, &Key::new("a"), |value: &mut Cell| {
			value.value += 6;
			value.value
		})
		.unwrap();

		assert_eq!(returned, 7);
		assert_eq!(get::<_, Cell>(&mut store, &Key::new("a")).unwrap(), Some(cell(7)));
	}

	#[test]
	fn modify_on_a_miss_starts_from_default_and_persists() {
		// Otherwise the first mutation of a never-written group is lost.
		let mut store = MockStore::default();

		modify(&mut store, &Key::new("fresh"), |value: &mut Cell| {
			value.value = 5;
		})
		.unwrap();

		assert_eq!(get::<_, Cell>(&mut store, &Key::new("fresh")).unwrap(), Some(cell(5)));
	}

	#[test]
	fn get_or_default_returns_the_default_only_for_an_absent_key() {
		let mut store = MockStore::default();
		set(&mut store, &Key::new("present"), &cell(8)).unwrap();

		assert_eq!(get_or_default::<_, Cell>(&mut store, &Key::new("present")).unwrap(), cell(8));
		assert_eq!(get_or_default::<_, Cell>(&mut store, &Key::new("absent")).unwrap(), Cell::default());
	}

	#[test]
	fn update_persists_the_mutation_and_returns_the_new_value() {
		// If the set half were skipped the returned value would disagree with the next read.
		let mut store = MockStore::default();

		let returned = update(&mut store, &Key::new("a"), |value: &mut Cell| {
			value.value += 4;
			Ok(())
		})
		.unwrap();

		assert_eq!(returned, cell(4));
		assert_eq!(get::<_, Cell>(&mut store, &Key::new("a")).unwrap(), Some(cell(4)));
	}
}
