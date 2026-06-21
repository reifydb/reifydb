// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
};

use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, GroupMeta, LatePolicy, MetaKey, expiry_due_range, expiry_key,
			meta_key_for,
		},
		span::Slot,
		state::StateCache,
		store::WindowStore,
	},
};

pub type RollingBuffer<C, Accumulator> = BTreeMap<C, Accumulator>;

pub type RollingBuckets<G, C, Contribution> = BTreeMap<(G, C), Vec<AccumulatorEvent<Contribution>>>;

pub struct RollingResult<G, Output> {
	pub row_number: RowNumber,
	pub group: G,
	pub value: Output,
	pub prior: Option<Output>,
	pub kind: EmitKind,
}

pub enum RollingEviction<C: Slot> {
	Capacity(usize),
	Before(C),
	BeforeStamp(u64),
}

pub enum RollingExpiry<G, Output> {
	Update {
		row_number: RowNumber,
		group: G,
		value: Output,
	},
	Remove {
		row_number: RowNumber,
		group: G,
	},
}

#[derive(Clone, Copy)]
enum IndexMode {
	Coord,
	Stamp,
}

#[derive(Serialize, Deserialize)]
#[serde(bound(serialize = "G: Serialize", deserialize = "G: DeserializeOwned"))]
struct RollingIndexEntry<G> {
	group: G,
	row_number: u64,
}

fn coord_min_key<C: Slot, A>(buffer: &RollingBuffer<C, A>) -> Option<u64> {
	buffer.keys().next().map(|c| c.order_key())
}

fn stamp_min_key<C, A: WindowAccumulator>(buffer: &RollingBuffer<C, A>) -> Option<u64> {
	buffer.values().filter_map(|a| a.stamp()).min()
}

type MetaLoaded<G, C> = HashMap<G, GroupMeta<C>>;
type BufferRows<G> = HashMap<G, (RowNumber, bool)>;

struct GroupSlot<C, Accumulator> {
	row_number: RowNumber,
	is_new: bool,
	buffer: RollingBuffer<C, Accumulator>,
	was_empty_before: bool,
	buffer_changed: bool,
	prior_index_key: Option<u64>,
}

pub struct RollingEngine<G, C, Accumulator> {
	buffers: StateCache<RowNumber, RollingBuffer<C, Accumulator>>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	late_policy: LatePolicy,
	_pd: PhantomData<G>,
}

impl<G, C, Accumulator> Default for RollingEngine<G, C, Accumulator>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Accumulator: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
{
	fn default() -> Self {
		Self::new()
	}
}

impl<G, C, Accumulator> RollingEngine<G, C, Accumulator>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Accumulator: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
{
	pub fn new() -> Self {
		Self::with_late_policy(LatePolicy::Drop)
	}

	pub fn with_late_policy(late_policy: LatePolicy) -> Self {
		Self {
			buffers: StateCache::<RowNumber, RollingBuffer<C, Accumulator>>::new(8),
			meta: StateCache::<MetaKey, GroupMeta<C>>::new_internal(64),
			late_policy,
			_pd: PhantomData,
		}
	}

	pub fn apply<S, K, CB, Output>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		capacity: usize,
		row_key: K,
		combine: CB,
	) -> Result<Vec<RollingResult<G, Output>>>
	where
		S: WindowStore,
		K: Fn(&G) -> EncodedKey,
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		self.apply_evicting(
			store,
			buckets,
			RollingEviction::Capacity(capacity),
			row_key,
			Accumulator::default,
			combine,
		)
	}

	pub fn apply_evicting<S, K, NA, CB, Output>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		eviction: RollingEviction<C>,
		row_key: K,
		new_accumulator: NA,
		combine: CB,
	) -> Result<Vec<RollingResult<G, Output>>>
	where
		S: WindowStore,
		K: Fn(&G) -> EncodedKey,
		NA: Fn() -> Accumulator,
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let index_mode = match eviction {
			RollingEviction::Capacity(_) => None,
			RollingEviction::Before(_) => Some(IndexMode::Coord),
			RollingEviction::BeforeStamp(_) => Some(IndexMode::Stamp),
		};
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let buffer_rows = self.resolve_buffer_rows(store, &buckets, &meta_loaded, &row_key)?;
		let group_slots = self.apply_events_into_buffers(
			store,
			buckets,
			&mut meta_loaded,
			&buffer_rows,
			&row_key,
			&eviction,
			&new_accumulator,
			index_mode,
		)?;
		let results = self.combine_and_collect(store, group_slots, &combine, index_mode)?;
		self.persist_meta(store, meta_loaded)?;
		Ok(results)
	}

	pub fn flush<S: WindowStore>(&mut self, store: &mut S) -> Result<()> {
		self.buffers.flush(store)?;
		self.meta.flush(store)?;
		Ok(())
	}

	fn warm_and_load_meta<S: WindowStore>(
		&mut self,
		store: &mut S,
		buckets: &RollingBuckets<G, C, Accumulator::Contribution>,
	) -> Result<MetaLoaded<G, C>> {
		let meta_keys: Vec<MetaKey> = buckets
			.keys()
			.map(|(group, _)| group)
			.collect::<BTreeSet<_>>()
			.into_iter()
			.map(meta_key_for)
			.collect();
		self.meta.warm(store, &meta_keys)?;

		let mut meta_loaded: MetaLoaded<G, C> = HashMap::new();
		for (group, _) in buckets.keys() {
			if !meta_loaded.contains_key(group) {
				let m = self.meta.get(store, &meta_key_for(group))?.unwrap_or_default();
				meta_loaded.insert(group.clone(), m);
			}
		}
		Ok(meta_loaded)
	}

	fn resolve_buffer_rows<S, K>(
		&mut self,
		store: &mut S,
		buckets: &RollingBuckets<G, C, Accumulator::Contribution>,
		meta_loaded: &MetaLoaded<G, C>,
		row_key: &K,
	) -> Result<BufferRows<G>>
	where
		S: WindowStore,
		K: Fn(&G) -> EncodedKey,
	{
		let mut buffer_rows: BufferRows<G> = HashMap::new();
		let mut resolve_order: Vec<G> = Vec::new();
		let mut group_keys: Vec<EncodedKey> = Vec::new();
		let mut seen: BTreeSet<G> = BTreeSet::new();
		for (group, coord) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.high_water);
			if initial_high_water.is_none_or(|hw| *coord >= hw) && seen.insert(group.clone()) {
				resolve_order.push(group.clone());
				group_keys.push(row_key(group));
			}
		}
		let resolved_rows = store.get_or_create_row_numbers(&group_keys)?;
		reifydb_assertions! {
			let requested = group_keys.len();
			let resolved = resolved_rows.len();
			assert!(
				requested == resolved,
				"get_or_create_row_numbers returned a different count than requested, so the resolve_order \
				 zip would silently truncate buffer_rows and survivor groups would be re-resolved one at a \
				 time in apply_events_into_buffers, changing the per-batch row-number lookup cost \
				 (requested={requested}, resolved={resolved})"
			);
		}
		let buffer_keys: Vec<RowNumber> = resolved_rows.iter().map(|(rn, _)| *rn).collect();
		for (group, resolved) in resolve_order.into_iter().zip(resolved_rows) {
			buffer_rows.insert(group, resolved);
		}
		self.buffers.warm(store, &buffer_keys)?;
		Ok(buffer_rows)
	}

	#[allow(clippy::too_many_arguments)]
	fn apply_events_into_buffers<S, K, NA>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		meta_loaded: &mut MetaLoaded<G, C>,
		buffer_rows: &BufferRows<G>,
		row_key: &K,
		eviction: &RollingEviction<C>,
		new_accumulator: &NA,
		index_mode: Option<IndexMode>,
	) -> Result<BTreeMap<G, GroupSlot<C, Accumulator>>>
	where
		S: WindowStore,
		K: Fn(&G) -> EncodedKey,
		NA: Fn() -> Accumulator,
	{
		let mut group_slots: BTreeMap<G, GroupSlot<C, Accumulator>> = BTreeMap::new();

		for ((group, coord), events) in buckets {
			let meta = meta_loaded.entry(group.clone()).or_default();

			let slot = match group_slots.get_mut(&group) {
				Some(s) => s,
				None => {
					let (row_number, is_new) = match buffer_rows.get(&group) {
						Some(&resolved) => resolved,
						None => {
							let key = row_key(&group);
							store.get_or_create_row_number(&key)?
						}
					};
					let buffer: RollingBuffer<C, Accumulator> =
						self.buffers.get(store, &row_number)?.unwrap_or_default();
					let was_empty_before = buffer.is_empty();
					let prior_index_key = match index_mode {
						Some(IndexMode::Coord) => coord_min_key(&buffer),
						Some(IndexMode::Stamp) => stamp_min_key(&buffer),
						None => None,
					};
					group_slots.insert(
						group.clone(),
						GroupSlot {
							row_number,
							is_new,
							buffer,
							was_empty_before,
							buffer_changed: false,
							prior_index_key,
						},
					);
					group_slots.get_mut(&group).expect("just inserted")
				}
			};

			let late = matches!(meta.high_water, Some(hw) if coord < hw)
				&& matches!(self.late_policy, LatePolicy::Drop)
				&& !slot.buffer.contains_key(&coord);

			let mut accumulator = slot.buffer.remove(&coord).unwrap_or_else(new_accumulator);
			let mut touched = false;
			for event in events {
				match event {
					AccumulatorEvent::Add(c) => {
						if late {
							continue;
						}
						accumulator.add(&c);
						touched = true;
					}
					AccumulatorEvent::Remove(c) => {
						if accumulator.is_empty() {
							continue;
						}
						accumulator.remove(&c);
						touched = true;
					}
				}
			}
			if !accumulator.is_empty() {
				slot.buffer.insert(coord, accumulator);
			}
			if !touched {
				continue;
			}
			match eviction {
				RollingEviction::Capacity(cap) => {
					while slot.buffer.len() > *cap {
						slot.buffer.pop_first();
					}
				}
				RollingEviction::Before(cutoff) => {
					while let Some((&oldest, _)) = slot.buffer.iter().next() {
						if oldest <= *cutoff {
							slot.buffer.pop_first();
						} else {
							break;
						}
					}
				}
				RollingEviction::BeforeStamp(cutoff) => {
					let stale: Vec<C> = slot
						.buffer
						.iter()
						.filter(|(_, accumulator)| {
							accumulator.stamp().is_some_and(|s| s <= *cutoff)
						})
						.map(|(coord, _)| *coord)
						.collect();
					for coord in stale {
						slot.buffer.remove(&coord);
					}
				}
			}
			slot.buffer_changed = true;

			meta.high_water = Some(match meta.high_water {
				Some(hw) if hw > coord => hw,
				_ => coord,
			});
		}
		Ok(group_slots)
	}

	fn combine_and_collect<S, CB, Output>(
		&mut self,
		store: &mut S,
		group_slots: BTreeMap<G, GroupSlot<C, Accumulator>>,
		combine: &CB,
		index_mode: Option<IndexMode>,
	) -> Result<Vec<RollingResult<G, Output>>>
	where
		S: WindowStore,
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		let mut results: Vec<RollingResult<G, Output>> = Vec::new();
		for (group, slot) in group_slots {
			if !slot.buffer_changed {
				continue;
			}
			if let Some(mode) = index_mode {
				let new_index_key = match mode {
					IndexMode::Coord => coord_min_key(&slot.buffer),
					IndexMode::Stamp => stamp_min_key(&slot.buffer),
				};
				if new_index_key != slot.prior_index_key {
					if let Some(old) = slot.prior_index_key {
						store.internal_drop(&expiry_key(old, &group, &[]))?;
					}
					if let Some(new) = new_index_key {
						store.internal_set(
							&expiry_key(new, &group, &[]),
							&RollingIndexEntry {
								group: group.clone(),
								row_number: slot.row_number.0,
							},
						)?;
					}
				}
			}
			let output = combine(&group, &slot.buffer);
			self.buffers.put(store, &slot.row_number, slot.buffer)?;

			if let Some(out) = output {
				let kind = if slot.is_new || slot.was_empty_before {
					EmitKind::Insert
				} else {
					EmitKind::Update
				};
				results.push(RollingResult {
					row_number: slot.row_number,
					group,
					value: out,
					prior: None,
					kind,
				});
			}
		}
		Ok(results)
	}

	pub fn expire_before<S, CB, Output>(
		&mut self,
		store: &mut S,
		cutoff: C,
		combine: CB,
	) -> Result<Vec<RollingExpiry<G, Output>>>
	where
		S: WindowStore,
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		let mut due: Vec<(EncodedKey, RollingIndexEntry<G>)> = Vec::new();
		store.internal_range_visit::<RollingIndexEntry<G>>(
			expiry_due_range(cutoff.order_key()),
			&mut |key, entry| {
				due.push((key, entry));
				Ok(())
			},
		)?;

		let mut out: Vec<RollingExpiry<G, Output>> = Vec::new();
		for (index_key, entry) in due {
			let row_number = RowNumber(entry.row_number);
			store.internal_drop(&index_key)?;
			let Some(mut buffer) = self.buffers.get(store, &row_number)? else {
				continue;
			};
			let before = buffer.len();
			buffer.retain(|&coord, _| coord > cutoff);
			if buffer.len() == before {
				if let Some(new) = coord_min_key(&buffer) {
					store.internal_set(
						&expiry_key(new, &entry.group, &[]),
						&RollingIndexEntry {
							group: entry.group.clone(),
							row_number: entry.row_number,
						},
					)?;
				}
				continue;
			}
			match combine(&entry.group, &buffer) {
				Some(value) if !buffer.is_empty() => {
					if let Some(new) = coord_min_key(&buffer) {
						store.internal_set(
							&expiry_key(new, &entry.group, &[]),
							&RollingIndexEntry {
								group: entry.group.clone(),
								row_number: entry.row_number,
							},
						)?;
					}
					self.buffers.put(store, &row_number, buffer)?;
					out.push(RollingExpiry::Update {
						row_number,
						group: entry.group,
						value,
					});
				}
				_ => {
					self.buffers.remove(store, &row_number)?;
					out.push(RollingExpiry::Remove {
						row_number,
						group: entry.group,
					});
				}
			}
		}
		Ok(out)
	}

	pub fn expire_before_stamp<S, CB, Output>(
		&mut self,
		store: &mut S,
		cutoff: u64,
		combine: CB,
	) -> Result<Vec<RollingExpiry<G, Output>>>
	where
		S: WindowStore,
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		let mut due: Vec<(EncodedKey, RollingIndexEntry<G>)> = Vec::new();
		store.internal_range_visit::<RollingIndexEntry<G>>(expiry_due_range(cutoff), &mut |key, entry| {
			due.push((key, entry));
			Ok(())
		})?;

		let mut out: Vec<RollingExpiry<G, Output>> = Vec::new();
		for (index_key, entry) in due {
			let row_number = RowNumber(entry.row_number);
			store.internal_drop(&index_key)?;
			let Some(mut buffer) = self.buffers.get(store, &row_number)? else {
				continue;
			};
			let before = buffer.len();
			buffer.retain(|_, accumulator| accumulator.stamp().is_none_or(|s| s > cutoff));
			if buffer.len() == before {
				if let Some(new) = stamp_min_key(&buffer) {
					store.internal_set(
						&expiry_key(new, &entry.group, &[]),
						&RollingIndexEntry {
							group: entry.group.clone(),
							row_number: entry.row_number,
						},
					)?;
				}
				continue;
			}
			match combine(&entry.group, &buffer) {
				Some(value) if !buffer.is_empty() => {
					if let Some(new) = stamp_min_key(&buffer) {
						store.internal_set(
							&expiry_key(new, &entry.group, &[]),
							&RollingIndexEntry {
								group: entry.group.clone(),
								row_number: entry.row_number,
							},
						)?;
					}
					self.buffers.put(store, &row_number, buffer)?;
					out.push(RollingExpiry::Update {
						row_number,
						group: entry.group,
						value,
					});
				}
				_ => {
					self.buffers.remove(store, &row_number)?;
					out.push(RollingExpiry::Remove {
						row_number,
						group: entry.group,
					});
				}
			}
		}
		Ok(out)
	}

	fn persist_meta<S: WindowStore>(&mut self, store: &mut S, meta_loaded: MetaLoaded<G, C>) -> Result<()> {
		for (group, meta) in meta_loaded {
			self.meta.set(store, &meta_key_for(&group), &meta)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use crate::{
		encoded::key::EncodedKey,
		window::engine::{
			AccumulatorEvent,
			rolling::{RollingBuckets, RollingBuffer, RollingEngine, RollingEviction, RollingExpiry},
			test_support::{MockStore, StampedSum, SumAccumulator},
		},
	};

	fn row_key(group: &u32) -> EncodedKey {
		EncodedKey::builder().u32(*group).build()
	}

	fn sum_combine(_group: &u32, buffer: &RollingBuffer<u64, SumAccumulator>) -> Option<i64> {
		if buffer.is_empty() {
			None
		} else {
			Some(buffer.values().map(|a| a.sum).sum())
		}
	}

	fn stamped_combine(_group: &u32, buffer: &RollingBuffer<u64, StampedSum>) -> Option<i64> {
		if buffer.is_empty() {
			None
		} else {
			Some(buffer.values().map(|a| a.sum).sum())
		}
	}

	#[test]
	fn expire_before_evicts_a_quiet_group_then_rekeys_then_removes() {
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new();
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Add(1)]);
		buckets.insert((1u32, 20u64), vec![AccumulatorEvent::Add(2)]);
		buckets.insert((1u32, 30u64), vec![AccumulatorEvent::Add(3)]);
		// Before(0) evicts nothing at apply (all coords > 0), so the buffer keeps 10,20,30.
		engine.apply_evicting(
			&mut store,
			buckets,
			RollingEviction::Before(0),
			row_key,
			SumAccumulator::default,
			sum_combine,
		)
		.unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(store.index_entry_count(), 1, "the group is indexed by its oldest coord");

		// A tick with no new events for this group evicts coords <= 20; coord 30 survives.
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new();
		let out = engine.expire_before(&mut store, 20, sum_combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(out.len(), 1);
		match &out[0] {
			RollingExpiry::Update {
				group,
				value,
				..
			} => {
				assert_eq!(*group, 1);
				assert_eq!(*value, 3, "only the surviving coord 30 contributes");
			}
			RollingExpiry::Remove {
				..
			} => panic!("group still has a live coord"),
		}
		assert_eq!(store.index_entry_count(), 1, "still one entry, re-keyed to coord 30");

		// The next tick evicts the last coord: the group empties and is removed.
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new();
		let out = engine.expire_before(&mut store, 30, sum_combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(out.len(), 1);
		match &out[0] {
			RollingExpiry::Remove {
				group,
				..
			} => assert_eq!(*group, 1),
			RollingExpiry::Update {
				..
			} => panic!("the group is empty and must be removed"),
		}
		assert_eq!(store.index_entry_count(), 0, "the emptied group leaves no index entry");

		// A further tick finds nothing due.
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new();
		assert!(engine.expire_before(&mut store, 1000, sum_combine).unwrap().is_empty());
	}

	#[test]
	fn expire_before_leaves_groups_whose_oldest_coord_is_not_due() {
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new();
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 100u64), vec![AccumulatorEvent::Add(1)]);
		buckets.insert((2u32, 5u64), vec![AccumulatorEvent::Add(9)]);
		engine.apply_evicting(
			&mut store,
			buckets,
			RollingEviction::Before(0),
			row_key,
			SumAccumulator::default,
			sum_combine,
		)
		.unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(store.index_entry_count(), 2);

		// Cutoff 5 is due only for group 2 (oldest coord 5); group 1 (oldest 100) is untouched.
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new();
		let out = engine.expire_before(&mut store, 5, sum_combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(out.len(), 1, "only the group with a due coord is processed");
		assert!(matches!(&out[0], RollingExpiry::Remove { group, .. } if *group == 2));
		assert_eq!(store.index_entry_count(), 1, "group 1 keeps its index entry");
	}

	#[test]
	fn expire_before_stamp_evicts_by_accumulator_stamp() {
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, u64, StampedSum>::new();
		let mut buckets: RollingBuckets<u32, u64, (i64, u64)> = BTreeMap::new();
		buckets.insert((1u32, 1u64), vec![AccumulatorEvent::Add((1, 10))]);
		buckets.insert((1u32, 2u64), vec![AccumulatorEvent::Add((2, 20))]);
		buckets.insert((1u32, 3u64), vec![AccumulatorEvent::Add((3, 30))]);
		engine.apply_evicting(
			&mut store,
			buckets,
			RollingEviction::BeforeStamp(0),
			row_key,
			StampedSum::default,
			stamped_combine,
		)
		.unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(store.index_entry_count(), 1, "indexed by the minimum stamp");

		// Evict accumulators stamped <= 20; the stamp-30 entry survives.
		let mut engine = RollingEngine::<u32, u64, StampedSum>::new();
		let out = engine.expire_before_stamp(&mut store, 20, stamped_combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(out.len(), 1);
		match &out[0] {
			RollingExpiry::Update {
				value,
				..
			} => assert_eq!(*value, 3),
			RollingExpiry::Remove {
				..
			} => panic!("a live entry remains"),
		}
		assert_eq!(store.index_entry_count(), 1, "re-keyed to the surviving stamp");
	}
}
