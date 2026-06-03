// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
};

use reifydb_value::reifydb_assertions;
use reifydb_value::{Result, value::row_number::RowNumber};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	window::{
		accumulator::WindowAccumulator,
		engine::{AccEvent, EmitKind, GroupMeta, MetaKey, meta_key_for},
		span::Slot,
		state::StateCache,
		store::WindowStore,
	},
};

pub type RollingBuffer<C, Acc> = BTreeMap<C, Acc>;

pub type RollingBuckets<G, C, Contribution> = BTreeMap<(G, C), Vec<AccEvent<Contribution>>>;

pub struct RollingResult<G, Output> {
	pub row_number: RowNumber,
	pub group: G,
	pub value: Output,
	pub kind: EmitKind,
}

type MetaLoaded<G, C> = HashMap<G, GroupMeta<C>>;
type BufferRows<G> = HashMap<G, (RowNumber, bool)>;

struct GroupSlot<C, Acc> {
	row_number: RowNumber,
	is_new: bool,
	buffer: RollingBuffer<C, Acc>,
	was_empty_before: bool,
	buffer_changed: bool,
}

pub struct RollingEngine<G, C, Acc> {
	buffers: StateCache<RowNumber, RollingBuffer<C, Acc>>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	_pd: PhantomData<G>,
}

impl<G, C, Acc> Default for RollingEngine<G, C, Acc>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Acc: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
{
	fn default() -> Self {
		Self::new()
	}
}

impl<G, C, Acc> RollingEngine<G, C, Acc>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Acc: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
{
	pub fn new() -> Self {
		Self {
			buffers: StateCache::<RowNumber, RollingBuffer<C, Acc>>::new(8),
			meta: StateCache::<MetaKey, GroupMeta<C>>::new_internal(64),
			_pd: PhantomData,
		}
	}

	pub fn apply<S, K, CB, Output>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, Acc::Contribution>,
		capacity: usize,
		row_key: K,
		combine: CB,
	) -> Result<Vec<RollingResult<G, Output>>>
	where
		S: WindowStore,
		K: Fn(&G) -> EncodedKey,
		CB: Fn(&G, &RollingBuffer<C, Acc>) -> Option<Output>,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let buffer_rows = self.resolve_buffer_rows(store, &buckets, &meta_loaded, &row_key)?;
		let group_slots = self.apply_events_into_buffers(
			store,
			buckets,
			&mut meta_loaded,
			&buffer_rows,
			&row_key,
			capacity,
		)?;
		let results = self.combine_and_collect(store, group_slots, &combine)?;
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
		buckets: &RollingBuckets<G, C, Acc::Contribution>,
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
		buckets: &RollingBuckets<G, C, Acc::Contribution>,
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

	fn apply_events_into_buffers<S, K>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, Acc::Contribution>,
		meta_loaded: &mut MetaLoaded<G, C>,
		buffer_rows: &BufferRows<G>,
		row_key: &K,
		capacity: usize,
	) -> Result<BTreeMap<G, GroupSlot<C, Acc>>>
	where
		S: WindowStore,
		K: Fn(&G) -> EncodedKey,
	{
		let mut group_slots: BTreeMap<G, GroupSlot<C, Acc>> = BTreeMap::new();

		for ((group, coord), events) in buckets {
			let meta = meta_loaded.entry(group.clone()).or_default();

			if let Some(hw) = meta.high_water
				&& coord < hw
			{
				continue;
			}

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
					let buffer: RollingBuffer<C, Acc> =
						self.buffers.get(store, &row_number)?.unwrap_or_default();
					let was_empty_before = buffer.is_empty();
					group_slots.insert(
						group.clone(),
						GroupSlot {
							row_number,
							is_new,
							buffer,
							was_empty_before,
							buffer_changed: false,
						},
					);
					group_slots.get_mut(&group).expect("just inserted")
				}
			};

			let mut acc = slot.buffer.remove(&coord).unwrap_or_default();
			for event in events {
				match event {
					AccEvent::Add(c) => acc.add(&c),
					AccEvent::Remove(c) => acc.remove(&c),
				}
			}
			if !acc.is_empty() {
				slot.buffer.insert(coord, acc);
			}
			while slot.buffer.len() > capacity {
				slot.buffer.pop_first();
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
		group_slots: BTreeMap<G, GroupSlot<C, Acc>>,
		combine: &CB,
	) -> Result<Vec<RollingResult<G, Output>>>
	where
		S: WindowStore,
		CB: Fn(&G, &RollingBuffer<C, Acc>) -> Option<Output>,
	{
		let mut results: Vec<RollingResult<G, Output>> = Vec::new();
		for (group, slot) in group_slots {
			if !slot.buffer_changed {
				continue;
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
					kind,
				});
			}
		}
		Ok(results)
	}

	fn persist_meta<S: WindowStore>(&mut self, store: &mut S, meta_loaded: MetaLoaded<G, C>) -> Result<()> {
		for (group, meta) in meta_loaded {
			self.meta.set(store, &meta_key_for(&group), &meta)?;
		}
		Ok(())
	}
}
