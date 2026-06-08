// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
};

use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, GroupMeta, MetaKey, meta_key_for,
			rolling::{RollingBuckets, RollingBuffer, RollingResult},
		},
		span::Slot,
		state::StateCache,
		store::WindowStore,
	},
};

type MetaLoaded<G, C> = HashMap<G, GroupMeta<C>>;
type BufferRows<G> = HashMap<G, (RowNumber, bool)>;

struct GroupSlot<C, Accumulator, Running> {
	row_number: RowNumber,
	is_new: bool,
	buffer: RollingBuffer<C, Accumulator>,
	running: Running,
	was_empty_before: bool,
	buffer_changed: bool,
}

pub struct RollingIncrementalEngine<G, C, Accumulator, Running> {
	buffers: StateCache<RowNumber, RollingBuffer<C, Accumulator>>,
	running: StateCache<RowNumber, Running>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	_pd: PhantomData<G>,
}

impl<G, C, Accumulator, Running> Default for RollingIncrementalEngine<G, C, Accumulator, Running>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Accumulator: WindowAccumulator,
	Running: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
{
	fn default() -> Self {
		Self::new()
	}
}

impl<G, C, Accumulator, Running> RollingIncrementalEngine<G, C, Accumulator, Running>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Accumulator: WindowAccumulator,
	Running: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
{
	pub fn new() -> Self {
		Self {
			buffers: StateCache::<RowNumber, RollingBuffer<C, Accumulator>>::new(8),
			running: StateCache::<RowNumber, Running>::new(8),
			meta: StateCache::<MetaKey, GroupMeta<C>>::new_internal(64),
			_pd: PhantomData,
		}
	}

	pub fn apply<S, K, WC, CR, Output>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		capacity: usize,
		row_key: K,
		window_contribution: WC,
		combine_running: CR,
	) -> Result<Vec<RollingResult<G, Output>>>
	where
		S: WindowStore,
		K: Fn(&G) -> EncodedKey,
		WC: Fn(&Accumulator::Output) -> Running::Contribution,
		CR: Fn(&G, &Running, &Accumulator::Output, C) -> Option<Output>,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let buffer_rows = self.resolve_buffer_rows(store, &buckets, &meta_loaded, &row_key)?;

		let mut group_slots: BTreeMap<G, GroupSlot<C, Accumulator, Running>> = BTreeMap::new();

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
					let running: Running =
						self.running.get(store, &row_number)?.unwrap_or_default();
					let was_empty_before = buffer.is_empty();
					group_slots.insert(
						group.clone(),
						GroupSlot {
							row_number,
							is_new,
							buffer,
							running,
							was_empty_before,
							buffer_changed: false,
						},
					);
					group_slots.get_mut(&group).expect("just inserted")
				}
			};

			let late =
				matches!(meta.high_water, Some(hw) if coord < hw) && !slot.buffer.contains_key(&coord);

			let mut accumulator = slot.buffer.remove(&coord).unwrap_or_default();
			let old_value = accumulator.finalize();
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
			if !touched {
				continue;
			}
			let new_value = accumulator.finalize();

			if let Some(old) = &old_value {
				slot.running.remove(&window_contribution(old));
			}
			if let Some(new) = &new_value {
				slot.running.add(&window_contribution(new));
			}

			if !accumulator.is_empty() {
				slot.buffer.insert(coord, accumulator);
			}
			while slot.buffer.len() > capacity {
				if let Some((_, evicted)) = slot.buffer.pop_first()
					&& let Some(value) = evicted.finalize()
				{
					slot.running.remove(&window_contribution(&value));
				}
			}
			slot.buffer_changed = true;

			meta.high_water = Some(match meta.high_water {
				Some(hw) if hw > coord => hw,
				_ => coord,
			});
		}

		let mut results: Vec<RollingResult<G, Output>> = Vec::new();
		for (group, slot) in group_slots {
			if !slot.buffer_changed {
				continue;
			}
			let output = match slot.buffer.iter().next_back() {
				Some((coord, accumulator)) => accumulator
					.finalize()
					.and_then(|newest| combine_running(&group, &slot.running, &newest, *coord)),
				None => None,
			};
			self.buffers.put(store, &slot.row_number, slot.buffer)?;
			self.running.put(store, &slot.row_number, slot.running)?;

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
		self.persist_meta(store, meta_loaded)?;
		Ok(results)
	}

	pub fn flush<S: WindowStore>(&mut self, store: &mut S) -> Result<()> {
		self.buffers.flush(store)?;
		self.running.flush(store)?;
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
			let resolved = resolved_rows.len();
			let requested = group_keys.len();
			assert!(
				resolved == requested,
				"get_or_create_row_numbers returned {resolved} rows for {requested} group keys; \
				 the zip below pairs resolve_order with resolved_rows by position, so a length \
				 mismatch would silently leave some groups without a buffer_rows entry and route \
				 them through the per-bucket get_or_create_row_number fallback, diverging behaviour"
			);
		}
		let state_keys: Vec<RowNumber> = resolved_rows.iter().map(|(rn, _)| *rn).collect();
		for (group, resolved) in resolve_order.into_iter().zip(resolved_rows) {
			buffer_rows.insert(group, resolved);
		}
		self.buffers.warm(store, &state_keys)?;
		self.running.warm(store, &state_keys)?;
		Ok(buffer_rows)
	}

	fn persist_meta<S: WindowStore>(&mut self, store: &mut S, meta_loaded: MetaLoaded<G, C>) -> Result<()> {
		for (group, meta) in meta_loaded {
			self.meta.set(store, &meta_key_for(&group), &meta)?;
		}
		Ok(())
	}
}
