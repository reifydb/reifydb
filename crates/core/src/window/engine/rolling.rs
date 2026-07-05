// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
};

use reifydb_codec::key::encoded::{EncodedKey, IntoEncodedKey};
use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::window::{
	accumulator::WindowAccumulator,
	engine::{
		AccumulatorEvent, EmitKind, GroupMeta, MetaKey, RunningKey, config::WindowEngineConfig,
		coord_due_range, coord_entry_key, coord_row_range, expiry_due_range, expiry_key, meta_key_for,
	},
	span::Slot,
	state::StateCache,
	store::WindowStore,
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

struct GroupSlot<C, Accumulator, Output> {
	row_number: RowNumber,
	is_new: bool,
	buffer: RollingBuffer<C, Accumulator>,
	was_empty_before: bool,
	buffer_changed: bool,
	prior_index_key: Option<u64>,
	prior_output: Option<Output>,
}

pub struct RollingEngine<G, C, Accumulator> {
	buffers: StateCache<RowNumber, RollingBuffer<C, Accumulator>>,
	running: Option<StateCache<RunningKey, Accumulator>>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	expire_batch: usize,
	_pd: PhantomData<G>,
}

struct RunnableGroupSlot<Accumulator>
where
	Accumulator: WindowAccumulator,
{
	row_number: RowNumber,
	is_new: bool,
	running: Accumulator,
	was_empty_before: bool,
	buffer_changed: bool,
	prior_min: Option<u64>,
	prior_output: Option<Accumulator::Output>,
}

fn merge_into<A: WindowAccumulator>(running: &mut A, other: &A) {
	if running.is_empty() {
		*running = other.clone();
	} else {
		running.merge(other);
	}
}

fn bootstrap_running<C, A>(buffer: &RollingBuffer<C, A>) -> A
where
	C: Slot,
	A: WindowAccumulator,
{
	let mut values = buffer.values();
	let Some(first) = values.next() else {
		return A::default();
	};
	let mut running = first.clone();
	for accumulator in values {
		running.merge(accumulator);
	}
	running
}

fn scan_running<S, A>(store: &mut S, row_number: RowNumber) -> Result<A>
where
	S: WindowStore,
	A: WindowAccumulator,
{
	let mut running = A::default();
	store.internal_range_visit::<A>(coord_row_range(row_number), None, &mut |_key, accumulator| {
		merge_into(&mut running, &accumulator);
		Ok(())
	})?;
	Ok(running)
}

fn peek_min_coord<S, A>(store: &mut S, row_number: RowNumber) -> Result<Option<u64>>
where
	S: WindowStore,
	A: WindowAccumulator,
{
	let mut min: Option<u64> = None;
	store.internal_range_visit::<A>(coord_row_range(row_number), Some(1), &mut |key, _accumulator| {
		let bytes = key.as_bytes();
		if bytes.len() == 17 {
			let mut coord = [0u8; 8];
			coord.copy_from_slice(&bytes[9..17]);
			min = Some(u64::from_be_bytes(coord));
		}
		Ok(())
	})?;
	Ok(min)
}

impl<G, C, Accumulator> RollingEngine<G, C, Accumulator>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Accumulator: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
{
	pub fn new(config: WindowEngineConfig) -> Self {
		Self {
			buffers: StateCache::<RowNumber, RollingBuffer<C, Accumulator>>::new(
				config.state_cache_capacity(),
			),
			running: None,
			meta: StateCache::<MetaKey, GroupMeta<C>>::new_internal(config.internal_state_cache_capacity()),
			expire_batch: config.expire_batch(),
			_pd: PhantomData,
		}
	}

	pub fn new_runnable(config: WindowEngineConfig) -> Self {
		let running = StateCache::<RunningKey, Accumulator>::new(config.state_cache_capacity());
		let mut engine = Self::new(config);
		engine.running = Some(running);
		engine
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
			&combine,
			index_mode,
		)?;
		let results = self.combine_and_collect(store, group_slots, &combine, index_mode)?;
		self.persist_meta(store, meta_loaded)?;
		Ok(results)
	}

	pub fn flush<S: WindowStore>(&mut self, store: &mut S) -> Result<()> {
		self.buffers.flush(store)?;
		if let Some(running) = &mut self.running {
			running.flush(store)?;
		}
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
	fn apply_events_into_buffers<S, K, NA, CB, Output>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		meta_loaded: &mut MetaLoaded<G, C>,
		buffer_rows: &BufferRows<G>,
		row_key: &K,
		eviction: &RollingEviction<C>,
		new_accumulator: &NA,
		combine: &CB,
		index_mode: Option<IndexMode>,
	) -> Result<BTreeMap<G, GroupSlot<C, Accumulator, Output>>>
	where
		S: WindowStore,
		K: Fn(&G) -> EncodedKey,
		NA: Fn() -> Accumulator,
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		let mut group_slots: BTreeMap<G, GroupSlot<C, Accumulator, Output>> = BTreeMap::new();

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
					let prior_output = if was_empty_before {
						None
					} else {
						combine(&group, &buffer)
					};
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
							prior_output,
						},
					);
					group_slots.get_mut(&group).expect("just inserted")
				}
			};

			let mut accumulator = slot.buffer.remove(&coord).unwrap_or_else(new_accumulator);
			let mut touched = false;
			for event in events {
				match event {
					AccumulatorEvent::Add(c) => {
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
		group_slots: BTreeMap<G, GroupSlot<C, Accumulator, Output>>,
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
			} else if let Some(prior) = slot.prior_output {
				results.push(RollingResult {
					row_number: slot.row_number,
					group,
					value: prior,
					prior: None,
					kind: EmitKind::Remove,
				});
			}
		}
		Ok(results)
	}

	fn migrate_blob<S: WindowStore>(
		&mut self,
		store: &mut S,
		row_number: RowNumber,
	) -> Result<Option<Accumulator>> {
		let Some(blob) = self.buffers.get(store, &row_number)? else {
			return Ok(None);
		};
		for (coord, accumulator) in &blob {
			store.internal_set(&coord_entry_key(row_number, coord.order_key()), accumulator)?;
		}
		self.buffers.remove(store, &row_number)?;
		Ok(Some(bootstrap_running(&blob)))
	}

	fn load_running<S: WindowStore>(
		&mut self,
		store: &mut S,
		row_number: RowNumber,
		migrated: Option<Accumulator>,
	) -> Result<Accumulator> {
		let running_cache = self.running.as_mut().expect("runnable engine has a running cache");
		if let Some(running) = running_cache.get(store, &RunningKey(row_number))? {
			return Ok(running);
		}
		match migrated {
			Some(running) => Ok(running),
			None => scan_running(store, row_number),
		}
	}

	pub fn apply_running<S, K, NA>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		eviction: RollingEviction<C>,
		row_key: K,
		new_accumulator: NA,
	) -> Result<Vec<RollingResult<G, Accumulator::Output>>>
	where
		S: WindowStore,
		K: Fn(&G) -> EncodedKey,
		NA: Fn() -> Accumulator,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		reifydb_assertions! {
			assert!(
				self.running.is_some(),
				"apply_running requires an engine constructed with new_runnable"
			);
		}
		let RollingEviction::Before(evict_cutoff) = eviction else {
			unimplemented!("apply_running supports only Before eviction");
		};
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let buffer_rows = self.resolve_buffer_rows(store, &buckets, &meta_loaded, &row_key)?;
		if let Some(running) = &mut self.running {
			let running_keys: Vec<RunningKey> =
				buffer_rows.values().map(|(row_number, _)| RunningKey(*row_number)).collect();
			running.warm(store, &running_keys)?;
		}

		let mut group_slots: BTreeMap<G, RunnableGroupSlot<Accumulator>> = BTreeMap::new();
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
					let migrated = self.migrate_blob(store, row_number)?;
					let prior_min = peek_min_coord::<S, Accumulator>(store, row_number)?;
					let running = if prior_min.is_none() {
						Accumulator::default()
					} else {
						self.load_running(store, row_number, migrated)?
					};
					let was_empty_before = prior_min.is_none();
					let prior_output = if was_empty_before {
						None
					} else {
						running.finalize()
					};
					group_slots.insert(
						group.clone(),
						RunnableGroupSlot {
							row_number,
							is_new,
							running,
							was_empty_before,
							buffer_changed: false,
							prior_min,
							prior_output,
						},
					);
					group_slots.get_mut(&group).expect("just inserted")
				}
			};

			let entry_key = coord_entry_key(slot.row_number, coord.order_key());
			let existing: Option<Accumulator> = store.internal_get(&entry_key)?;

			let mut accumulator = existing.unwrap_or_else(&new_accumulator);
			let before = accumulator.clone();
			let mut touched = false;
			for event in events {
				match event {
					AccumulatorEvent::Add(c) => {
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
			if !before.is_empty() {
				slot.running.unmerge(&before);
			}
			if !accumulator.is_empty() {
				merge_into(&mut slot.running, &accumulator);
				store.internal_set(&entry_key, &accumulator)?;
			} else {
				store.internal_drop(&entry_key)?;
			}
			slot.buffer_changed = true;

			meta.high_water = Some(match meta.high_water {
				Some(hw) if hw > coord => hw,
				_ => coord,
			});
		}

		let mut results: Vec<RollingResult<G, Accumulator::Output>> = Vec::new();
		for (group, mut slot) in group_slots {
			if !slot.buffer_changed {
				continue;
			}
			let mut due: Vec<(EncodedKey, Accumulator)> = Vec::new();
			store.internal_range_visit::<Accumulator>(
				coord_due_range(slot.row_number, evict_cutoff.order_key()),
				None,
				&mut |key, accumulator| {
					due.push((key, accumulator));
					Ok(())
				},
			)?;
			for (key, evicted) in due {
				store.internal_drop(&key)?;
				slot.running.unmerge(&evicted);
			}
			let new_min = peek_min_coord::<S, Accumulator>(store, slot.row_number)?;
			if new_min != slot.prior_min {
				if let Some(old) = slot.prior_min {
					store.internal_drop(&expiry_key(old, &group, &[]))?;
				}
				if let Some(new) = new_min {
					store.internal_set(
						&expiry_key(new, &group, &[]),
						&RollingIndexEntry {
							group: group.clone(),
							row_number: slot.row_number.0,
						},
					)?;
				}
			}
			let output = if new_min.is_none() {
				None
			} else {
				slot.running.finalize()
			};
			let running_cache = self.running.as_mut().expect("runnable engine has a running cache");
			if new_min.is_none() {
				running_cache.remove(store, &RunningKey(slot.row_number))?;
			} else {
				running_cache.put(store, &RunningKey(slot.row_number), slot.running)?;
			}

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
			} else if let Some(prior) = slot.prior_output {
				results.push(RollingResult {
					row_number: slot.row_number,
					group,
					value: prior,
					prior: None,
					kind: EmitKind::Remove,
				});
			}
		}
		self.persist_meta(store, meta_loaded)?;
		Ok(results)
	}

	pub fn expire_before_running<S>(
		&mut self,
		store: &mut S,
		cutoff: C,
	) -> Result<Vec<RollingExpiry<G, Accumulator::Output>>>
	where
		S: WindowStore,
	{
		reifydb_assertions! {
			assert!(
				self.running.is_some(),
				"expire_before_running requires an engine constructed with new_runnable"
			);
		}
		let mut due: Vec<(EncodedKey, RollingIndexEntry<G>)> = Vec::new();
		store.internal_range_visit::<RollingIndexEntry<G>>(
			expiry_due_range(cutoff.order_key()),
			Some(self.expire_batch),
			&mut |key, entry| {
				due.push((key, entry));
				Ok(())
			},
		)?;

		let mut out: Vec<RollingExpiry<G, Accumulator::Output>> = Vec::new();
		for (index_key, entry) in due {
			let row_number = RowNumber(entry.row_number);
			store.internal_drop(&index_key)?;
			let migrated = self.migrate_blob(store, row_number)?;
			let mut expired: Vec<(EncodedKey, Accumulator)> = Vec::new();
			store.internal_range_visit::<Accumulator>(
				coord_due_range(row_number, cutoff.order_key()),
				None,
				&mut |key, accumulator| {
					expired.push((key, accumulator));
					Ok(())
				},
			)?;
			if expired.is_empty() {
				if let Some(new) = peek_min_coord::<S, Accumulator>(store, row_number)? {
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
			let mut running = self.load_running(store, row_number, migrated)?;
			for (key, accumulator) in expired {
				store.internal_drop(&key)?;
				running.unmerge(&accumulator);
			}
			let new_min = peek_min_coord::<S, Accumulator>(store, row_number)?;
			match (running.finalize(), new_min) {
				(Some(value), Some(new)) => {
					store.internal_set(
						&expiry_key(new, &entry.group, &[]),
						&RollingIndexEntry {
							group: entry.group.clone(),
							row_number: entry.row_number,
						},
					)?;
					let running_cache =
						self.running.as_mut().expect("runnable engine has a running cache");
					running_cache.put(store, &RunningKey(row_number), running)?;
					out.push(RollingExpiry::Update {
						row_number,
						group: entry.group,
						value,
					});
				}
				_ => {
					let mut leftover: Vec<EncodedKey> = Vec::new();
					store.internal_range_visit::<Accumulator>(
						coord_row_range(row_number),
						None,
						&mut |key, _accumulator| {
							leftover.push(key);
							Ok(())
						},
					)?;
					for key in leftover {
						store.internal_drop(&key)?;
					}
					self.buffers.remove(store, &row_number)?;
					let running_cache =
						self.running.as_mut().expect("runnable engine has a running cache");
					running_cache.remove(store, &RunningKey(row_number))?;
					out.push(RollingExpiry::Remove {
						row_number,
						group: entry.group,
					});
				}
			}
		}
		Ok(out)
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
			Some(self.expire_batch),
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
		store.internal_range_visit::<RollingIndexEntry<G>>(
			expiry_due_range(cutoff),
			Some(self.expire_batch),
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

	use reifydb_codec::key::encoded::EncodedKey;

	use crate::window::engine::{
		AccumulatorEvent, EmitKind,
		config::WindowEngineConfig,
		rolling::{
			RollingBuckets, RollingBuffer, RollingEngine, RollingEviction, RollingExpiry, RollingResult,
		},
		test_support::{MockStore, StampedSum, SumAccumulator},
	};

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder().state_cache_capacity(8).internal_state_cache_capacity(64).build()
	}

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
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
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
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
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
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
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
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
		assert!(engine.expire_before(&mut store, 1000, sum_combine).unwrap().is_empty());
	}

	#[test]
	fn expire_before_leaves_groups_whose_oldest_coord_is_not_due() {
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
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
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let out = engine.expire_before(&mut store, 5, sum_combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(out.len(), 1, "only the group with a due coord is processed");
		assert!(matches!(&out[0], RollingExpiry::Remove { group, .. } if *group == 2));
		assert_eq!(store.index_entry_count(), 1, "group 1 keeps its index entry");
	}

	#[test]
	fn expire_before_processes_at_most_expire_batch_then_resumes_next_tick() {
		// Same guard rail as the tumbling engine: a due-group burst must not be drained in a
		// single tick, because all node ticks run serialized in the flow actor and one bloated
		// operator would stall every other flow. Capped groups stay in the due index and drain
		// on later ticks. The due index sorts by inverted coord (encode_u64), so the scan
		// yields the newest-due groups first and the oldest backlog defers.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Add(1)]);
		buckets.insert((2u32, 20u64), vec![AccumulatorEvent::Add(2)]);
		buckets.insert((3u32, 30u64), vec![AccumulatorEvent::Add(3)]);
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
		assert_eq!(store.index_entry_count(), 3);

		let capped = WindowEngineConfig::builder()
			.state_cache_capacity(8)
			.internal_state_cache_capacity(64)
			.expire_batch(2)
			.build();

		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(capped);
		let first = engine.expire_before(&mut store, 1000, sum_combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(first.len(), 2, "one tick drains at most expire_batch groups");
		assert!(matches!(&first[0], RollingExpiry::Remove { group, .. } if *group == 3));
		assert!(matches!(&first[1], RollingExpiry::Remove { group, .. } if *group == 2));
		assert_eq!(store.index_entry_count(), 1, "the deferred group keeps its index entry");

		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(capped);
		let second = engine.expire_before(&mut store, 1000, sum_combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(second.len(), 1, "the next tick picks up the deferred group");
		assert!(matches!(&second[0], RollingExpiry::Remove { group, .. } if *group == 1));
		assert_eq!(store.index_entry_count(), 0);
	}

	#[test]
	fn expire_before_stamp_evicts_by_accumulator_stamp() {
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, u64, StampedSum>::new(test_config());
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
		let mut engine = RollingEngine::<u32, u64, StampedSum>::new(test_config());
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

	#[test]
	fn withdrawn_value_is_reconstructed_after_restart() {
		// The terminal Remove emitted when a rolling group empties must carry the value that was
		// last published for that group. `prior_output` is never persisted; it is recomputed as
		// `combine(buffer)` from the persisted buffer at the start of the batch. This test drops the
		// engine between the publish and the retraction (a restart / panic-recovery) and asserts the
		// withdrawn value still equals the originally published value. That proves the reconstruction
		// is exact and depends on no in-memory state - it holds only because `combine` is a pure
		// function of the persisted buffer. If a future combine read non-persisted state, or the
		// reconstruction were sourced from an ephemeral cache instead of the buffer, the second engine
		// would withdraw a wrong or empty value and this test would fail.
		let mut store = MockStore::default();

		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Add(5)]);
		let published: Vec<RollingResult<u32, i64>> =
			engine.apply(&mut store, buckets, 4, row_key, sum_combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(published.len(), 1);
		assert!(matches!(published[0].kind, EmitKind::Insert));
		assert_eq!(published[0].value, 5);

		// Restart: a brand new engine with no in-memory GroupSlot / prior_output, reading only the
		// persisted buffer left behind by the first engine.
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Remove(5)]);
		let withdrawn: Vec<RollingResult<u32, i64>> =
			engine.apply(&mut store, buckets, 4, row_key, sum_combine).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(withdrawn.len(), 1, "emptying the group emits exactly one terminal diff");
		assert!(
			matches!(withdrawn[0].kind, EmitKind::Remove),
			"the group emptied under retraction, so the last published row must be withdrawn"
		);
		assert_eq!(
			withdrawn[0].value, 5,
			"the withdrawn value is the reconstructed last-published output, not a stale or zeroed value"
		);
		assert_eq!(
			withdrawn[0].row_number, published[0].row_number,
			"the withdrawal targets the same row that was published"
		);
	}

	#[test]
	fn buffer_survives_lru_eviction() {
		// The other way a read reaches the store is LRU eviction, no restart needed: the state cache
		// holds only 8 groups, so tracking more evicts the oldest and the next access re-reads it from
		// the store. This exercises the same persist/reload path as the restart test within a single
		// long-lived engine. We publish 11 groups so group 1 is evicted, flush, then retract group 1
		// and assert its buffer is read back intact - the terminal Remove carries the originally
		// published value. It would fail if the buffer failed to round-trip through the store (a
		// serialization break, or a second Data cache colliding on the same key).
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());

		let mut published_group_1: Vec<RollingResult<u32, i64>> = Vec::new();
		for group in 1u32..=11u32 {
			let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
			buckets.insert((group, 10u64), vec![AccumulatorEvent::Add(i64::from(group))]);
			let out: Vec<RollingResult<u32, i64>> =
				engine.apply(&mut store, buckets, 4, row_key, sum_combine).unwrap();
			if group == 1 {
				published_group_1 = out;
			}
		}
		engine.flush(&mut store).unwrap();
		assert_eq!(published_group_1.len(), 1);
		assert!(matches!(published_group_1[0].kind, EmitKind::Insert));
		assert_eq!(published_group_1[0].value, 1);

		// Group 1 was published first and pushed out of the 8-slot cache by the later groups, so the
		// same engine must re-read its buffer from the store to apply this retraction.
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Remove(1)]);
		let withdrawn: Vec<RollingResult<u32, i64>> =
			engine.apply(&mut store, buckets, 4, row_key, sum_combine).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(withdrawn.len(), 1, "emptying the evicted group emits exactly one terminal diff");
		assert!(
			matches!(withdrawn[0].kind, EmitKind::Remove),
			"the evicted group emptied under retraction, so the last published row must be withdrawn"
		);
		assert_eq!(
			withdrawn[0].value, 1,
			"the withdrawn value is reconstructed from the evicted group's persisted buffer"
		);
		assert_eq!(
			withdrawn[0].row_number, published_group_1[0].row_number,
			"the withdrawal targets the same row that was published for group 1"
		);
	}

	fn describe(results: &[RollingResult<u32, i64>]) -> Vec<(u32, EmitKind, i64)> {
		results.iter().map(|r| (r.group, r.kind, r.value)).collect()
	}

	fn describe_expiries(expiries: &[RollingExpiry<u32, i64>]) -> Vec<(u32, Option<i64>)> {
		expiries.iter()
			.map(|e| match e {
				RollingExpiry::Update {
					group,
					value,
					..
				} => (*group, Some(*value)),
				RollingExpiry::Remove {
					group,
					..
				} => (*group, None),
			})
			.collect()
	}

	#[test]
	fn runnable_engine_matches_legacy_recombine_across_seeded_churn() {
		// The runnable engine replaces the O(buffer) recombine with a running
		// accumulator maintained by merge/unmerge. Its observable behavior -
		// emitted kinds, values, expiry updates, terminal removes, and the
		// expiry-index bookkeeping - must be indistinguishable from the legacy
		// engine on an identical seeded add/remove/expire workload; integer sums
		// make the comparison exact. A divergence means the running maintenance
		// missed a mutation path.
		let mut legacy_store = MockStore::default();
		let mut runnable_store = MockStore::default();
		let mut legacy = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut runnable = RollingEngine::<u32, u64, SumAccumulator>::new_runnable(test_config());

		let mut state = 0xDEAD_BEEF_CAFE_1234u64;
		let mut roll = |bound: u64| {
			state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			(state >> 33) % bound
		};
		let mut coord_base = 100u64;
		let mut cutoff = 0u64;
		let mut added: Vec<(u32, u64, i64)> = Vec::new();

		for round in 0..200u64 {
			let mut plan: Vec<(u32, u64, i64, bool)> = Vec::new();
			for _ in 0..=roll(3) {
				let group = roll(5) as u32;
				let coord = coord_base + roll(40);
				let value = roll(1_000) as i64 + 1;
				plan.push((group, coord, value, true));
				added.push((group, coord, value));
			}
			if round % 4 == 3 && !added.is_empty() {
				let (group, coord, value) = added.remove((roll(added.len() as u64)) as usize);
				plan.push((group, coord, value, false));
			}
			let build = |plan: &[(u32, u64, i64, bool)]| {
				let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
				for &(group, coord, value, is_add) in plan {
					let event = if is_add {
						AccumulatorEvent::Add(value)
					} else {
						AccumulatorEvent::Remove(value)
					};
					buckets.entry((group, coord)).or_default().push(event);
				}
				buckets
			};
			let legacy_out = legacy
				.apply_evicting(
					&mut legacy_store,
					build(&plan),
					RollingEviction::Before(cutoff),
					row_key,
					SumAccumulator::default,
					sum_combine,
				)
				.unwrap();
			let runnable_out = runnable
				.apply_running(
					&mut runnable_store,
					build(&plan),
					RollingEviction::Before(cutoff),
					row_key,
					SumAccumulator::default,
				)
				.unwrap();
			assert_eq!(
				describe(&legacy_out),
				describe(&runnable_out),
				"apply diverged from the legacy recombine at round {round}"
			);

			if round % 5 == 4 {
				cutoff = coord_base.saturating_sub(30);
				let legacy_exp = legacy.expire_before(&mut legacy_store, cutoff, sum_combine).unwrap();
				let runnable_exp = runnable.expire_before_running(&mut runnable_store, cutoff).unwrap();
				assert_eq!(
					describe_expiries(&legacy_exp),
					describe_expiries(&runnable_exp),
					"expiry diverged from the legacy recombine at round {round}"
				);
				added.retain(|(_, coord, _)| *coord > cutoff);
			}
			coord_base += roll(20);
		}

		legacy.flush(&mut legacy_store).unwrap();
		runnable.flush(&mut runnable_store).unwrap();
		assert_eq!(
			legacy_store.index_entry_count(),
			runnable_store.index_entry_count(),
			"expiry-index bookkeeping diverged"
		);

		// Drain everything: terminal removes must match group-for-group.
		let legacy_final = legacy.expire_before(&mut legacy_store, u64::MAX - 1, sum_combine).unwrap();
		let runnable_final = runnable.expire_before_running(&mut runnable_store, u64::MAX - 1).unwrap();
		assert_eq!(
			describe_expiries(&legacy_final),
			describe_expiries(&runnable_final),
			"terminal drain diverged"
		);
		assert!(
			legacy_final.iter().all(|e| matches!(e, RollingExpiry::Remove { .. })),
			"draining past every coord must terminally remove all groups"
		);
	}

	#[test]
	fn runnable_engine_bootstraps_running_from_a_legacy_buffer() {
		// First run after the fix deploys over persisted pre-fix state: buffers
		// exist, running entries do not. The runnable engine must recombine the
		// buffer once (bootstrap) instead of treating the group as empty, both
		// on the apply path and on the expiry path.
		let mut store = MockStore::default();
		let mut legacy = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Add(5)]);
		buckets.insert((1u32, 20u64), vec![AccumulatorEvent::Add(7)]);
		legacy.apply_evicting(
			&mut store,
			buckets,
			RollingEviction::Before(0),
			row_key,
			SumAccumulator::default,
			sum_combine,
		)
		.unwrap();
		legacy.flush(&mut store).unwrap();

		let mut runnable = RollingEngine::<u32, u64, SumAccumulator>::new_runnable(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 30u64), vec![AccumulatorEvent::Add(100)]);
		let out = runnable
			.apply_running(
				&mut store,
				buckets,
				RollingEviction::Before(0),
				row_key,
				SumAccumulator::default,
			)
			.unwrap();
		assert_eq!(
			describe(&out),
			vec![(1u32, EmitKind::Update, 112i64)],
			"bootstrap must fold the pre-existing buffer into the running sum"
		);

		let expired = runnable.expire_before_running(&mut store, 20).unwrap();
		assert_eq!(
			describe_expiries(&expired),
			vec![(1u32, Some(100i64))],
			"expiring the pre-fix coords must subtract exactly their contributions"
		);
		runnable.flush(&mut store).unwrap();

		// A fresh runnable engine over the flushed state reads the persisted
		// running entry back (no bootstrap) and drains to a terminal remove.
		let mut reopened = RollingEngine::<u32, u64, SumAccumulator>::new_runnable(test_config());
		let drained = reopened.expire_before_running(&mut store, u64::MAX - 1).unwrap();
		assert_eq!(
			describe_expiries(&drained),
			vec![(1u32, None)],
			"the last coord expiring must terminally remove"
		);
	}

	#[test]
	fn per_coord_storage_leaves_nothing_behind_after_terminal_drain() {
		// Per-coord persistence must clean up completely: after every group
		// expires, no coord entries, running entries, or expiry-index entries
		// may remain, and a migrated legacy blob must be gone the moment the
		// runnable engine first touches its group. Leaked entries are exactly
		// the kind of unbounded state growth this engine exists to prevent.
		let mut store = MockStore::default();
		let mut legacy = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Add(5)]);
		buckets.insert((1u32, 20u64), vec![AccumulatorEvent::Add(7)]);
		legacy.apply_evicting(
			&mut store,
			buckets,
			RollingEviction::Before(0),
			row_key,
			SumAccumulator::default,
			sum_combine,
		)
		.unwrap();
		legacy.flush(&mut store).unwrap();
		assert_eq!(store.blob_entry_count(), 1, "legacy engine persists the buffer blob under a state key");

		let mut runnable = RollingEngine::<u32, u64, SumAccumulator>::new_runnable(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((2u32, 30u64), vec![AccumulatorEvent::Add(1)]);
		buckets.insert((1u32, 30u64), vec![AccumulatorEvent::Add(100)]);
		runnable.apply_running(
			&mut store,
			buckets,
			RollingEviction::Before(0),
			row_key,
			SumAccumulator::default,
		)
		.unwrap();
		runnable.flush(&mut store).unwrap();
		assert_eq!(store.blob_entry_count(), 0, "migrating a group must delete its legacy buffer blob");
		assert_eq!(store.coord_entry_count(), 4, "each live coord is its own internal entry");
		assert_eq!(store.running_entry_count(), 2, "each live group persists one running entry");

		let drained = runnable.expire_before_running(&mut store, u64::MAX - 1).unwrap();
		runnable.flush(&mut store).unwrap();
		assert_eq!(drained.len(), 2, "both groups drain");
		assert!(drained.iter().all(|e| matches!(e, RollingExpiry::Remove { .. })));
		assert_eq!(store.coord_entry_count(), 0, "terminal removal must delete every coord entry");
		assert_eq!(store.running_entry_count(), 0, "terminal removal must delete the running entry");
		assert_eq!(store.index_entry_count(), 0, "terminal removal must delete the expiry index entry");
	}
}
