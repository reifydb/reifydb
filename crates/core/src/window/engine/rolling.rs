// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
	ops::Bound,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	state::OperatorState,
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};

use crate::{
	key::operator_state::{GroupId, GroupSet},
	metrics::heap::{HeapSize, StateCompleteness, StateMemory},
	state::{
		cache::{StateCache, StateView},
		map::PersistedMap,
		store::StateStore,
	},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, BatchMeta, BufferKey, EmitKind, GroupMeta, MetaHighWater, MetaKey,
			RunningKey, buffer_range, config::WindowEngineConfig, decode_buffer_key, decode_meta_key,
			decode_running_key, expiry::ExpiryIndex, expiry_key, load_batch_meta, meta_key_for, meta_range,
			persist_batch_meta, running_range, sweep_stale_meta,
		},
		span::Slot,
	},
};

pub type RollingBuffer<C, Accumulator> = PersistedMap<C, Accumulator>;

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
		group_id: GroupId,
		value: Output,
	},
	Remove {
		row_number: RowNumber,
		group: G,
		group_id: GroupId,
	},
}

#[derive(Clone, Copy)]
enum IndexMode {
	Coord,
	Stamp,
}

#[operator_state]
#[derive(Clone)]
pub struct RollingIndexEntry<G> {
	group: G,
	row_number: u64,
	group_id: u64,
}

fn coord_min_key<C: Slot, A>(buffer: &RollingBuffer<C, A>) -> Option<u64> {
	buffer.keys().next().map(|c| c.order_key())
}

fn stamp_min_key<C, A: WindowAccumulator>(buffer: &RollingBuffer<C, A>) -> Option<u64> {
	buffer.values().filter_map(|a| a.stamp()).min()
}

type MetaLoaded<G, C> = HashMap<G, BatchMeta<C>>;
type BufferRows<G> = HashMap<G, (GroupId, RowNumber, bool)>;

struct GroupSlot<C, Accumulator, Output> {
	group_id: GroupId,
	row_number: RowNumber,
	is_new: bool,
	buffer: RollingBuffer<C, Accumulator>,
	was_empty_before: bool,
	buffer_changed: bool,
	prior_index_key: Option<u64>,
	prior_output: Option<Output>,
}

pub struct RollingEngine<G, C, Accumulator> {
	buffers: StateCache<BufferKey, RollingBuffer<C, Accumulator>>,
	running: Option<StateCache<RunningKey, Accumulator>>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	expiry: ExpiryIndex<RollingIndexEntry<G>>,
	meta_low_water: Option<u64>,
	expire_batch: usize,
	lag: u64,
	hydrated: bool,
	group_scoped: bool,
	_pd: PhantomData<G>,
}

struct RunnableGroupSlot<C, Accumulator>
where
	Accumulator: WindowAccumulator,
{
	group_id: GroupId,
	row_number: RowNumber,
	is_new: bool,
	buffer: RollingBuffer<C, Accumulator>,
	running: Accumulator,
	was_empty_before: bool,
	buffer_changed: bool,
	prior_min: Option<u64>,
	old_frontier: Option<u64>,
	prior_output: Option<Accumulator::Output>,
}

fn merge_into<A: WindowAccumulator>(running: &mut A, other: &A) {
	if running.is_empty() {
		*running = other.clone();
	} else {
		running.merge(other);
	}
}

fn frontier_for<C: Slot>(lag: u64, high_water: &Option<C>) -> Option<u64> {
	if lag == 0 {
		Some(u64::MAX)
	} else {
		high_water.as_ref().map(|hw| hw.order_key().saturating_sub(lag))
	}
}

fn is_merged_coord(coord: u64, frontier: Option<u64>) -> bool {
	frontier.is_some_and(|f| coord <= f)
}

fn running_below<C: Slot, A: WindowAccumulator>(buffer: &RollingBuffer<C, A>, frontier: Option<u64>) -> A {
	let mut running = A::default();
	let Some(frontier) = frontier else {
		return running;
	};
	for (coord, accumulator) in buffer.iter() {
		if coord.order_key() > frontier {
			break;
		}
		merge_into(&mut running, accumulator);
	}
	running
}

impl<G, C, Accumulator> RollingEngine<G, C, Accumulator>
where
	G: Clone + Eq + Ord + Hash + Debug,
	C: Slot + Hash + HeapSize,
	Accumulator: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
	GroupMeta<C>: OperatorState,
	RollingIndexEntry<G>: OperatorState,
	RollingBuffer<C, Accumulator>: OperatorState,
{
	/// Groups addressed at node scope: one contiguous buffer range, so the cache can
	/// prove absence by hydrating it.
	pub fn new(config: WindowEngineConfig) -> Self {
		Self::scoped(config, false)
	}

	/// Groups interned in the substrate, so a partition's buffer reclaims with it. Each
	/// one sits inside its own group rather than in a shared range, so there is nothing
	/// to hydrate: absence is proven by the caller minting a fresh row number instead.
	pub fn group_scoped(config: WindowEngineConfig) -> Self {
		Self::scoped(config, true)
	}

	fn scoped(config: WindowEngineConfig, group_scoped: bool) -> Self {
		Self {
			buffers: StateCache::<BufferKey, RollingBuffer<C, Accumulator>>::new_internal(config.budget()),
			running: None,
			meta: StateCache::<MetaKey, GroupMeta<C>>::new_internal(config.budget()),
			expiry: ExpiryIndex::new(),
			meta_low_water: None,
			expire_batch: config.expire_batch(),
			lag: 0,
			hydrated: false,
			group_scoped,
			_pd: PhantomData,
		}
	}

	fn hydrate_once<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		if self.hydrated {
			return Ok(());
		}
		if !self.group_scoped {
			self.buffers.hydrate(store, buffer_range(), decode_buffer_key)?;
			if let Some(running) = &mut self.running {
				running.hydrate(store, running_range(), decode_running_key)?;
			}
		}
		self.meta.hydrate(store, meta_range(), decode_meta_key)?;
		self.hydrated = true;
		Ok(())
	}

	pub fn new_runnable(config: WindowEngineConfig) -> Self {
		Self::runnable(config, false)
	}

	pub fn new_runnable_group_scoped(config: WindowEngineConfig) -> Self {
		Self::runnable(config, true)
	}

	fn runnable(config: WindowEngineConfig, group_scoped: bool) -> Self {
		let running = StateCache::<RunningKey, Accumulator>::new_internal(config.budget());
		let mut engine = Self::scoped(config, group_scoped);
		engine.running = Some(running);
		engine
	}

	pub fn invalidate_groups(&mut self, groups: &GroupSet) -> usize {
		let mut dropped = self.buffers.invalidate_group_data(groups);
		if let Some(running) = &mut self.running {
			dropped += running.invalidate_group_data(groups);
		}
		dropped
	}

	pub fn with_lag(mut self, lag: u64) -> Self {
		self.lag = lag;
		self
	}

	pub fn approximate_memory(&self) -> StateMemory {
		let mut memory = self.meta.approximate_memory()
			+ self.buffers.approximate_memory()
			+ self.expiry.approximate_memory();
		if let Some(running) = &self.running {
			memory = memory + running.approximate_memory();
		}
		memory
	}

	pub fn dirty_memory(&self) -> StateMemory {
		let mut memory = self.meta.dirty_memory() + self.buffers.dirty_memory();
		if let Some(running) = &self.running {
			memory = memory + running.dirty_memory();
		}
		memory
	}

	pub fn membership_memory(&self) -> StateMemory {
		let mut memory = self.meta.membership_memory() + self.buffers.membership_memory();
		if let Some(running) = &self.running {
			memory = memory + running.membership_memory();
		}
		memory
	}

	pub fn completeness(&self) -> StateCompleteness {
		let mut completeness = self.meta.completeness().merge(self.buffers.completeness());
		if let Some(running) = &self.running {
			completeness = completeness.merge(running.completeness());
		}
		completeness
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
		S: StateStore,
		K: Fn(&G) -> (GroupId, EncodedKey),
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
		S: StateStore,
		K: Fn(&G) -> (GroupId, EncodedKey),
		NA: Fn() -> Accumulator,
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		self.hydrate_once(store)?;
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

	pub fn flush<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		self.buffers.flush(store)?;
		if let Some(running) = &mut self.running {
			running.flush(store)?;
		}
		self.meta.flush(store)?;
		Ok(())
	}

	fn warm_and_load_meta<S: StateStore>(
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
				let batch = load_batch_meta(store, &mut self.meta, &meta_key_for(group))?;
				meta_loaded.insert(group.clone(), batch);
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
		S: StateStore,
		K: Fn(&G) -> (GroupId, EncodedKey),
	{
		let mut buffer_rows: BufferRows<G> = HashMap::new();
		let mut seen: BTreeSet<G> = BTreeSet::new();
		for (group, coord) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.initial);
			if initial_high_water.is_none_or(|hw| *coord >= hw) && seen.insert(group.clone()) {
				let (id, key) = row_key(group);
				let (row_number, is_new) = store.get_or_create_row_number(id, &key)?;
				buffer_rows.insert(group.clone(), (id, row_number, is_new));
			}
		}
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
		S: StateStore,
		K: Fn(&G) -> (GroupId, EncodedKey),
		NA: Fn() -> Accumulator,
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		let mut group_slots: BTreeMap<G, GroupSlot<C, Accumulator, Output>> = BTreeMap::new();

		for ((group, coord), events) in buckets {
			let meta = meta_loaded.entry(group.clone()).or_default();

			let slot = match group_slots.get_mut(&group) {
				Some(s) => s,
				None => {
					let (group_id, row_number, is_new) = match buffer_rows.get(&group) {
						Some(&resolved) => resolved,
						None => {
							let (id, key) = row_key(&group);
							let (row_number, is_new) =
								store.get_or_create_row_number(id, &key)?;
							(id, row_number, is_new)
						}
					};
					let buffer: RollingBuffer<C, Accumulator> = self
						.buffers
						.get(store, &BufferKey::new(group_id, row_number))?
						.unwrap_or_default();
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
							group_id,
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

			meta.observe(coord);
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
		S: StateStore,
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
						self.expiry.drop_key(store, &expiry_key(old, &group, &[]))?;
					}
					if let Some(new) = new_index_key {
						self.expiry.set(
							store,
							expiry_key(new, &group, &[]),
							RollingIndexEntry {
								group: group.clone(),
								row_number: slot.row_number.0,
								group_id: slot.group_id.0,
							},
						)?;
					}
				}
			}
			let output = combine(&group, &slot.buffer);
			if slot.buffer.is_empty() {
				self.buffers.remove(store, &BufferKey::new(slot.group_id, slot.row_number))?;
			} else {
				self.buffers.put(
					store,
					&BufferKey::new(slot.group_id, slot.row_number),
					slot.buffer,
				)?;
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
		Ok(results)
	}

	fn load_running<S: StateStore>(
		&mut self,
		store: &mut S,
		buffer: &RollingBuffer<C, Accumulator>,
		group_id: GroupId,
		row_number: RowNumber,
		frontier: Option<u64>,
	) -> Result<Accumulator> {
		let running_cache = self.running.as_mut().expect("runnable engine has a running cache");
		if let Some(running) = running_cache.get(store, &RunningKey::new(group_id, row_number))? {
			return Ok(running);
		}
		Ok(running_below(buffer, frontier))
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
		S: StateStore,
		K: Fn(&G) -> (GroupId, EncodedKey),
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
		self.hydrate_once(store)?;
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let buffer_rows = self.resolve_buffer_rows(store, &buckets, &meta_loaded, &row_key)?;
		if let Some(running) = &mut self.running {
			let running_keys: Vec<RunningKey> =
				buffer_rows.values().map(|(group, row, _)| RunningKey::new(*group, *row)).collect();
			running.warm(store, &running_keys)?;
		}

		let mut group_slots: BTreeMap<G, RunnableGroupSlot<C, Accumulator>> = BTreeMap::new();
		for ((group, coord), events) in buckets {
			let meta = meta_loaded.entry(group.clone()).or_default();

			let slot = match group_slots.get_mut(&group) {
				Some(s) => s,
				None => {
					let (group_id, row_number, is_new) = match buffer_rows.get(&group) {
						Some(&resolved) => resolved,
						None => {
							let (id, key) = row_key(&group);
							let (row_number, is_new) =
								store.get_or_create_row_number(id, &key)?;
							(id, row_number, is_new)
						}
					};
					let buffer: RollingBuffer<C, Accumulator> = self
						.buffers
						.get(store, &BufferKey::new(group_id, row_number))?
						.unwrap_or_default();
					let old_frontier = frontier_for(self.lag, &meta.high_water());
					let prior_min = coord_min_key(&buffer);
					let merged_before = prior_min.is_some_and(|m| is_merged_coord(m, old_frontier));
					let running = if merged_before {
						self.load_running(store, &buffer, group_id, row_number, old_frontier)?
					} else {
						Accumulator::default()
					};
					let was_empty_before = !merged_before;
					let prior_output = if merged_before {
						running.finalize()
					} else {
						None
					};
					group_slots.insert(
						group.clone(),
						RunnableGroupSlot {
							group_id,
							row_number,
							is_new,
							buffer,
							running,
							was_empty_before,
							buffer_changed: false,
							prior_min,
							old_frontier,
							prior_output,
						},
					);
					group_slots.get_mut(&group).expect("just inserted")
				}
			};

			let mut accumulator = slot.buffer.get(&coord).cloned().unwrap_or_else(&new_accumulator);
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
			if is_merged_coord(coord.order_key(), slot.old_frontier) {
				if !before.is_empty() {
					slot.running.unmerge(&before);
				}
				if !accumulator.is_empty() {
					merge_into(&mut slot.running, &accumulator);
				}
			}
			if !accumulator.is_empty() {
				slot.buffer.insert(coord, accumulator);
			} else {
				slot.buffer.remove(&coord);
			}
			slot.buffer_changed = true;

			meta.observe(coord);
		}

		let mut results: Vec<RollingResult<G, Accumulator::Output>> = Vec::new();
		for (group, mut slot) in group_slots {
			if !slot.buffer_changed {
				continue;
			}
			let high_water = meta_loaded.get(&group).expect("touched group has loaded meta").high_water();
			let new_frontier = frontier_for(self.lag, &high_water);
			if new_frontier > slot.old_frontier
				&& let Some(upto) = new_frontier
			{
				let lo = match slot.old_frontier {
					Some(after) => Bound::Excluded(C::from_order_key(after)),
					None => Bound::Unbounded,
				};
				let running = &mut slot.running;
				for (_, accumulator) in
					slot.buffer.range((lo, Bound::Included(C::from_order_key(upto))))
				{
					merge_into(running, accumulator);
				}
			}
			let due: Vec<C> = slot.buffer.range(..=evict_cutoff).map(|(coord, _)| *coord).collect();
			for coord in due {
				let Some(evicted) = slot.buffer.remove(&coord) else {
					continue;
				};
				if is_merged_coord(coord.order_key(), new_frontier) {
					slot.running.unmerge(&evicted);
				}
			}
			let new_min = coord_min_key(&slot.buffer);
			if new_min != slot.prior_min {
				if let Some(old) = slot.prior_min {
					self.expiry.drop_key(store, &expiry_key(old, &group, &[]))?;
				}
				if let Some(new) = new_min {
					self.expiry.set(
						store,
						expiry_key(new, &group, &[]),
						RollingIndexEntry {
							group: group.clone(),
							row_number: slot.row_number.0,
							group_id: slot.group_id.0,
						},
					)?;
				}
			}
			let merged_any = new_min.is_some_and(|m| is_merged_coord(m, new_frontier));
			let output = if merged_any {
				slot.running.finalize()
			} else {
				None
			};
			if slot.buffer.is_empty() {
				self.buffers.remove(store, &BufferKey::new(slot.group_id, slot.row_number))?;
			} else {
				self.buffers.put(
					store,
					&BufferKey::new(slot.group_id, slot.row_number),
					slot.buffer,
				)?;
			}
			let running_cache = self.running.as_mut().expect("runnable engine has a running cache");
			if merged_any {
				running_cache.put(
					store,
					&RunningKey::new(slot.group_id, slot.row_number),
					slot.running,
				)?;
			} else {
				running_cache.remove(store, &RunningKey::new(slot.group_id, slot.row_number))?;
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
		S: StateStore,
	{
		reifydb_assertions! {
			assert!(
				self.running.is_some(),
				"expire_before_running requires an engine constructed with new_runnable"
			);
		}
		self.hydrate_once(store)?;
		let due = self.expiry.due(store, cutoff.order_key(), self.expire_batch)?;

		let mut out: Vec<RollingExpiry<G, Accumulator::Output>> = Vec::new();
		for (index_key, entry) in due {
			let row_number = RowNumber(entry.row_number);
			let group_id = GroupId(entry.group_id);
			self.expiry.drop_key(store, &index_key)?;
			let frontier = if self.lag == 0 {
				Some(u64::MAX)
			} else {
				let lag = self.lag;
				self.meta
					.read(store, &meta_key_for(&entry.group), |view| match view {
						StateView::Archived(meta) => {
							GroupMeta::<C>::archived_high_water_order(meta)
								.map(|hw| hw.saturating_sub(lag))
						}
						StateView::Native(meta) => frontier_for(lag, &meta.high_water),
					})?
					.flatten()
			};
			let mut buffer: RollingBuffer<C, Accumulator> =
				self.buffers.get(store, &BufferKey::new(group_id, row_number))?.unwrap_or_default();
			let expired: Vec<C> = buffer.range(..=cutoff).map(|(coord, _)| *coord).collect();
			if expired.is_empty() {
				if let Some(new) = coord_min_key(&buffer) {
					self.expiry.set(
						store,
						expiry_key(new, &entry.group, &[]),
						RollingIndexEntry {
							group: entry.group.clone(),
							row_number: entry.row_number,
							group_id: entry.group_id,
						},
					)?;
				}
				continue;
			}
			let mut running = self.load_running(store, &buffer, group_id, row_number, frontier)?;
			let mut unmerged_any = false;
			for coord in expired {
				let Some(accumulator) = buffer.remove(&coord) else {
					continue;
				};
				if is_merged_coord(coord.order_key(), frontier) {
					running.unmerge(&accumulator);
					unmerged_any = true;
				}
			}
			let new_min = coord_min_key(&buffer);
			let merged_any = new_min.is_some_and(|m| is_merged_coord(m, frontier));
			let finalized = if merged_any {
				running.finalize()
			} else {
				None
			};
			match (new_min, merged_any, finalized) {
				(Some(new), true, Some(value)) => {
					self.expiry.set(
						store,
						expiry_key(new, &entry.group, &[]),
						RollingIndexEntry {
							group: entry.group.clone(),
							row_number: entry.row_number,
							group_id: entry.group_id,
						},
					)?;
					self.buffers.put(store, &BufferKey::new(group_id, row_number), buffer)?;
					let running_cache =
						self.running.as_mut().expect("runnable engine has a running cache");
					running_cache.put(store, &RunningKey::new(group_id, row_number), running)?;
					out.push(RollingExpiry::Update {
						row_number,
						group: entry.group,
						group_id,
						value,
					});
				}
				(Some(new), false, _) => {
					self.expiry.set(
						store,
						expiry_key(new, &entry.group, &[]),
						RollingIndexEntry {
							group: entry.group.clone(),
							row_number: entry.row_number,
							group_id: entry.group_id,
						},
					)?;
					self.buffers.put(store, &BufferKey::new(group_id, row_number), buffer)?;
					let running_cache =
						self.running.as_mut().expect("runnable engine has a running cache");
					running_cache.remove(store, &RunningKey::new(group_id, row_number))?;
					if unmerged_any {
						out.push(RollingExpiry::Remove {
							row_number,
							group: entry.group,
							group_id,
						});
					}
				}
				_ => {
					self.buffers.remove(store, &BufferKey::new(group_id, row_number))?;
					let running_cache =
						self.running.as_mut().expect("runnable engine has a running cache");
					running_cache.remove(store, &RunningKey::new(group_id, row_number))?;
					out.push(RollingExpiry::Remove {
						row_number,
						group: entry.group,
						group_id,
					});
				}
			}
		}
		Ok(out)
	}

	pub fn expire_meta<S: StateStore>(&mut self, store: &mut S, threshold: u64) -> Result<usize> {
		sweep_stale_meta(store, &mut self.meta, threshold, &mut self.meta_low_water)
	}

	pub fn expire_before<S, CB, Output>(
		&mut self,
		store: &mut S,
		cutoff: C,
		combine: CB,
	) -> Result<Vec<RollingExpiry<G, Output>>>
	where
		S: StateStore,
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		self.hydrate_once(store)?;
		let due = self.expiry.due(store, cutoff.order_key(), self.expire_batch)?;

		let mut out: Vec<RollingExpiry<G, Output>> = Vec::new();
		for (index_key, entry) in due {
			let row_number = RowNumber(entry.row_number);
			let group_id = GroupId(entry.group_id);
			self.expiry.drop_key(store, &index_key)?;
			let mut buffer: RollingBuffer<C, Accumulator> =
				self.buffers.get(store, &BufferKey::new(group_id, row_number))?.unwrap_or_default();
			if buffer.is_empty() {
				continue;
			}
			let before = buffer.len();
			buffer.retain(|&coord, _| coord > cutoff);
			if buffer.len() == before {
				if let Some(new) = coord_min_key(&buffer) {
					self.expiry.set(
						store,
						expiry_key(new, &entry.group, &[]),
						RollingIndexEntry {
							group: entry.group.clone(),
							row_number: entry.row_number,
							group_id: entry.group_id,
						},
					)?;
				}
				continue;
			}
			match combine(&entry.group, &buffer) {
				Some(value) if !buffer.is_empty() => {
					if let Some(new) = coord_min_key(&buffer) {
						self.expiry.set(
							store,
							expiry_key(new, &entry.group, &[]),
							RollingIndexEntry {
								group: entry.group.clone(),
								row_number: entry.row_number,
								group_id: entry.group_id,
							},
						)?;
					}
					self.buffers.put(store, &BufferKey::new(group_id, row_number), buffer)?;
					out.push(RollingExpiry::Update {
						row_number,
						group: entry.group,
						group_id,
						value,
					});
				}
				_ => {
					self.buffers.remove(store, &BufferKey::new(group_id, row_number))?;
					out.push(RollingExpiry::Remove {
						row_number,
						group: entry.group,
						group_id,
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
		S: StateStore,
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		self.hydrate_once(store)?;
		let due = self.expiry.due(store, cutoff, self.expire_batch)?;

		let mut out: Vec<RollingExpiry<G, Output>> = Vec::new();
		for (index_key, entry) in due {
			let row_number = RowNumber(entry.row_number);
			let group_id = GroupId(entry.group_id);
			self.expiry.drop_key(store, &index_key)?;
			let mut buffer: RollingBuffer<C, Accumulator> =
				self.buffers.get(store, &BufferKey::new(group_id, row_number))?.unwrap_or_default();
			if buffer.is_empty() {
				continue;
			}
			let before = buffer.len();
			buffer.retain(|_, accumulator| accumulator.stamp().is_none_or(|s| s > cutoff));
			if buffer.len() == before {
				if let Some(new) = stamp_min_key(&buffer) {
					self.expiry.set(
						store,
						expiry_key(new, &entry.group, &[]),
						RollingIndexEntry {
							group: entry.group.clone(),
							row_number: entry.row_number,
							group_id: entry.group_id,
						},
					)?;
				}
				continue;
			}
			match combine(&entry.group, &buffer) {
				Some(value) if !buffer.is_empty() => {
					if let Some(new) = stamp_min_key(&buffer) {
						self.expiry.set(
							store,
							expiry_key(new, &entry.group, &[]),
							RollingIndexEntry {
								group: entry.group.clone(),
								row_number: entry.row_number,
								group_id: entry.group_id,
							},
						)?;
					}
					self.buffers.put(store, &BufferKey::new(group_id, row_number), buffer)?;
					out.push(RollingExpiry::Update {
						row_number,
						group: entry.group,
						group_id,
						value,
					});
				}
				_ => {
					self.buffers.remove(store, &BufferKey::new(group_id, row_number))?;
					out.push(RollingExpiry::Remove {
						row_number,
						group: entry.group,
						group_id,
					});
				}
			}
		}
		Ok(out)
	}

	fn persist_meta<S: StateStore>(&mut self, store: &mut S, meta_loaded: MetaLoaded<G, C>) -> Result<()> {
		persist_batch_meta(store, &mut self.meta, meta_loaded)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::{BTreeMap, BTreeSet};

	use reifydb_codec::key::encoded::EncodedKey;

	use crate::{
		key::operator_state::GroupId,
		state::budget::OperatorStateBudgetHandle,
		window::engine::{
			AccumulatorEvent, EmitKind,
			config::WindowEngineConfig,
			rolling::{
				RollingBuckets, RollingBuffer, RollingEngine, RollingEviction, RollingExpiry,
				RollingResult,
			},
			test_support::{MockStore, StampedSum, SumAccumulator},
		},
	};

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder(OperatorStateBudgetHandle::default()).build()
	}

	fn row_key(group: &u32) -> (GroupId, EncodedKey) {
		(GroupId::NODE_SCOPE, node_row_key(group))
	}

	fn node_row_key(group: &u32) -> EncodedKey {
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
	fn meta_reclaimed_when_group_stale_past_threshold() {
		// Invariant: a group whose high water has fallen below the staleness threshold has gone
		// quiet; its per-group GroupMeta ('W') must be reclaimed. `persist_meta` never removes it,
		// so without the sweep a quiet group leaks one internal-state key forever.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Add(1)]);
		buckets.insert((1u32, 20u64), vec![AccumulatorEvent::Add(2)]);
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
		assert_eq!(store.meta_entry_count(), 1, "the group's meta is persisted on apply");

		let dropped = engine.expire_meta(&mut store, 100).unwrap();
		assert_eq!(dropped, 1, "the group's high water (20) is below the threshold (100)");
		assert_eq!(store.meta_entry_count(), 0, "a stale group must not leak its GroupMeta");
	}

	#[test]
	fn meta_survives_while_group_high_water_at_or_after_threshold() {
		// Safety boundary: a group whose high water is at or beyond the threshold is still live and
		// must keep its meta.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Add(1)]);
		buckets.insert((1u32, 20u64), vec![AccumulatorEvent::Add(2)]);
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

		let dropped = engine.expire_meta(&mut store, 5).unwrap();
		assert_eq!(dropped, 0, "high water (20) is not below the threshold (5)");
		assert_eq!(store.meta_entry_count(), 1, "a group within the staleness horizon keeps its meta");
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

		let capped = WindowEngineConfig::builder(OperatorStateBudgetHandle::default()).expire_batch(2).build();

		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new(capped.clone());
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
	fn runnable_engine_matches_recombine_across_seeded_churn() {
		// The runnable engine replaces the O(buffer) recombine with a running
		// accumulator maintained by merge/unmerge. Its observable behavior -
		// emitted kinds, values, expiry updates, terminal removes, and the
		// expiry-index bookkeeping - must be indistinguishable from the recombine
		// engine on an identical seeded add/remove/expire workload; integer sums
		// make the comparison exact. A divergence means the running maintenance
		// missed a mutation path.
		let mut recombine_store = MockStore::default();
		let mut runnable_store = MockStore::default();
		let mut recombine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
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
			let recombine_out = recombine
				.apply_evicting(
					&mut recombine_store,
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
				describe(&recombine_out),
				describe(&runnable_out),
				"apply diverged from the recombine at round {round}"
			);

			if round % 5 == 4 {
				cutoff = coord_base.saturating_sub(30);
				let recombine_exp =
					recombine.expire_before(&mut recombine_store, cutoff, sum_combine).unwrap();
				let runnable_exp = runnable.expire_before_running(&mut runnable_store, cutoff).unwrap();
				assert_eq!(
					describe_expiries(&recombine_exp),
					describe_expiries(&runnable_exp),
					"expiry diverged from the recombine at round {round}"
				);
				added.retain(|(_, coord, _)| *coord > cutoff);
			}
			coord_base += roll(20);
		}

		recombine.flush(&mut recombine_store).unwrap();
		runnable.flush(&mut runnable_store).unwrap();
		assert_eq!(
			recombine_store.index_entry_count(),
			runnable_store.index_entry_count(),
			"expiry-index bookkeeping diverged"
		);

		// Drain everything: terminal removes must match group-for-group.
		let recombine_final = recombine.expire_before(&mut recombine_store, u64::MAX - 1, sum_combine).unwrap();
		let runnable_final = runnable.expire_before_running(&mut runnable_store, u64::MAX - 1).unwrap();
		assert_eq!(
			describe_expiries(&recombine_final),
			describe_expiries(&runnable_final),
			"terminal drain diverged"
		);
		assert!(
			recombine_final.iter().all(|e| matches!(e, RollingExpiry::Remove { .. })),
			"draining past every coord must terminally remove all groups"
		);
	}

	#[test]
	fn runnable_engine_bootstraps_running_from_recombine_coords() {
		// The recombine and running paths share per-coord storage: coords
		// written by the recombine path (apply_evicting) must be folded into
		// the running accumulator the first time the runnable path touches the
		// group, both on the apply path and on the expiry path.
		let mut store = MockStore::default();
		let mut recombine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Add(5)]);
		buckets.insert((1u32, 20u64), vec![AccumulatorEvent::Add(7)]);
		recombine
			.apply_evicting(
				&mut store,
				buckets,
				RollingEviction::Before(0),
				row_key,
				SumAccumulator::default,
				sum_combine,
			)
			.unwrap();
		recombine.flush(&mut store).unwrap();

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
		// may remain. The recombine (apply_evicting) and running (apply_running)
		// paths share the same per-coord storage, so coords written by one are
		// picked up by the other. Leaked entries are exactly the kind of
		// unbounded state growth this engine exists to prevent.
		let mut store = MockStore::default();
		let mut recombine = RollingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Add(5)]);
		buckets.insert((1u32, 20u64), vec![AccumulatorEvent::Add(7)]);
		recombine
			.apply_evicting(
				&mut store,
				buckets,
				RollingEviction::Before(0),
				row_key,
				SumAccumulator::default,
				sum_combine,
			)
			.unwrap();
		recombine.flush(&mut store).unwrap();
		assert_eq!(
			store.buffer_coord_count::<SumAccumulator>(),
			2,
			"the recombine path persists both coords in the group's buffer"
		);

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
		assert_eq!(store.buffer_coord_count::<SumAccumulator>(), 4, "every live coord is persisted");
		assert_eq!(store.buffer_entry_count(), 2, "each live group persists one buffer entry");
		assert_eq!(store.running_entry_count(), 2, "each live group persists one running entry");

		let drained = runnable.expire_before_running(&mut store, u64::MAX - 1).unwrap();
		runnable.flush(&mut store).unwrap();
		assert_eq!(drained.len(), 2, "both groups drain");
		assert!(drained.iter().all(|e| matches!(e, RollingExpiry::Remove { .. })));
		assert_eq!(store.buffer_entry_count(), 0, "terminal removal must delete the group's buffer entry");
		assert_eq!(store.running_entry_count(), 0, "terminal removal must delete the running entry");
		assert_eq!(store.index_entry_count(), 0, "terminal removal must delete the expiry index entry");
	}

	#[test]
	fn lagged_runnable_engine_matches_a_semantic_oracle_across_seeded_churn() {
		// The lagged fast path maintains a running accumulator plus a merge
		// frontier at high_water - lag instead of recombining the buffer on
		// every touch. This drives a seeded add/retract/evict/expire workload
		// and checks emissions against an independently computed oracle: a
		// coord contributes exactly when it sits at or below the group's
		// monotone high water minus lag, coords survive until an eviction or
		// expiry cutoff passes them, and pending coords survive expiry even
		// while the group's visible row is withdrawn (the deliberate fix over
		// the blob recombine, which destroyed them). Emissions fold into a
		// visible-row map that must equal the oracle after every round, so an
		// early merge, a missed crossing, a double count, or a missed emission
		// surfaces as a state mismatch at the exact round.
		const LAG: u64 = 5;
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new_runnable(test_config()).with_lag(LAG);

		let mut state = 0xFEED_FACE_0123_4567u64;
		let mut roll = |bound: u64| {
			state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			(state >> 33) % bound
		};
		let mut coord_base = 100u64;
		let mut cutoff = 0u64;
		let mut added: Vec<(u32, u64, i64)> = Vec::new();
		let mut live: BTreeMap<(u32, u64), (i64, u64)> = BTreeMap::new();
		let mut group_hw: BTreeMap<u32, u64> = BTreeMap::new();
		let mut engine_visible: BTreeMap<u32, i64> = BTreeMap::new();

		fn oracle_visible(
			live: &BTreeMap<(u32, u64), (i64, u64)>,
			group_hw: &BTreeMap<u32, u64>,
			group: u32,
			lag: u64,
		) -> Option<i64> {
			let frontier = group_hw.get(&group)?.saturating_sub(lag);
			let mut sum = 0i64;
			let mut any = false;
			for (&(_, coord), &(coord_sum, _)) in live.range((group, 0)..=(group, u64::MAX)) {
				if coord <= frontier {
					sum += coord_sum;
					any = true;
				}
			}
			if any {
				Some(sum)
			} else {
				None
			}
		}

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

			let mut changed: BTreeSet<u32> = BTreeSet::new();
			for &(group, coord, value, is_add) in &plan {
				if is_add {
					let entry = live.entry((group, coord)).or_insert((0, 0));
					entry.0 += value;
					entry.1 += 1;
				} else if let Some(entry) = live.get_mut(&(group, coord)) {
					entry.0 -= value;
					entry.1 -= 1;
					if entry.1 == 0 {
						live.remove(&(group, coord));
					}
				} else {
					continue;
				}
				changed.insert(group);
				let hw = group_hw.entry(group).or_insert(0);
				*hw = (*hw).max(coord);
			}
			for &group in &changed {
				let dead: Vec<(u32, u64)> =
					live.range((group, 0)..=(group, cutoff)).map(|(&key, _)| key).collect();
				for key in dead {
					live.remove(&key);
				}
			}

			let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
			for &(group, coord, value, is_add) in &plan {
				let event = if is_add {
					AccumulatorEvent::Add(value)
				} else {
					AccumulatorEvent::Remove(value)
				};
				buckets.entry((group, coord)).or_default().push(event);
			}
			let out = engine
				.apply_running(
					&mut store,
					buckets,
					RollingEviction::Before(cutoff),
					row_key,
					SumAccumulator::default,
				)
				.unwrap();
			for r in &out {
				if matches!(r.kind, EmitKind::Remove) {
					let prior = engine_visible.remove(&r.group);
					assert_eq!(
						prior,
						Some(r.value),
						"withdrawn value must be the last published value (round {round})"
					);
				} else {
					engine_visible.insert(r.group, r.value);
				}
			}
			for group in 0u32..5 {
				assert_eq!(
					engine_visible.get(&group).copied(),
					oracle_visible(&live, &group_hw, group, LAG),
					"visible row diverged from the oracle for group {group} after apply round {round}"
				);
			}

			if round % 5 == 4 {
				cutoff = coord_base.saturating_sub(60);
				let expiries = engine.expire_before_running(&mut store, cutoff).unwrap();
				let dead: Vec<(u32, u64)> = live
					.iter()
					.filter(|&(&(_, coord), _)| coord <= cutoff)
					.map(|(&key, _)| key)
					.collect();
				for key in dead {
					live.remove(&key);
				}
				added.retain(|(_, coord, _)| *coord > cutoff);
				for e in &expiries {
					match e {
						RollingExpiry::Update {
							group,
							value,
							..
						} => {
							engine_visible.insert(*group, *value);
						}
						RollingExpiry::Remove {
							group,
							..
						} => {
							engine_visible.remove(group);
						}
					}
				}
				for group in 0u32..5 {
					assert_eq!(
						engine_visible.get(&group).copied(),
						oracle_visible(&live, &group_hw, group, LAG),
						"visible row diverged from the oracle for group {group} after expiry round {round}"
					);
				}
			}
			coord_base += roll(20);
		}

		let drained = engine.expire_before_running(&mut store, u64::MAX - 1).unwrap();
		for e in &drained {
			match e {
				RollingExpiry::Update {
					group,
					value,
					..
				} => {
					engine_visible.insert(*group, *value);
				}
				RollingExpiry::Remove {
					group,
					..
				} => {
					engine_visible.remove(group);
				}
			}
		}
		assert!(engine_visible.is_empty(), "the terminal drain must withdraw every visible row");
		engine.flush(&mut store).unwrap();
		assert_eq!(store.buffer_entry_count(), 0, "the terminal drain must delete every buffer entry");
		assert_eq!(store.running_entry_count(), 0, "the terminal drain must delete every running entry");
		assert_eq!(store.index_entry_count(), 0, "the terminal drain must delete every index entry");
	}

	#[test]
	fn lagged_running_holds_back_coords_within_the_lag_horizon() {
		// With lag 10, a coord contributes only once the group's high water
		// has moved at least lag past it. This pins the full pending
		// lifecycle: a first event emits nothing (the lagged window is still
		// empty), later events pull older coords across the frontier one
		// batch at a time, a retraction of a still-pending coord never
		// touches the published aggregate, and evicting every merged coord
		// while only pending ones remain withdraws the row.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new_runnable(test_config()).with_lag(10);

		let apply = |engine: &mut RollingEngine<u32, u64, SumAccumulator>,
		             store: &mut MockStore,
		             coord: u64,
		             value: i64,
		             is_add: bool,
		             cutoff: u64| {
			let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
			let event = if is_add {
				AccumulatorEvent::Add(value)
			} else {
				AccumulatorEvent::Remove(value)
			};
			buckets.insert((1u32, coord), vec![event]);
			engine.apply_running(
				store,
				buckets,
				RollingEviction::Before(cutoff),
				row_key,
				SumAccumulator::default,
			)
			.unwrap()
		};

		let out = apply(&mut engine, &mut store, 100, 5, true, 0);
		assert!(out.is_empty(), "a lone coord inside the lag horizon must publish nothing");

		let out = apply(&mut engine, &mut store, 115, 7, true, 0);
		assert_eq!(
			describe(&out),
			vec![(1u32, EmitKind::Insert, 5i64)],
			"advancing high water to 115 merges only coord 100; coord 115 itself stays pending"
		);

		let out = apply(&mut engine, &mut store, 130, 9, true, 0);
		assert_eq!(
			describe(&out),
			vec![(1u32, EmitKind::Update, 12i64)],
			"coord 115 crosses the frontier at high water 130; coord 130 stays pending"
		);

		let out = apply(&mut engine, &mut store, 130, 9, false, 0);
		assert_eq!(
			describe(&out),
			vec![(1u32, EmitKind::Update, 12i64)],
			"retracting the still-pending coord 130 must not change the published aggregate"
		);

		let out = apply(&mut engine, &mut store, 200, 1, true, 150);
		assert_eq!(
			describe(&out),
			vec![(1u32, EmitKind::Remove, 12i64)],
			"evicting every merged coord while coord 200 is still pending withdraws the row"
		);
		engine.flush(&mut store).unwrap();
		assert_eq!(
			store.buffer_coord_count::<SumAccumulator>(),
			1,
			"the pending coord survives the withdrawal"
		);
		assert_eq!(store.running_entry_count(), 0, "a group with no merged coord persists no running entry");
	}

	#[test]
	fn lagged_expiry_retains_pending_coords() {
		// Deliberate divergence from the blob recombine, which destroys the
		// whole buffer when a due group has no coord older than newest - lag,
		// silently losing pending coords that would have slid into the lagged
		// window later. The fast path must withdraw the visible row but keep
		// the pending coords, and a later event that advances the frontier
		// must surface their contribution.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, u64, SumAccumulator>::new_runnable(test_config()).with_lag(10);

		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 100u64), vec![AccumulatorEvent::Add(5)]);
		buckets.insert((1u32, 115u64), vec![AccumulatorEvent::Add(7)]);
		let out = engine
			.apply_running(
				&mut store,
				buckets,
				RollingEviction::Before(0),
				row_key,
				SumAccumulator::default,
			)
			.unwrap();
		assert_eq!(describe(&out), vec![(1u32, EmitKind::Insert, 5i64)]);

		let expired = engine.expire_before_running(&mut store, 105).unwrap();
		assert_eq!(
			describe_expiries(&expired),
			vec![(1u32, None)],
			"expiring the only merged coord withdraws the row"
		);
		engine.flush(&mut store).unwrap();
		assert_eq!(
			store.buffer_coord_count::<SumAccumulator>(),
			1,
			"the pending coord 115 must survive the expiry"
		);
		assert_eq!(store.index_entry_count(), 1, "the group stays indexed at its pending coord");

		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 130u64), vec![AccumulatorEvent::Add(9)]);
		let out = engine
			.apply_running(
				&mut store,
				buckets,
				RollingEviction::Before(105),
				row_key,
				SumAccumulator::default,
			)
			.unwrap();
		assert_eq!(
			describe(&out),
			vec![(1u32, EmitKind::Insert, 7i64)],
			"the retained coord 115 crosses the frontier at high water 130 and surfaces"
		);
	}
}
