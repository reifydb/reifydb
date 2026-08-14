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
	row::operator::OperatorState,
};
use reifydb_core::{
	key::operator_state::{GroupId, GroupStateKey},
	metrics::heap::HeapSize,
	state::{cache::StateCache, store::StateStore},
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};

use crate::{
	operator::state::{
		expiry::{expiry_drop, expiry_due, expiry_earliest, expiry_key, expiry_set},
		seal::coord::{Coord, IsZero},
	},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, BatchMeta, BufferKey, EmitKind, GroupMeta, MetaKey, RunningKey,
			config::WindowEngineConfig, load_batch_meta, meta_key_for, note_when_expiry_capped,
			persist_batch_meta, sweep_stale_meta,
		},
		span::Slot,
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
	Nothing,
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

#[operator_state]
#[derive(Clone)]
pub struct RollingIndexEntry<G> {
	group: G,
	slot_key: Vec<u8>,
	group_id: u64,
}

fn coord_min_key<C: Slot, A>(buffer: &RollingBuffer<C, A>) -> Option<u64> {
	buffer.keys().next().map(|c| c.order_key().to_order())
}

type MetaLoaded<G, C> = HashMap<G, BatchMeta<C>>;
type BufferRows<G> = HashMap<G, (GroupId, EncodedKey)>;

struct GroupSlot<C, Accumulator, Output> {
	group_id: GroupId,
	key: EncodedKey,
	buffer: RollingBuffer<C, Accumulator>,
	buffer_changed: bool,
	prior_index_key: Option<u64>,
	prior_output: Option<Output>,
}

pub struct RollingEngine<G, C: Slot, Accumulator> {
	buffers: StateCache<BufferKey, RollingBuffer<C, Accumulator>>,
	running: Option<StateCache<RunningKey, Accumulator>>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	meta_low_water: Option<u64>,
	expire_batch: usize,
	lag: <C::Coord as Coord>::Span,
	_pd: PhantomData<G>,
}

struct RunnableGroupSlot<C: Slot, Accumulator>
where
	Accumulator: WindowAccumulator,
{
	group_id: GroupId,
	key: EncodedKey,
	buffer: RollingBuffer<C, Accumulator>,
	running: Accumulator,
	buffer_changed: bool,
	prior_min: Option<u64>,
	old_frontier: Option<C::Coord>,
	prior_output: Option<Accumulator::Output>,
}

fn merge_into<A: WindowAccumulator>(running: &mut A, other: &A) {
	if running.is_empty() {
		*running = other.clone();
	} else {
		running.merge(other);
	}
}

fn frontier_for<C: Slot>(lag: <C::Coord as Coord>::Span, high_water: &Option<C>) -> Option<C::Coord> {
	if lag.is_zero() {
		Some(<C::Coord as Coord>::MAX)
	} else {
		high_water.as_ref().map(|hw| hw.order_key().saturating_sub_span(lag))
	}
}

fn is_merged_coord<C: Coord>(coord: C, frontier: Option<C>) -> bool {
	frontier.is_some_and(|f| coord <= f)
}

fn running_below<C: Slot, A: WindowAccumulator>(buffer: &RollingBuffer<C, A>, frontier: Option<C::Coord>) -> A {
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
	pub fn new(config: WindowEngineConfig) -> Self {
		Self {
			buffers: StateCache::<BufferKey, RollingBuffer<C, Accumulator>>::new(),
			running: None,
			meta: StateCache::<MetaKey, GroupMeta<C>>::new(),
			meta_low_water: None,
			expire_batch: config.expire_batch(),
			lag: Default::default(),
			_pd: PhantomData,
		}
	}

	pub fn new_runnable(config: WindowEngineConfig) -> Self {
		let mut engine = Self::new(config);
		engine.running = Some(StateCache::<RunningKey, Accumulator>::new());
		engine
	}

	pub fn with_lag(mut self, lag: <C::Coord as Coord>::Span) -> Self {
		self.lag = lag;
		self
	}

	pub fn apply<K, CB, Output>(
		&mut self,
		store: &mut dyn StateStore,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		capacity: usize,
		row_key: K,
		combine: CB,
	) -> Result<Vec<RollingResult<G, Output>>>
	where
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

	pub fn apply_evicting<K, NA, CB, Output>(
		&mut self,
		store: &mut dyn StateStore,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		eviction: RollingEviction<C>,
		row_key: K,
		new_accumulator: NA,
		combine: CB,
	) -> Result<Vec<RollingResult<G, Output>>>
	where
		K: Fn(&G) -> (GroupId, EncodedKey),
		NA: Fn() -> Accumulator,
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let indexed = matches!(eviction, RollingEviction::Before(_) | RollingEviction::Nothing);
		let mut meta_loaded = self.load_meta(store, &buckets)?;
		let buffer_rows = self.resolve_buffer_rows(&buckets, &meta_loaded, &row_key)?;
		let group_slots = self.apply_events_into_buffers(
			store,
			buckets,
			&mut meta_loaded,
			&buffer_rows,
			&row_key,
			&eviction,
			&new_accumulator,
			&combine,
			indexed,
		)?;
		let results = self.combine_and_collect(store, group_slots, &combine, indexed)?;
		self.persist_meta(store, meta_loaded)?;
		Ok(results)
	}

	fn load_meta(
		&mut self,
		store: &mut dyn StateStore,
		buckets: &RollingBuckets<G, C, Accumulator::Contribution>,
	) -> Result<MetaLoaded<G, C>> {
		let mut meta_loaded: MetaLoaded<G, C> = HashMap::new();
		for (group, _) in buckets.keys() {
			if !meta_loaded.contains_key(group) {
				let batch = load_batch_meta(store, &mut self.meta, &meta_key_for(group))?;
				meta_loaded.insert(group.clone(), batch);
			}
		}
		Ok(meta_loaded)
	}

	fn resolve_buffer_rows<K>(
		&mut self,
		buckets: &RollingBuckets<G, C, Accumulator::Contribution>,
		meta_loaded: &MetaLoaded<G, C>,
		row_key: &K,
	) -> Result<BufferRows<G>>
	where
		K: Fn(&G) -> (GroupId, EncodedKey),
	{
		let mut buffer_rows: BufferRows<G> = HashMap::new();
		let mut seen: BTreeSet<G> = BTreeSet::new();
		for (group, coord) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.initial);
			if initial_high_water.is_none_or(|hw| *coord >= hw) && seen.insert(group.clone()) {
				let (id, key) = row_key(group);
				buffer_rows.insert(group.clone(), (id, key));
			}
		}
		Ok(buffer_rows)
	}

	#[allow(clippy::too_many_arguments)]
	fn apply_events_into_buffers<K, NA, CB, Output>(
		&mut self,
		store: &mut dyn StateStore,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		meta_loaded: &mut MetaLoaded<G, C>,
		buffer_rows: &BufferRows<G>,
		row_key: &K,
		eviction: &RollingEviction<C>,
		new_accumulator: &NA,
		combine: &CB,
		indexed: bool,
	) -> Result<BTreeMap<G, GroupSlot<C, Accumulator, Output>>>
	where
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
					let (group_id, key) = match buffer_rows.get(&group) {
						Some(resolved) => resolved.clone(),
						None => row_key(&group),
					};
					let buffer: RollingBuffer<C, Accumulator> = self
						.buffers
						.get(store, &BufferKey::new(group_id, key.clone()))?
						.unwrap_or_default();
					let was_empty_before = buffer.is_empty();
					let prior_output = if was_empty_before {
						None
					} else {
						combine(&group, &buffer)
					};
					let prior_index_key = if indexed {
						coord_min_key(&buffer)
					} else {
						None
					};
					group_slots.insert(
						group.clone(),
						GroupSlot {
							group_id,
							key,
							buffer,
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
				RollingEviction::Nothing => {}
			}
			slot.buffer_changed = true;

			meta.observe(coord);
		}
		Ok(group_slots)
	}

	fn combine_and_collect<CB, Output>(
		&mut self,
		store: &mut dyn StateStore,
		group_slots: BTreeMap<G, GroupSlot<C, Accumulator, Output>>,
		combine: &CB,
		indexed: bool,
	) -> Result<Vec<RollingResult<G, Output>>>
	where
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		let mut results: Vec<RollingResult<G, Output>> = Vec::new();
		for (group, slot) in group_slots {
			if !slot.buffer_changed {
				continue;
			}
			if indexed {
				let new_index_key = coord_min_key(&slot.buffer);
				if new_index_key != slot.prior_index_key {
					if let Some(old) = slot.prior_index_key {
						expiry_drop(store, &expiry_key(old, &group, &[]))?;
					}
					if let Some(new) = new_index_key {
						expiry_set(
							store,
							expiry_key(new, &group, &[]),
							RollingIndexEntry {
								group: group.clone(),
								slot_key: slot.key.as_bytes().to_vec(),
								group_id: slot.group_id.0,
							},
						)?;
					}
				}
			}
			let output = combine(&group, &slot.buffer);
			if slot.buffer.is_empty() {
				self.buffers.remove(store, &BufferKey::new(slot.group_id, slot.key.clone()))?;
			} else {
				self.buffers.put(
					store,
					&BufferKey::new(slot.group_id, slot.key.clone()),
					slot.buffer,
				)?;
			}

			if let Some(out) = output {
				let (row_number, is_new) = store.get_or_create_row_number(slot.group_id, &slot.key)?;
				let kind = if is_new {
					EmitKind::Insert
				} else {
					EmitKind::Update
				};
				results.push(RollingResult {
					row_number,
					group,
					value: out,
					prior: None,
					kind,
				});
			} else if let Some(prior) = slot.prior_output {
				let (row_number, _is_new) = store.get_or_create_row_number(slot.group_id, &slot.key)?;
				store.remove_row_number(slot.group_id, &slot.key)?;
				results.push(RollingResult {
					row_number,
					group,
					value: prior,
					prior: None,
					kind: EmitKind::Remove,
				});
			}
		}
		Ok(results)
	}

	fn load_running(
		&mut self,
		store: &mut dyn StateStore,
		buffer: &RollingBuffer<C, Accumulator>,
		group_id: GroupId,
		slot: &EncodedKey,
		frontier: Option<C::Coord>,
	) -> Result<Accumulator> {
		let running_cache = self.running.as_mut().expect("runnable engine has a running cache");
		if let Some(running) = running_cache.get(store, &RunningKey::new(group_id, slot.clone()))? {
			return Ok(running);
		}
		Ok(running_below(buffer, frontier))
	}

	pub fn apply_running<K, NA>(
		&mut self,
		store: &mut dyn StateStore,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		eviction: RollingEviction<C>,
		row_key: K,
		new_accumulator: NA,
	) -> Result<Vec<RollingResult<G, Accumulator::Output>>>
	where
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
		let evict_cutoff = match eviction {
			RollingEviction::Before(cutoff) => Some(cutoff),
			RollingEviction::Nothing => None,
			RollingEviction::Capacity(_) => {
				unimplemented!("apply_running supports only Before eviction")
			}
		};
		let mut meta_loaded = self.load_meta(store, &buckets)?;
		let buffer_rows = self.resolve_buffer_rows(&buckets, &meta_loaded, &row_key)?;

		let mut group_slots: BTreeMap<G, RunnableGroupSlot<C, Accumulator>> = BTreeMap::new();
		for ((group, coord), events) in buckets {
			let meta = meta_loaded.entry(group.clone()).or_default();

			let slot = match group_slots.get_mut(&group) {
				Some(s) => s,
				None => {
					let (group_id, key) = match buffer_rows.get(&group) {
						Some(resolved) => resolved.clone(),
						None => row_key(&group),
					};
					let buffer: RollingBuffer<C, Accumulator> = self
						.buffers
						.get(store, &BufferKey::new(group_id, key.clone()))?
						.unwrap_or_default();
					let old_frontier = frontier_for(self.lag, &meta.high_water());
					let prior_min = coord_min_key(&buffer);
					let merged_before = prior_min.is_some_and(|m| {
						is_merged_coord(<C::Coord as Coord>::from_order(m), old_frontier)
					});
					let running = if merged_before {
						self.load_running(store, &buffer, group_id, &key, old_frontier)?
					} else {
						Accumulator::default()
					};
					let prior_output = if merged_before {
						running.finalize()
					} else {
						None
					};
					group_slots.insert(
						group.clone(),
						RunnableGroupSlot {
							group_id,
							key,
							buffer,
							running,
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
			if let Some(evict_cutoff) = evict_cutoff {
				let due: Vec<C> = slot.buffer.range(..=evict_cutoff).map(|(coord, _)| *coord).collect();
				for coord in due {
					let Some(evicted) = slot.buffer.remove(&coord) else {
						continue;
					};
					if is_merged_coord(coord.order_key(), new_frontier) {
						slot.running.unmerge(&evicted);
					}
				}
			}
			let new_min = coord_min_key(&slot.buffer);
			if new_min != slot.prior_min {
				if let Some(old) = slot.prior_min {
					expiry_drop(store, &expiry_key(old, &group, &[]))?;
				}
				if let Some(new) = new_min {
					expiry_set(
						store,
						expiry_key(new, &group, &[]),
						RollingIndexEntry {
							group: group.clone(),
							slot_key: slot.key.as_bytes().to_vec(),
							group_id: slot.group_id.0,
						},
					)?;
				}
			}
			let merged_any = new_min
				.is_some_and(|m| is_merged_coord(<C::Coord as Coord>::from_order(m), new_frontier));
			let output = if merged_any {
				slot.running.finalize()
			} else {
				None
			};
			if slot.buffer.is_empty() {
				self.buffers.remove(store, &BufferKey::new(slot.group_id, slot.key.clone()))?;
			} else {
				self.buffers.put(
					store,
					&BufferKey::new(slot.group_id, slot.key.clone()),
					slot.buffer,
				)?;
			}
			let running_cache = self.running.as_mut().expect("runnable engine has a running cache");
			if merged_any {
				running_cache.put(
					store,
					&RunningKey::new(slot.group_id, slot.key.clone()),
					slot.running,
				)?;
			} else {
				running_cache.remove(store, &RunningKey::new(slot.group_id, slot.key.clone()))?;
			}

			if let Some(out) = output {
				let (row_number, is_new) = store.get_or_create_row_number(slot.group_id, &slot.key)?;
				let kind = if is_new {
					EmitKind::Insert
				} else {
					EmitKind::Update
				};
				results.push(RollingResult {
					row_number,
					group,
					value: out,
					prior: None,
					kind,
				});
			} else if let Some(prior) = slot.prior_output {
				let (row_number, _is_new) = store.get_or_create_row_number(slot.group_id, &slot.key)?;
				store.remove_row_number(slot.group_id, &slot.key)?;
				results.push(RollingResult {
					row_number,
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

	pub fn expire_before_running(
		&mut self,
		store: &mut dyn StateStore,
		cutoff: C,
	) -> Result<Vec<RollingExpiry<G, Accumulator::Output>>> {
		reifydb_assertions! {
			assert!(
				self.running.is_some(),
				"expire_before_running requires an engine constructed with new_runnable"
			);
		}
		let due: Vec<(GroupStateKey, RollingIndexEntry<G>)> =
			expiry_due(store, cutoff.order_key().to_order(), self.expire_batch)?;

		let mut out: Vec<RollingExpiry<G, Accumulator::Output>> = Vec::new();
		for (index_key, entry) in due {
			let slot = EncodedKey::new(&entry.slot_key);
			let group_id = GroupId(entry.group_id);
			expiry_drop(store, &index_key)?;
			let frontier = if self.lag.is_zero() {
				Some(<C::Coord as Coord>::MAX)
			} else {
				let lag = self.lag;
				self.meta
					.get(store, &meta_key_for(&entry.group))?
					.and_then(|meta| frontier_for::<C>(lag, &meta.high_water))
			};
			let mut buffer: RollingBuffer<C, Accumulator> =
				self.buffers.get(store, &BufferKey::new(group_id, slot.clone()))?.unwrap_or_default();
			let expired: Vec<C> = buffer.range(..=cutoff).map(|(coord, _)| *coord).collect();
			if expired.is_empty() {
				if let Some(new) = coord_min_key(&buffer) {
					expiry_set(
						store,
						expiry_key(new, &entry.group, &[]),
						RollingIndexEntry {
							group: entry.group.clone(),
							slot_key: entry.slot_key.clone(),
							group_id: entry.group_id,
						},
					)?;
				}
				continue;
			}
			let mut running = self.load_running(store, &buffer, group_id, &slot, frontier)?;
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
			let merged_any =
				new_min.is_some_and(|m| is_merged_coord(<C::Coord as Coord>::from_order(m), frontier));
			let finalized = if merged_any {
				running.finalize()
			} else {
				None
			};
			match (new_min, merged_any, finalized) {
				(Some(new), true, Some(value)) => {
					expiry_set(
						store,
						expiry_key(new, &entry.group, &[]),
						RollingIndexEntry {
							group: entry.group.clone(),
							slot_key: entry.slot_key.clone(),
							group_id: entry.group_id,
						},
					)?;
					self.buffers.put(store, &BufferKey::new(group_id, slot.clone()), buffer)?;
					let running_cache =
						self.running.as_mut().expect("runnable engine has a running cache");
					running_cache.put(store, &RunningKey::new(group_id, slot.clone()), running)?;
					let (row_number, _) = store.get_or_create_row_number(group_id, &slot)?;
					out.push(RollingExpiry::Update {
						row_number,
						group: entry.group,
						group_id,
						value,
					});
				}
				(Some(new), false, _) => {
					expiry_set(
						store,
						expiry_key(new, &entry.group, &[]),
						RollingIndexEntry {
							group: entry.group.clone(),
							slot_key: entry.slot_key.clone(),
							group_id: entry.group_id,
						},
					)?;
					self.buffers.put(store, &BufferKey::new(group_id, slot.clone()), buffer)?;
					let running_cache =
						self.running.as_mut().expect("runnable engine has a running cache");
					running_cache.remove(store, &RunningKey::new(group_id, slot.clone()))?;
					if unmerged_any {
						let (row_number, _) =
							store.get_or_create_row_number(group_id, &slot)?;
						store.remove_row_number(group_id, &slot)?;
						out.push(RollingExpiry::Remove {
							row_number,
							group: entry.group,
							group_id,
						});
					}
				}
				_ => {
					self.buffers.remove(store, &BufferKey::new(group_id, slot.clone()))?;
					let running_cache =
						self.running.as_mut().expect("runnable engine has a running cache");
					running_cache.remove(store, &RunningKey::new(group_id, slot.clone()))?;
					let (row_number, _) = store.get_or_create_row_number(group_id, &slot)?;
					store.remove_row_number(group_id, &slot)?;
					out.push(RollingExpiry::Remove {
						row_number,
						group: entry.group,
						group_id,
					});
				}
			}
		}
		note_when_expiry_capped(out.len(), self.expire_batch);
		Ok(out)
	}

	pub fn expire_meta(&mut self, store: &mut dyn StateStore, threshold: u64) -> Result<usize> {
		sweep_stale_meta(store, &mut self.meta, threshold, &mut self.meta_low_water)
	}

	pub fn earliest_expiry(&mut self, store: &mut dyn StateStore) -> Result<Option<u64>> {
		expiry_earliest(store)
	}

	pub fn expire_before<CB, Output>(
		&mut self,
		store: &mut dyn StateStore,
		cutoff: C,
		combine: CB,
	) -> Result<Vec<RollingExpiry<G, Output>>>
	where
		CB: Fn(&G, &RollingBuffer<C, Accumulator>) -> Option<Output>,
	{
		let due: Vec<(GroupStateKey, RollingIndexEntry<G>)> =
			expiry_due(store, cutoff.order_key().to_order(), self.expire_batch)?;

		let mut out: Vec<RollingExpiry<G, Output>> = Vec::new();
		for (index_key, entry) in due {
			let slot = EncodedKey::new(&entry.slot_key);
			let group_id = GroupId(entry.group_id);
			expiry_drop(store, &index_key)?;
			let mut buffer: RollingBuffer<C, Accumulator> =
				self.buffers.get(store, &BufferKey::new(group_id, slot.clone()))?.unwrap_or_default();
			if buffer.is_empty() {
				continue;
			}
			let before = buffer.len();
			buffer.retain(|&coord, _| coord > cutoff);
			if buffer.len() == before {
				if let Some(new) = coord_min_key(&buffer) {
					expiry_set(
						store,
						expiry_key(new, &entry.group, &[]),
						RollingIndexEntry {
							group: entry.group.clone(),
							slot_key: entry.slot_key.clone(),
							group_id: entry.group_id,
						},
					)?;
				}
				continue;
			}
			match combine(&entry.group, &buffer) {
				Some(value) if !buffer.is_empty() => {
					if let Some(new) = coord_min_key(&buffer) {
						expiry_set(
							store,
							expiry_key(new, &entry.group, &[]),
							RollingIndexEntry {
								group: entry.group.clone(),
								slot_key: entry.slot_key.clone(),
								group_id: entry.group_id,
							},
						)?;
					}
					self.buffers.put(store, &BufferKey::new(group_id, slot.clone()), buffer)?;
					let (row_number, _) = store.get_or_create_row_number(group_id, &slot)?;
					out.push(RollingExpiry::Update {
						row_number,
						group: entry.group,
						group_id,
						value,
					});
				}
				_ => {
					self.buffers.remove(store, &BufferKey::new(group_id, slot.clone()))?;
					let (row_number, _) = store.get_or_create_row_number(group_id, &slot)?;
					store.remove_row_number(group_id, &slot)?;
					out.push(RollingExpiry::Remove {
						row_number,
						group: entry.group,
						group_id,
					});
				}
			}
		}
		note_when_expiry_capped(out.len(), self.expire_batch);
		Ok(out)
	}

	fn persist_meta(&mut self, store: &mut dyn StateStore, meta_loaded: MetaLoaded<G, C>) -> Result<()> {
		persist_batch_meta(store, &mut self.meta, meta_loaded)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::{BTreeMap, BTreeSet};

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::key::operator_state::GroupId;
	use reifydb_value::{
		factory::time::{at_millis, millis},
		value::datetime::DateTime,
	};

	use crate::{
		operator::state::{mock::MockStore, seal::coord::Coord},
		window::{
			accumulator::mock::SumAccumulator,
			engine::{
				AccumulatorEvent, EmitKind,
				config::WindowEngineConfig,
				rolling::{
					RollingBuckets, RollingBuffer, RollingEngine, RollingEviction, RollingExpiry,
					RollingResult,
				},
			},
		},
	};

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder().build()
	}

	fn order(millis: u64) -> u64 {
		<DateTime as Coord>::to_order(at_millis(millis))
	}

	fn row_key(group: &u32) -> (GroupId, EncodedKey) {
		(GroupId::ROOT, node_row_key(group))
	}

	fn node_row_key(group: &u32) -> EncodedKey {
		EncodedKey::builder().u32(*group).build()
	}

	fn past_every_coord() -> DateTime {
		// The drain-everything cutoff. It stays one millisecond below the coordinate maximum because
		// the running frontier uses the maximum itself as its "no high water yet" sentinel, and a
		// cutoff sitting exactly on that sentinel would compare equal to it rather than after it.
		DateTime::MAX.saturating_sub(millis(1))
	}

	fn sum_combine(_group: &u32, buffer: &RollingBuffer<DateTime, SumAccumulator>) -> Option<i64> {
		if buffer.is_empty() {
			None
		} else {
			Some(buffer.values().map(|a| a.sum).sum())
		}
	}

	#[test]
	fn meta_reclaimed_when_group_stale_past_threshold() {
		// A group whose high water falls below the staleness threshold has gone quiet and its
		// GroupMeta must be reclaimed; `persist_meta` never removes it, so without the sweep a
		// quiet group leaks one internal-state key forever.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(1)]);
		buckets.insert((1u32, at_millis(20)), vec![AccumulatorEvent::Add(2)]);
		engine.apply_evicting(
			&mut store,
			buckets,
			RollingEviction::Before(at_millis(0)),
			row_key,
			SumAccumulator::default,
			sum_combine,
		)
		.unwrap();
		assert_eq!(store.meta_entry_count(), 1, "the group's meta is persisted on apply");

		let dropped = engine.expire_meta(&mut store, order(100)).unwrap();
		assert_eq!(dropped, 1, "the group's high water (20) is below the threshold (100)");
		assert_eq!(store.meta_entry_count(), 0, "a stale group must not leak its GroupMeta");
	}

	#[test]
	fn meta_survives_while_group_high_water_at_or_after_threshold() {
		// A group whose high water is at or beyond the threshold is still live and keeps its meta.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(1)]);
		buckets.insert((1u32, at_millis(20)), vec![AccumulatorEvent::Add(2)]);
		engine.apply_evicting(
			&mut store,
			buckets,
			RollingEviction::Before(at_millis(0)),
			row_key,
			SumAccumulator::default,
			sum_combine,
		)
		.unwrap();

		let dropped = engine.expire_meta(&mut store, 5).unwrap();
		assert_eq!(dropped, 0, "high water (20) is not below the threshold (5)");
		assert_eq!(store.meta_entry_count(), 1, "a group within the staleness horizon keeps its meta");
	}

	#[test]
	fn nothing_to_evict_retains_the_coordinate_at_zero_and_still_indexes_the_group() {
		// Eviction is inclusive, so clamping a not-yet-elapsed span to Before(0) would make an epoch
		// coordinate unretainable. The group must still be indexed, or the tick that first has
		// something to evict cannot see it.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(0)), vec![AccumulatorEvent::Add(7)]);

		let results = engine
			.apply_evicting(
				&mut store,
				buckets,
				RollingEviction::Nothing,
				row_key,
				SumAccumulator::default,
				sum_combine,
			)
			.unwrap();

		assert_eq!(results.len(), 1, "the group must publish rather than come back empty");
		assert_eq!(results[0].value, 7, "the contribution at the epoch must survive the tick");
		assert_eq!(store.index_entry_count(), 1, "Nothing must index the group exactly as Before does");
	}

	#[test]
	fn evicting_before_zero_still_drops_the_coordinate_at_zero() {
		// The counterpart: a real Before(0) means the span has elapsed and zero is outside the
		// window, so the coordinate at zero must go. Only the absence of a cutoff retains it.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(0)), vec![AccumulatorEvent::Add(7)]);

		let results = engine
			.apply_evicting(
				&mut store,
				buckets,
				RollingEviction::Before(at_millis(0)),
				row_key,
				SumAccumulator::default,
				sum_combine,
			)
			.unwrap();

		assert!(
			results.iter().all(|r| r.value == 0),
			"a coordinate at or below the cutoff must not contribute, got {:?}",
			results.iter().map(|r| r.value).collect::<Vec<_>>()
		);
	}

	#[test]
	fn expire_before_evicts_a_quiet_group_then_rekeys_then_removes() {
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(1)]);
		buckets.insert((1u32, at_millis(20)), vec![AccumulatorEvent::Add(2)]);
		buckets.insert((1u32, at_millis(30)), vec![AccumulatorEvent::Add(3)]);
		// Before(0) evicts nothing at apply (all coords > 0), so the buffer keeps 10,20,30.
		engine.apply_evicting(
			&mut store,
			buckets,
			RollingEviction::Before(at_millis(0)),
			row_key,
			SumAccumulator::default,
			sum_combine,
		)
		.unwrap();
		assert_eq!(store.index_entry_count(), 1, "the group is indexed by its oldest coord");

		// A tick with no new events for this group evicts coords <= 20; coord 30 survives.
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let out = engine.expire_before(&mut store, at_millis(20), sum_combine).unwrap();
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
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let out = engine.expire_before(&mut store, at_millis(30), sum_combine).unwrap();
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
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		assert!(engine.expire_before(&mut store, at_millis(1000), sum_combine).unwrap().is_empty());
	}

	#[test]
	fn expire_before_leaves_groups_whose_oldest_coord_is_not_due() {
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(100)), vec![AccumulatorEvent::Add(1)]);
		buckets.insert((2u32, at_millis(5)), vec![AccumulatorEvent::Add(9)]);
		engine.apply_evicting(
			&mut store,
			buckets,
			RollingEviction::Before(at_millis(0)),
			row_key,
			SumAccumulator::default,
			sum_combine,
		)
		.unwrap();
		assert_eq!(store.index_entry_count(), 2);

		// Cutoff 5 is due only for group 2 (oldest coord 5); group 1 (oldest 100) is untouched.
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let out = engine.expire_before(&mut store, at_millis(5), sum_combine).unwrap();
		assert_eq!(out.len(), 1, "only the group with a due coord is processed");
		assert!(matches!(&out[0], RollingExpiry::Remove { group, .. } if *group == 2));
		assert_eq!(store.index_entry_count(), 1, "group 1 keeps its index entry");
	}

	#[test]
	fn expire_before_processes_at_most_expire_batch_then_resumes_next_tick() {
		// Node ticks run serialized, so draining a due-group burst in one tick lets one bloated
		// operator stall every other flow. Capped groups stay in the due index, which sorts by
		// inverted coord so the oldest backlog defers.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(1)]);
		buckets.insert((2u32, at_millis(20)), vec![AccumulatorEvent::Add(2)]);
		buckets.insert((3u32, at_millis(30)), vec![AccumulatorEvent::Add(3)]);
		engine.apply_evicting(
			&mut store,
			buckets,
			RollingEviction::Before(at_millis(0)),
			row_key,
			SumAccumulator::default,
			sum_combine,
		)
		.unwrap();
		assert_eq!(store.index_entry_count(), 3);

		let capped = WindowEngineConfig::builder().expire_batch(2).build();

		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(capped.clone());
		let first = engine.expire_before(&mut store, at_millis(1000), sum_combine).unwrap();
		assert_eq!(first.len(), 2, "one tick drains at most expire_batch groups");
		assert!(matches!(&first[0], RollingExpiry::Remove { group, .. } if *group == 3));
		assert!(matches!(&first[1], RollingExpiry::Remove { group, .. } if *group == 2));
		assert_eq!(store.index_entry_count(), 1, "the deferred group keeps its index entry");

		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(capped);
		let second = engine.expire_before(&mut store, at_millis(1000), sum_combine).unwrap();
		assert_eq!(second.len(), 1, "the next tick picks up the deferred group");
		assert!(matches!(&second[0], RollingExpiry::Remove { group, .. } if *group == 1));
		assert_eq!(store.index_entry_count(), 0);
	}

	#[test]
	fn withdrawn_value_is_reconstructed_after_restart() {
		// `prior_output` is never persisted, so the terminal Remove's value is recomputed as
		// `combine(buffer)` from the persisted buffer. That reconstruction is exact only because
		// `combine` is a pure function of the buffer; a combine reading non-persisted state breaks it.
		let mut store = MockStore::default();

		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(5)]);
		let published: Vec<RollingResult<u32, i64>> =
			engine.apply(&mut store, buckets, 4, row_key, sum_combine).unwrap();
		assert_eq!(published.len(), 1);
		assert!(matches!(published[0].kind, EmitKind::Insert));
		assert_eq!(published[0].value, 5);

		// A brand new engine with no in-memory GroupSlot or prior_output, reading only the
		// persisted buffer left behind by the first engine.
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Remove(5)]);
		let withdrawn: Vec<RollingResult<u32, i64>> =
			engine.apply(&mut store, buckets, 4, row_key, sum_combine).unwrap();

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
		// The other way a read reaches the store is LRU eviction, with no restart: the cache holds
		// 8 groups, so tracking more evicts the oldest and the next access re-reads it. Same
		// persist/reload path as the restart test, inside one long-lived engine.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());

		let mut published_group_1: Vec<RollingResult<u32, i64>> = Vec::new();
		for group in 1u32..=11u32 {
			let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
			buckets.insert((group, at_millis(10)), vec![AccumulatorEvent::Add(i64::from(group))]);
			let out: Vec<RollingResult<u32, i64>> =
				engine.apply(&mut store, buckets, 4, row_key, sum_combine).unwrap();
			if group == 1 {
				published_group_1 = out;
			}
		}
		assert_eq!(published_group_1.len(), 1);
		assert!(matches!(published_group_1[0].kind, EmitKind::Insert));
		assert_eq!(published_group_1[0].value, 1);

		// Group 1 was pushed out of the 8-slot cache by the later groups, so the same engine must
		// re-read its buffer from the store to apply this retraction.
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Remove(1)]);
		let withdrawn: Vec<RollingResult<u32, i64>> =
			engine.apply(&mut store, buckets, 4, row_key, sum_combine).unwrap();

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
		// The runnable engine replaces the O(buffer) recombine with a running accumulator kept by
		// merge/unmerge, so its observable behavior must be indistinguishable from the recombine
		// engine on an identical workload. A divergence means the maintenance missed a mutation path.
		let mut recombine_store = MockStore::default();
		let mut runnable_store = MockStore::default();
		let mut recombine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut runnable = RollingEngine::<u32, DateTime, SumAccumulator>::new_runnable(test_config());

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
				let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
				for &(group, coord, value, is_add) in plan {
					let event = if is_add {
						AccumulatorEvent::Add(value)
					} else {
						AccumulatorEvent::Remove(value)
					};
					buckets.entry((group, at_millis(coord))).or_default().push(event);
				}
				buckets
			};
			let recombine_out = recombine
				.apply_evicting(
					&mut recombine_store,
					build(&plan),
					RollingEviction::Before(at_millis(cutoff)),
					row_key,
					SumAccumulator::default,
					sum_combine,
				)
				.unwrap();
			let runnable_out = runnable
				.apply_running(
					&mut runnable_store,
					build(&plan),
					RollingEviction::Before(at_millis(cutoff)),
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
				let recombine_exp = recombine
					.expire_before(&mut recombine_store, at_millis(cutoff), sum_combine)
					.unwrap();
				let runnable_exp =
					runnable.expire_before_running(&mut runnable_store, at_millis(cutoff)).unwrap();
				assert_eq!(
					describe_expiries(&recombine_exp),
					describe_expiries(&runnable_exp),
					"expiry diverged from the recombine at round {round}"
				);
				added.retain(|(_, coord, _)| *coord > cutoff);
			}
			coord_base += roll(20);
		}

		assert_eq!(
			recombine_store.index_entry_count(),
			runnable_store.index_entry_count(),
			"expiry-index bookkeeping diverged"
		);

		// Drain everything: terminal removes must match group-for-group.
		let recombine_final =
			recombine.expire_before(&mut recombine_store, past_every_coord(), sum_combine).unwrap();
		let runnable_final = runnable.expire_before_running(&mut runnable_store, past_every_coord()).unwrap();
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
		// The two paths share per-coord storage, so coords written by apply_evicting must fold into
		// the running accumulator the first time the runnable path touches the group, on both the
		// apply and the expiry path.
		let mut store = MockStore::default();
		let mut recombine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(5)]);
		buckets.insert((1u32, at_millis(20)), vec![AccumulatorEvent::Add(7)]);
		recombine
			.apply_evicting(
				&mut store,
				buckets,
				RollingEviction::Before(at_millis(0)),
				row_key,
				SumAccumulator::default,
				sum_combine,
			)
			.unwrap();

		let mut runnable = RollingEngine::<u32, DateTime, SumAccumulator>::new_runnable(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(30)), vec![AccumulatorEvent::Add(100)]);
		let out = runnable
			.apply_running(
				&mut store,
				buckets,
				RollingEviction::Before(at_millis(0)),
				row_key,
				SumAccumulator::default,
			)
			.unwrap();
		assert_eq!(
			describe(&out),
			vec![(1u32, EmitKind::Update, 112i64)],
			"bootstrap must fold the pre-existing buffer into the running sum"
		);

		let expired = runnable.expire_before_running(&mut store, at_millis(20)).unwrap();
		assert_eq!(
			describe_expiries(&expired),
			vec![(1u32, Some(100i64))],
			"expiring the pre-fix coords must subtract exactly their contributions"
		);

		// A fresh runnable engine over the flushed state reads the persisted running entry back
		// without bootstrapping, and drains to a terminal remove.
		let mut reopened = RollingEngine::<u32, DateTime, SumAccumulator>::new_runnable(test_config());
		let drained = reopened.expire_before_running(&mut store, past_every_coord()).unwrap();
		assert_eq!(
			describe_expiries(&drained),
			vec![(1u32, None)],
			"the last coord expiring must terminally remove"
		);
	}

	#[test]
	fn per_coord_storage_leaves_nothing_behind_after_terminal_drain() {
		// After every group expires no coord, running or expiry-index entry may remain. The two
		// apply paths share per-coord storage, so a leak on either is the unbounded state growth
		// this engine exists to prevent.
		let mut store = MockStore::default();
		let mut recombine = RollingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(5)]);
		buckets.insert((1u32, at_millis(20)), vec![AccumulatorEvent::Add(7)]);
		recombine
			.apply_evicting(
				&mut store,
				buckets,
				RollingEviction::Before(at_millis(0)),
				row_key,
				SumAccumulator::default,
				sum_combine,
			)
			.unwrap();
		assert_eq!(
			store.buffer_coord_count::<SumAccumulator>(),
			2,
			"the recombine path persists both coords in the group's buffer"
		);

		let mut runnable = RollingEngine::<u32, DateTime, SumAccumulator>::new_runnable(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((2u32, at_millis(30)), vec![AccumulatorEvent::Add(1)]);
		buckets.insert((1u32, at_millis(30)), vec![AccumulatorEvent::Add(100)]);
		runnable.apply_running(
			&mut store,
			buckets,
			RollingEviction::Before(at_millis(0)),
			row_key,
			SumAccumulator::default,
		)
		.unwrap();
		assert_eq!(store.buffer_coord_count::<SumAccumulator>(), 4, "every live coord is persisted");
		assert_eq!(store.buffer_entry_count(), 2, "each live group persists one buffer entry");
		assert_eq!(store.running_entry_count(), 2, "each live group persists one running entry");

		let drained = runnable.expire_before_running(&mut store, past_every_coord()).unwrap();
		assert_eq!(drained.len(), 2, "both groups drain");
		assert!(drained.iter().all(|e| matches!(e, RollingExpiry::Remove { .. })));
		assert_eq!(store.buffer_entry_count(), 0, "terminal removal must delete the group's buffer entry");
		assert_eq!(store.running_entry_count(), 0, "terminal removal must delete the running entry");
		assert_eq!(store.index_entry_count(), 0, "terminal removal must delete the expiry index entry");
	}

	#[test]
	fn lagged_runnable_engine_matches_a_semantic_oracle_across_seeded_churn() {
		// The lagged fast path keeps a running accumulator plus a merge frontier at high_water - lag
		// rather than recombining. Emissions fold into a visible-row map checked against an
		// independent oracle each round, so an early merge, missed crossing or double count shows up.
		const LAG: u64 = 5;
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new_runnable(test_config())
			.with_lag(millis(LAG));

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

			let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
			for &(group, coord, value, is_add) in &plan {
				let event = if is_add {
					AccumulatorEvent::Add(value)
				} else {
					AccumulatorEvent::Remove(value)
				};
				buckets.entry((group, at_millis(coord))).or_default().push(event);
			}
			let out = engine
				.apply_running(
					&mut store,
					buckets,
					RollingEviction::Before(at_millis(cutoff)),
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
				let expiries = engine.expire_before_running(&mut store, at_millis(cutoff)).unwrap();
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

		let drained = engine.expire_before_running(&mut store, past_every_coord()).unwrap();
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
		assert_eq!(store.buffer_entry_count(), 0, "the terminal drain must delete every buffer entry");
		assert_eq!(store.running_entry_count(), 0, "the terminal drain must delete every running entry");
		assert_eq!(store.index_entry_count(), 0, "the terminal drain must delete every index entry");
	}

	#[test]
	fn lagged_running_holds_back_coords_within_the_lag_horizon() {
		// A coord contributes only once the group's high water has moved at least lag past it, so a
		// first event emits nothing, later events pull older coords across the frontier, a
		// retraction of a pending coord is invisible, and only-pending eviction withdraws the row.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new_runnable(test_config())
			.with_lag(millis(10));

		let apply = |engine: &mut RollingEngine<u32, DateTime, SumAccumulator>,
		             store: &mut MockStore,
		             coord: u64,
		             value: i64,
		             is_add: bool,
		             cutoff: u64| {
			let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
			let event = if is_add {
				AccumulatorEvent::Add(value)
			} else {
				AccumulatorEvent::Remove(value)
			};
			buckets.insert((1u32, at_millis(coord)), vec![event]);
			engine.apply_running(
				store,
				buckets,
				RollingEviction::Before(at_millis(cutoff)),
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
		assert_eq!(
			store.buffer_coord_count::<SumAccumulator>(),
			1,
			"the pending coord survives the withdrawal"
		);
		assert_eq!(store.running_entry_count(), 0, "a group with no merged coord persists no running entry");
	}

	#[test]
	fn lagged_expiry_retains_pending_coords() {
		// The blob recombine destroys the whole buffer when a due group has no coord older than
		// newest - lag, losing pending coords that would have slid into the window later. The fast
		// path withdraws the visible row but keeps them for a later event to surface.
		let mut store = MockStore::default();
		let mut engine = RollingEngine::<u32, DateTime, SumAccumulator>::new_runnable(test_config())
			.with_lag(millis(10));

		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(100)), vec![AccumulatorEvent::Add(5)]);
		buckets.insert((1u32, at_millis(115)), vec![AccumulatorEvent::Add(7)]);
		let out = engine
			.apply_running(
				&mut store,
				buckets,
				RollingEviction::Before(at_millis(0)),
				row_key,
				SumAccumulator::default,
			)
			.unwrap();
		assert_eq!(describe(&out), vec![(1u32, EmitKind::Insert, 5i64)]);

		let expired = engine.expire_before_running(&mut store, at_millis(105)).unwrap();
		assert_eq!(
			describe_expiries(&expired),
			vec![(1u32, None)],
			"expiring the only merged coord withdraws the row"
		);
		assert_eq!(
			store.buffer_coord_count::<SumAccumulator>(),
			1,
			"the pending coord 115 must survive the expiry"
		);
		assert_eq!(store.index_entry_count(), 1, "the group stays indexed at its pending coord");

		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(130)), vec![AccumulatorEvent::Add(9)]);
		let out = engine
			.apply_running(
				&mut store,
				buckets,
				RollingEviction::Before(at_millis(105)),
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
