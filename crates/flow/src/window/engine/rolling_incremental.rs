// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
};

use reifydb_codec::{
	key::encoded::{EncodedKey, IntoEncodedKey},
	state::OperatorState,
};
use reifydb_core::{
	key::operator_state::{GroupId, GroupSet},
	metrics::heap::{HeapSize, StateCompleteness, StateMemory},
	state::{cache::StateCache, store::StateStore},
};
use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};

use crate::window::{
	accumulator::WindowAccumulator,
	engine::{
		AccumulatorEvent, BatchMeta, EmitKind, GroupMeta, MetaKey, RunningKey, WindowStateKey,
		config::WindowEngineConfig,
		decode_meta_key, load_batch_meta, meta_key_for, meta_range, persist_batch_meta,
		rolling::{RollingBuckets, RollingBuffer, RollingResult},
		sweep_stale_meta,
	},
	span::Slot,
};

type MetaLoaded<G, C> = HashMap<G, BatchMeta<C>>;
type BufferRows<G> = HashMap<G, (GroupId, RowNumber, bool)>;

struct GroupSlot<C, Accumulator, Running, Output> {
	group_id: GroupId,
	row_number: RowNumber,
	is_new: bool,
	buffer: RollingBuffer<C, Accumulator>,
	running: Running,
	was_empty_before: bool,
	buffer_changed: bool,
	prior_output: Option<Output>,
}

pub struct RollingIncrementalEngine<G, C, Accumulator, Running> {
	buffers: StateCache<WindowStateKey, RollingBuffer<C, Accumulator>>,
	running: StateCache<RunningKey, Running>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	meta_low_water: Option<u64>,
	hydrated: bool,
	_pd: PhantomData<G>,
}

impl<G, C, Accumulator, Running> RollingIncrementalEngine<G, C, Accumulator, Running>
where
	G: Clone + Eq + Ord + Hash + Debug,
	C: Slot + Hash,
	Accumulator: WindowAccumulator,
	Running: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
	C: HeapSize,
	GroupMeta<C>: OperatorState,
	RollingBuffer<C, Accumulator>: OperatorState + HeapSize,
{
	pub fn new(config: WindowEngineConfig) -> Self {
		Self {
			buffers: StateCache::<WindowStateKey, RollingBuffer<C, Accumulator>>::new(config.budget()),
			running: StateCache::<RunningKey, Running>::new(config.budget()),
			meta: StateCache::<MetaKey, GroupMeta<C>>::new(config.budget()),
			meta_low_water: None,
			hydrated: false,
			_pd: PhantomData,
		}
	}

	pub fn expire_meta<S: StateStore>(&mut self, store: &mut S, threshold: u64) -> Result<usize> {
		self.hydrate_once(store)?;
		sweep_stale_meta(store, &mut self.meta, threshold, &mut self.meta_low_water)
	}

	pub fn invalidate_groups(&mut self, groups: &GroupSet) -> usize {
		self.buffers.invalidate_group_data(groups) + self.running.invalidate_group_data(groups)
	}

	fn hydrate_once<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		if self.hydrated {
			return Ok(());
		}
		self.meta.hydrate(store, meta_range(), decode_meta_key)?;
		self.hydrated = true;
		Ok(())
	}

	pub fn approximate_memory(&self) -> StateMemory {
		self.buffers.approximate_memory() + self.running.approximate_memory() + self.meta.approximate_memory()
	}

	pub fn dirty_memory(&self) -> StateMemory {
		self.buffers.dirty_memory() + self.running.dirty_memory() + self.meta.dirty_memory()
	}

	pub fn membership_memory(&self) -> StateMemory {
		self.buffers.membership_memory() + self.running.membership_memory() + self.meta.membership_memory()
	}

	pub fn completeness(&self) -> StateCompleteness {
		self.buffers.completeness().merge(self.running.completeness()).merge(self.meta.completeness())
	}

	#[allow(clippy::too_many_arguments)]
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
		S: StateStore,
		K: Fn(&G) -> EncodedKey,
		WC: Fn(&Accumulator::Output) -> Running::Contribution,
		CR: Fn(&G, &Running, &Accumulator::Output, C) -> Option<Output>,
	{
		self.hydrate_once(store)?;
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let buffer_rows = self.resolve_buffer_rows(store, &buckets, &meta_loaded, &row_key)?;

		let mut group_slots: BTreeMap<G, GroupSlot<C, Accumulator, Running, Output>> = BTreeMap::new();

		for ((group, coord), events) in buckets {
			let meta = meta_loaded.entry(group.clone()).or_default();

			let slot = match group_slots.get_mut(&group) {
				Some(s) => s,
				None => {
					let (group_id, row_number, is_new) = match buffer_rows.get(&group) {
						Some(&resolved) => resolved,
						None => {
							let key = row_key(&group);
							let group_id = store.intern_group(&key)?;
							let (row_number, is_new) =
								store.get_or_create_row_number(group_id, &key)?;
							(group_id, row_number, is_new)
						}
					};
					let buffer: RollingBuffer<C, Accumulator> = self
						.buffers
						.get(store, &WindowStateKey::new(group_id, row_number))?
						.unwrap_or_default();
					let running: Running = self
						.running
						.get(store, &RunningKey::new(group_id, row_number))?
						.unwrap_or_default();
					let was_empty_before = buffer.is_empty();
					let prior_output = match buffer.iter().next_back() {
						Some((coord, accumulator)) => {
							accumulator.finalize().and_then(|newest| {
								combine_running(&group, &running, &newest, *coord)
							})
						}
						None => None,
					};
					group_slots.insert(
						group.clone(),
						GroupSlot {
							group_id,
							row_number,
							is_new,
							buffer,
							running,
							was_empty_before,
							buffer_changed: false,
							prior_output,
						},
					);
					group_slots.get_mut(&group).expect("just inserted")
				}
			};

			let mut accumulator = slot.buffer.remove(&coord).unwrap_or_default();
			let old_value = accumulator.finalize();
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

			meta.observe(coord);
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
			self.buffers.put(store, &WindowStateKey::new(slot.group_id, slot.row_number), slot.buffer)?;
			self.running.put(store, &RunningKey::new(slot.group_id, slot.row_number), slot.running)?;

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

	pub fn flush<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		self.buffers.flush(store)?;
		self.running.flush(store)?;
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
		K: Fn(&G) -> EncodedKey,
	{
		let mut buffer_rows: BufferRows<G> = HashMap::new();
		let mut resolve_order: Vec<G> = Vec::new();
		let mut group_keys: Vec<EncodedKey> = Vec::new();
		let mut seen: BTreeSet<G> = BTreeSet::new();
		for (group, coord) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.initial);
			if initial_high_water.is_none_or(|hw| *coord >= hw) && seen.insert(group.clone()) {
				resolve_order.push(group.clone());
				group_keys.push(row_key(group));
			}
		}
		let mut resolved_rows: Vec<(GroupId, RowNumber, bool)> = Vec::with_capacity(group_keys.len());
		for key in &group_keys {
			let group = store.intern_group(key)?;
			let (row_number, is_new) = store.get_or_create_row_number(group, key)?;
			resolved_rows.push((group, row_number, is_new));
		}
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
		let buffer_keys: Vec<WindowStateKey> =
			resolved_rows.iter().map(|(group, rn, _)| WindowStateKey::new(*group, *rn)).collect();
		let running_keys: Vec<RunningKey> =
			resolved_rows.iter().map(|(group, rn, _)| RunningKey::new(*group, *rn)).collect();
		for (group, resolved) in resolve_order.into_iter().zip(resolved_rows) {
			buffer_rows.insert(group, resolved);
		}
		self.buffers.warm(store, &buffer_keys)?;
		self.running.warm(store, &running_keys)?;
		Ok(buffer_rows)
	}

	fn persist_meta<S: StateStore>(&mut self, store: &mut S, meta_loaded: MetaLoaded<G, C>) -> Result<()> {
		persist_batch_meta(store, &mut self.meta, meta_loaded)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::state::budget::OperatorStateBudgetHandle;

	use crate::window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind,
			config::WindowEngineConfig,
			rolling::{RollingBuckets, RollingResult},
			rolling_incremental::RollingIncrementalEngine,
			test_support::{MockStore, SumAccumulator},
		},
	};

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder(OperatorStateBudgetHandle::default()).build()
	}

	fn row_key(group: &u32) -> EncodedKey {
		EncodedKey::builder().u32(*group).build()
	}

	fn running_sum(_group: &u32, running: &SumAccumulator, _newest: &i64, _coord: u64) -> Option<i64> {
		running.finalize()
	}

	#[test]
	fn buffer_survives_restart_without_running_collision() {
		// rolling_incremental keeps two Data-backend caches - the rolling `buffers` and the `running`
		// accumulator - and both must live in distinct store keyspaces. They are keyed by the same
		// RowNumber, so if their keyspaces are not separated, `running` (flushed last) clobbers the
		// buffer's store slot and a later buffer read decodes running's bytes. Within one live engine
		// this is hidden because reads are served from each cache's in-memory map; a restart is one of
		// the two ways a read actually reaches the store. This test publishes a window, drops the
		// engine (a restart / panic-recovery), then retracts the only contribution with a fresh engine
		// whose caches are empty, and asserts the buffer is read back intact - the terminal Remove
		// still carries the originally published value. It fails if `buffers` and `running` share a
		// store key.
		let mut store = MockStore::default();

		let mut engine =
			RollingIncrementalEngine::<u32, u64, SumAccumulator, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Add(5)]);
		let published: Vec<RollingResult<u32, i64>> =
			engine.apply(&mut store, buckets, 4, row_key, |v: &i64| *v, running_sum).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(published.len(), 1);
		assert!(matches!(published[0].kind, EmitKind::Insert));
		assert_eq!(published[0].value, 5);

		// Restart: a brand new engine with empty caches, forced to read the persisted buffer and
		// running accumulator back from the store.
		let mut engine =
			RollingIncrementalEngine::<u32, u64, SumAccumulator, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Remove(5)]);
		let withdrawn: Vec<RollingResult<u32, i64>> =
			engine.apply(&mut store, buckets, 4, row_key, |v: &i64| *v, running_sum).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(withdrawn.len(), 1, "emptying the group emits exactly one terminal diff");
		assert!(
			matches!(withdrawn[0].kind, EmitKind::Remove),
			"the group emptied under retraction, so the last published row must be withdrawn"
		);
		assert_eq!(
			withdrawn[0].value, 5,
			"the withdrawn value is reconstructed from the persisted buffer plus running accumulator"
		);
		assert_eq!(
			withdrawn[0].row_number, published[0].row_number,
			"the withdrawal targets the same row that was published"
		);
	}

	#[test]
	fn buffer_survives_lru_eviction_without_running_collision() {
		// The second way a read reaches the store is LRU eviction - no restart needed. The state cache
		// holds only 8 groups, so an engine tracking more than 8 groups evicts the oldest ones; the
		// next access re-reads them from the store. This exercises the same buffers/running keyspace
		// collision as the restart test, but within a single long-lived engine. We publish 11 groups so
		// the earliest (group 1) is evicted, flush, then retract group 1 and assert its buffer is read
		// back intact. It fails if `buffers` and `running` share a store key.
		let mut store = MockStore::default();
		let mut engine =
			RollingIncrementalEngine::<u32, u64, SumAccumulator, SumAccumulator>::new(test_config());

		let mut published_group_1: Vec<RollingResult<u32, i64>> = Vec::new();
		for group in 1u32..=11u32 {
			let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
			buckets.insert((group, 10u64), vec![AccumulatorEvent::Add(i64::from(group))]);
			let out: Vec<RollingResult<u32, i64>> =
				engine.apply(&mut store, buckets, 4, row_key, |v: &i64| *v, running_sum).unwrap();
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
			engine.apply(&mut store, buckets, 4, row_key, |v: &i64| *v, running_sum).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(withdrawn.len(), 1, "emptying the evicted group emits exactly one terminal diff");
		assert!(
			matches!(withdrawn[0].kind, EmitKind::Remove),
			"the evicted group emptied under retraction, so the last published row must be withdrawn"
		);
		assert_eq!(
			withdrawn[0].value, 1,
			"the withdrawn value is reconstructed from the evicted group's persisted buffer and running"
		);
		assert_eq!(
			withdrawn[0].row_number, published_group_1[0].row_number,
			"the withdrawal targets the same row that was published for group 1"
		);
	}
}
