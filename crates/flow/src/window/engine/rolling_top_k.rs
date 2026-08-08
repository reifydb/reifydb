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
	row::operator::OperatorState,
};
use reifydb_core::{
	key::operator_group_state::GroupId,
	metrics::heap::{HeapSize, StateCompleteness, StateMemory},
	state::{cache::StateCache, map::PersistedMap, store::StateStore},
};
use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};

use crate::window::{
	accumulator::WindowAccumulator,
	engine::{
		AccumulatorEvent, BatchMeta, BufferKey, EmitKey, GroupMeta, MetaKey, config::WindowEngineConfig,
		decode_meta_key, load_batch_meta, meta_key_for, meta_range, persist_batch_meta,
		rolling::RollingBuckets, sweep_stale_meta,
	},
	span::Slot,
};

pub type RollingTopKBuffer<C, Accumulator> = BTreeMap<C, Accumulator>;

pub type RollingTopKEmit<SK, Output> = PersistedMap<SK, Output>;

pub enum TopKEmit<Output> {
	Insert {
		row_number: RowNumber,
		value: Output,
	},
	Update {
		row_number: RowNumber,
		prior: Output,
		value: Output,
	},
	Remove {
		row_number: RowNumber,
		value: Output,
	},
}

type MetaLoaded<G, C> = HashMap<G, BatchMeta<C>>;
type StateRows<G> = HashMap<G, (GroupId, RowNumber)>;

struct GroupSlot<C, Accumulator, SK, Output> {
	group_id: GroupId,
	state_row_number: RowNumber,
	buffer: PersistedMap<C, Accumulator>,
	prior_emit: RollingTopKEmit<SK, Output>,
	buffer_changed: bool,
}

pub struct RollingTopKEngine<G, C, Accumulator, SK, Output> {
	buffers: StateCache<BufferKey, PersistedMap<C, Accumulator>>,
	last_emit: StateCache<EmitKey, RollingTopKEmit<SK, Output>>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	meta_low_water: Option<u64>,
	hydrated: bool,
	_pd: PhantomData<(G, C, Accumulator)>,
}

impl<G, C, Accumulator, SK, Output> RollingTopKEngine<G, C, Accumulator, SK, Output>
where
	G: Clone + Eq + Ord + Hash + Debug,
	C: Slot + Hash,
	Accumulator: WindowAccumulator,
	SK: Clone + Eq + Ord + Hash + Debug,
	Output: Clone + Debug + PartialEq,
	for<'a> &'a G: IntoEncodedKey,
	C: HeapSize,
	SK: HeapSize,
	Output: HeapSize,
	GroupMeta<C>: OperatorState,
	RollingTopKEmit<SK, Output>: OperatorState,
	PersistedMap<C, Accumulator>: OperatorState,
{
	pub fn new(config: WindowEngineConfig) -> Self {
		Self {
			buffers: StateCache::<BufferKey, PersistedMap<C, Accumulator>>::new(config.budget()),
			last_emit: StateCache::<EmitKey, RollingTopKEmit<SK, Output>>::new(config.budget()),
			meta: StateCache::<MetaKey, GroupMeta<C>>::new(config.budget()),
			meta_low_water: None,
			hydrated: false,
			_pd: PhantomData,
		}
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
		self.buffers.approximate_memory() + self.last_emit.approximate_memory() + self.meta.approximate_memory()
	}

	pub fn dirty_memory(&self) -> StateMemory {
		self.buffers.dirty_memory() + self.last_emit.dirty_memory() + self.meta.dirty_memory()
	}

	pub fn membership_memory(&self) -> StateMemory {
		self.buffers.membership_memory() + self.last_emit.membership_memory() + self.meta.membership_memory()
	}

	pub fn completeness(&self) -> StateCompleteness {
		self.buffers.completeness().merge(self.last_emit.completeness()).merge(self.meta.completeness())
	}

	pub fn expire_meta<S: StateStore>(&mut self, store: &mut S, threshold: u64) -> Result<usize> {
		sweep_stale_meta(store, &mut self.meta, threshold, &mut self.meta_low_water)
	}

	#[allow(clippy::too_many_arguments)]
	pub fn apply<S, SKF, RKF, CB>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		capacity: usize,
		state_key: SKF,
		row_key: RKF,
		combine: CB,
	) -> Result<Vec<TopKEmit<Output>>>
	where
		S: StateStore,
		SKF: Fn(&G) -> EncodedKey,
		RKF: Fn(&G, &SK) -> EncodedKey,
		CB: Fn(&G, &RollingTopKBuffer<C, Accumulator>) -> RollingTopKEmit<SK, Output>,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		self.hydrate_once(store)?;
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let state_rows = self.resolve_state_rows(store, &buckets, &meta_loaded, &state_key)?;
		let group_slots = self.apply_events_into_buffers(
			store,
			buckets,
			&mut meta_loaded,
			&state_rows,
			&state_key,
			capacity,
		)?;
		let emits = self.diff_emits(store, group_slots, &row_key, &combine)?;
		self.persist_meta(store, meta_loaded)?;
		Ok(emits)
	}

	pub fn flush<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		self.buffers.flush(store)?;
		self.last_emit.flush(store)?;
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

	fn resolve_state_rows<S, SKF>(
		&mut self,
		store: &mut S,
		buckets: &RollingBuckets<G, C, Accumulator::Contribution>,
		meta_loaded: &MetaLoaded<G, C>,
		state_key: &SKF,
	) -> Result<StateRows<G>>
	where
		S: StateStore,
		SKF: Fn(&G) -> EncodedKey,
	{
		let mut state_rows: StateRows<G> = HashMap::new();
		let mut resolve_order: Vec<G> = Vec::new();
		let mut state_lookup_keys: Vec<EncodedKey> = Vec::new();
		let mut seen: BTreeSet<G> = BTreeSet::new();
		for (group, coord) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.initial);
			if initial_high_water.is_none_or(|hw| *coord >= hw) && seen.insert(group.clone()) {
				resolve_order.push(group.clone());
				state_lookup_keys.push(state_key(group));
			}
		}
		let mut resolved_rows: Vec<(GroupId, RowNumber)> = Vec::with_capacity(state_lookup_keys.len());
		for key in &state_lookup_keys {
			let group = store.intern_group(key)?;
			let (row_number, _is_new) = store.get_or_create_row_number(group, key)?;
			resolved_rows.push((group, row_number));
		}
		reifydb_assertions! {
			let resolved = resolved_rows.len();
			let requested = state_lookup_keys.len();
			assert!(
				resolved == requested,
				"get_or_create_row_numbers returned {resolved} rows for {requested} group keys; \
				 the zip below pairs resolve_order with resolved_rows by position, so a length \
				 mismatch would silently leave some groups without a state_rows entry and route \
				 them through the per-bucket get_or_create_row_number fallback, diverging behaviour"
			);
		}
		let emit_keys: Vec<EmitKey> =
			resolved_rows.iter().map(|(group, rn)| EmitKey::new(*group, *rn)).collect();
		for (group, resolved) in resolve_order.into_iter().zip(resolved_rows) {
			state_rows.insert(group, resolved);
		}
		self.last_emit.warm(store, &emit_keys)?;
		Ok(state_rows)
	}

	#[allow(clippy::too_many_arguments)]
	fn apply_events_into_buffers<S, SKF>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		meta_loaded: &mut MetaLoaded<G, C>,
		state_rows: &StateRows<G>,
		state_key: &SKF,
		capacity: usize,
	) -> Result<BTreeMap<G, GroupSlot<C, Accumulator, SK, Output>>>
	where
		S: StateStore,
		SKF: Fn(&G) -> EncodedKey,
	{
		let mut group_slots: BTreeMap<G, GroupSlot<C, Accumulator, SK, Output>> = BTreeMap::new();

		for ((group, coord), events) in buckets {
			let meta = meta_loaded.entry(group.clone()).or_default();

			let slot = match group_slots.get_mut(&group) {
				Some(s) => s,
				None => {
					let (group_id, state_row_number) = match state_rows.get(&group) {
						Some(&resolved) => resolved,
						None => {
							let key = state_key(&group);
							let group_id = store.intern_group(&key)?;
							let (rn, _is_new) =
								store.get_or_create_row_number(group_id, &key)?;
							(group_id, rn)
						}
					};
					let buffer: PersistedMap<C, Accumulator> = self
						.buffers
						.get(store, &BufferKey::of_row(group_id, state_row_number))?
						.unwrap_or_default();
					let prior_emit = self
						.last_emit
						.get(store, &EmitKey::new(group_id, state_row_number))?
						.unwrap_or_default();
					group_slots.insert(
						group.clone(),
						GroupSlot {
							group_id,
							state_row_number,
							buffer,
							prior_emit,
							buffer_changed: false,
						},
					);
					group_slots.get_mut(&group).expect("just inserted")
				}
			};

			let mut accumulator = slot.buffer.remove(&coord).unwrap_or_default();
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
			while slot.buffer.len() > capacity {
				slot.buffer.pop_first();
			}
			slot.buffer_changed = true;

			reifydb_assertions! {
				let next_high_water = match meta.high_water() {
					Some(hw) if hw > coord => hw,
					_ => coord,
				};
				assert!(
					next_high_water >= coord,
					"high_water regressed below the window coord it just admitted, so the next batch would \
					 treat an already-processed window as late and silently drop its events (coord={coord:?}, \
					 prev_high_water={prev:?}, next_high_water={next_high_water:?})",
					prev = meta.high_water()
				);
				if let Some(prev) = meta.high_water() {
					assert!(
						next_high_water >= prev,
						"high_water moved backwards across an admit, breaking the monotonic late-event \
						 cutoff that buried-window dropping relies on (coord={coord:?}, prev_high_water={prev:?}, \
						 next_high_water={next_high_water:?})"
					);
				}
			}
			meta.observe(coord);
		}

		Ok(group_slots)
	}

	fn diff_emits<S, RKF, CB>(
		&mut self,
		store: &mut S,
		group_slots: BTreeMap<G, GroupSlot<C, Accumulator, SK, Output>>,
		row_key: &RKF,
		combine: &CB,
	) -> Result<Vec<TopKEmit<Output>>>
	where
		S: StateStore,
		RKF: Fn(&G, &SK) -> EncodedKey,
		CB: Fn(&G, &RollingTopKBuffer<C, Accumulator>) -> RollingTopKEmit<SK, Output>,
	{
		let mut emits: Vec<TopKEmit<Output>> = Vec::new();

		for (group, slot) in group_slots {
			if !slot.buffer_changed {
				continue;
			}
			let new_emit = combine(&group, &slot.buffer);

			for (sk, new_out) in &new_emit {
				let key = row_key(&group, sk);
				let (rn, is_new) = store.get_or_create_row_number(slot.group_id, &key)?;
				match (is_new, slot.prior_emit.get(sk)) {
					(true, _) => {
						emits.push(TopKEmit::Insert {
							row_number: rn,
							value: new_out.clone(),
						});
					}
					(false, Some(prior_out)) => {
						if prior_out != new_out {
							emits.push(TopKEmit::Update {
								row_number: rn,
								prior: prior_out.clone(),
								value: new_out.clone(),
							});
						}
					}
					(false, None) => {
						emits.push(TopKEmit::Update {
							row_number: rn,
							prior: new_out.clone(),
							value: new_out.clone(),
						});
					}
				}
			}
			for (sk, prior_out) in &slot.prior_emit {
				if !new_emit.contains_key(sk) {
					let key = row_key(&group, sk);
					let (rn, _is_new_alloc) =
						store.get_or_create_row_number(slot.group_id, &key)?;
					emits.push(TopKEmit::Remove {
						row_number: rn,
						value: prior_out.clone(),
					});
					store.remove_row_number(slot.group_id, &key)?;
				}
			}

			if slot.buffer.is_empty() {
				self.buffers.remove(store, &BufferKey::of_row(slot.group_id, slot.state_row_number))?;
			} else {
				self.buffers.put(
					store,
					&BufferKey::of_row(slot.group_id, slot.state_row_number),
					slot.buffer,
				)?;
			}
			if new_emit.is_empty() {
				self.last_emit.remove(store, &EmitKey::new(slot.group_id, slot.state_row_number))?;
			} else {
				self.last_emit.put(
					store,
					&EmitKey::new(slot.group_id, slot.state_row_number),
					new_emit,
				)?;
			}
		}

		Ok(emits)
	}

	fn persist_meta<S: StateStore>(&mut self, store: &mut S, meta_loaded: MetaLoaded<G, C>) -> Result<()> {
		persist_batch_meta(store, &mut self.meta, meta_loaded)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::state::{budget::OperatorStateBudgetHandle, store::StateStore};
	use reifydb_value::{factory::time::at_millis, value::datetime::DateTime};

	use super::{RollingTopKBuffer, RollingTopKEmit, RollingTopKEngine, TopKEmit};
	use crate::window::engine::{
		AccumulatorEvent,
		config::WindowEngineConfig,
		rolling::RollingBuckets,
		test_support::{MockStore, SumAccumulator},
	};

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder(OperatorStateBudgetHandle::default()).build()
	}

	fn state_key(group: &u32) -> EncodedKey {
		EncodedKey::builder().u32(*group).build()
	}

	fn row_key(group: &u32, sk: &u32) -> EncodedKey {
		EncodedKey::builder().u32(*group).u32(*sk).build()
	}

	fn combine(_group: &u32, buffer: &RollingTopKBuffer<DateTime, SumAccumulator>) -> RollingTopKEmit<u32, i64> {
		let mut out = BTreeMap::new();
		if !buffer.is_empty() {
			out.insert(0u32, buffer.values().map(|a| a.sum).sum());
		}
		out.into()
	}

	#[test]
	fn group_state_survives_restart() {
		// A group emptying under retraction withdraws the vanishing ranked key using the persisted
		// `last_emit`, so dropping the engine between publish and retraction forces the GroupState
		// back through the store. It fails on a serialization break or an unpersisted last_emit.
		let mut store = MockStore::default();

		let mut engine = RollingTopKEngine::<u32, DateTime, SumAccumulator, u32, i64>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(5)]);
		let published = engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(published.len(), 1);
		let published_row = match &published[0] {
			TopKEmit::Insert {
				row_number,
				value,
			} => {
				assert_eq!(*value, 5);
				*row_number
			}
			_ => panic!("expected an Insert for the newly published group"),
		};

		// A brand new engine with empty caches, forced to reload the persisted GroupState.
		let mut engine = RollingTopKEngine::<u32, DateTime, SumAccumulator, u32, i64>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Remove(5)]);
		let withdrawn = engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(withdrawn.len(), 1, "emptying the group emits exactly one terminal diff");
		match &withdrawn[0] {
			TopKEmit::Remove {
				row_number,
				value,
			} => {
				assert_eq!(
					*value, 5,
					"the withdrawn value is the reloaded last_emit, not a stale or zeroed value"
				);
				assert_eq!(
					*row_number, published_row,
					"the withdrawal targets the same row that was published"
				);
			}
			_ => panic!("the group emptied under retraction, so it must emit a terminal Remove"),
		}
	}

	#[test]
	fn a_group_whose_state_was_reclaimed_updates_its_ranked_row_rather_than_inserting_a_second() {
		// The data phase takes the buffer and the last emitted ranking together, so the group comes
		// back with no memory of what it ranked. The ranked row's mapping is the only thing left
		// that knows the sink still holds that row; an Insert against it ranks the key twice.
		let mut store = MockStore::default();
		let ranked_key = row_key(&1, &0);

		let mut engine = RollingTopKEngine::<u32, DateTime, SumAccumulator, u32, i64>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(5)]);
		let published = engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
		engine.flush(&mut store).unwrap();
		let published_row = match &published[0] {
			TopKEmit::Insert {
				row_number,
				..
			} => *row_number,
			_ => panic!("precondition: the first ranking is an insert"),
		};

		let group = store.lookup_group(&state_key(&1)).unwrap().expect("applying the group interns it");
		assert!(store.drop_group_data_entries() > 0, "precondition: the sweep must have erased something");
		assert!(
			store.contains_row_mapping(group, &ranked_key),
			"precondition: the identity half must survive the data phase"
		);

		let mut engine = RollingTopKEngine::<u32, DateTime, SumAccumulator, u32, i64>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(3)]);
		let republished = engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();

		assert_eq!(republished.len(), 1);
		match &republished[0] {
			TopKEmit::Update {
				row_number,
				..
			} => assert_eq!(
				*row_number, published_row,
				"the woken group must re-rank on the row it published"
			),
			_ => panic!("the ranked row survived the sweep, so this is an update and not a second insert"),
		}
	}

	#[test]
	fn withdrawn_ranking_reclaims_its_row_number_mapping() {
		// Every ranked (group, secondary) mints a row-number mapping, which must be reclaimed when
		// the ranking is withdrawn or the mapping keyspace grows per ranked key ever seen. The
		// emitted Remove does not close it: Remove withdraws the view row, not the mapping.
		let mut store = MockStore::default();
		// `combine` publishes the ranking under secondary key 0, so the ranked row's mapping is
		// row_key(group=1, sk=0), distinct from the rolling coord (10).
		let ranked_key = row_key(&1, &0);

		let mut engine = RollingTopKEngine::<u32, DateTime, SumAccumulator, u32, i64>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(5)]);
		engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
		engine.flush(&mut store).unwrap();
		// The mapping is scoped to the interned group, not NODE_SCOPE, and reclamation deletes by
		// group prefix - a lookup under the wrong group would report absence and pass while the
		// mapping leaked. The group is read back rather than assumed from the allocator.
		let group = store.lookup_group(&state_key(&1)).unwrap().expect("applying the group interns it");
		assert!(store.contains_row_mapping(group, &ranked_key), "publishing the ranking mints its mapping");

		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Remove(5)]);
		engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert!(
			!store.contains_row_mapping(group, &ranked_key),
			"withdrawing the ranking must reclaim its row-number mapping, not leak it"
		);
	}

	#[test]
	fn group_state_survives_lru_eviction() {
		// The other way the GroupState is read back is LRU eviction, with no restart: the cache
		// holds 8 groups, so tracking more evicts the oldest and the next access re-reads it.
		let mut store = MockStore::default();
		let mut engine = RollingTopKEngine::<u32, DateTime, SumAccumulator, u32, i64>::new(test_config());

		let mut published_row_1 = None;
		for group in 1u32..=11u32 {
			let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
			buckets.insert((group, at_millis(10)), vec![AccumulatorEvent::Add(i64::from(group))]);
			let out = engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
			if group == 1 {
				assert_eq!(out.len(), 1);
				published_row_1 = match &out[0] {
					TopKEmit::Insert {
						row_number,
						value,
					} => {
						assert_eq!(*value, 1);
						Some(*row_number)
					}
					_ => panic!("expected an Insert for group 1"),
				};
			}
		}
		engine.flush(&mut store).unwrap();
		let published_row_1 = published_row_1.expect("group 1 published an Insert");

		// Group 1 was pushed out of the 8-slot cache by the later groups, so the same engine must
		// re-read its GroupState from the store to apply this retraction.
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Remove(1)]);
		let withdrawn = engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(withdrawn.len(), 1, "emptying the evicted group emits exactly one terminal diff");
		match &withdrawn[0] {
			TopKEmit::Remove {
				row_number,
				value,
			} => {
				assert_eq!(*value, 1, "the withdrawn value is the reloaded last_emit for group 1");
				assert_eq!(
					*row_number, published_row_1,
					"the withdrawal targets the same row that was published for group 1"
				);
			}
			_ => panic!("the evicted group emptied under retraction, so it must emit a terminal Remove"),
		}
	}
	#[test]
	fn per_coord_churn_matches_a_recomputed_ranking_oracle() {
		// The buffer lives as per-coord entries and the ranking as a separate last_emit entry, but
		// the engine must still emit what a from-scratch recombine would. A single ranked key
		// reduces the visible state to one value, checked against a live-buffer oracle each batch.
		const CAP: usize = 4;
		let mut store = MockStore::default();
		let mut engine = RollingTopKEngine::<u32, DateTime, SumAccumulator, u32, i64>::new(test_config());

		let mut state = 0x1234_5678_9abc_def0u64;
		let mut roll = |bound: u64| {
			state = state.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
			(state >> 33) % bound
		};

		let mut live: BTreeMap<u64, (i64, u64)> = BTreeMap::new();
		let mut added: Vec<(u64, i64)> = Vec::new();
		let mut visible: Option<i64> = None;
		let mut coord_base = 100u64;

		for round in 0..200u64 {
			let mut plan: Vec<(u64, i64, bool)> = Vec::new();
			for _ in 0..=roll(3) {
				let coord = coord_base + roll(20);
				let value = roll(1_000) as i64 + 1;
				plan.push((coord, value, true));
				added.push((coord, value));
			}
			if round % 3 == 2 && !added.is_empty() {
				let (coord, value) = added.remove((roll(added.len() as u64)) as usize);
				plan.push((coord, value, false));
			}

			for &(coord, value, is_add) in &plan {
				let e = live.entry(coord).or_insert((0, 0));
				if is_add {
					e.0 += value;
					e.1 += 1;
				} else if e.1 > 0 {
					e.0 -= value;
					e.1 -= 1;
					if e.1 == 0 {
						live.remove(&coord);
					}
				} else {
					live.remove(&coord);
				}
			}
			while live.len() > CAP {
				let &lowest = live.keys().next().unwrap();
				live.remove(&lowest);
			}

			let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
			for &(coord, value, is_add) in &plan {
				let ev = if is_add {
					AccumulatorEvent::Add(value)
				} else {
					AccumulatorEvent::Remove(value)
				};
				buckets.entry((1u32, at_millis(coord))).or_default().push(ev);
			}
			let emits = engine.apply(&mut store, buckets, CAP, state_key, row_key, combine).unwrap();
			engine.flush(&mut store).unwrap();
			for e in &emits {
				match e {
					TopKEmit::Insert {
						value,
						..
					}
					| TopKEmit::Update {
						value,
						..
					} => visible = Some(*value),
					TopKEmit::Remove {
						..
					} => visible = None,
				}
			}

			let oracle = if live.is_empty() {
				None
			} else {
				Some(live.values().map(|(s, _)| *s).sum::<i64>())
			};
			assert_eq!(visible, oracle, "visible ranking diverged from the oracle after round {round}");
			coord_base += roll(10);
		}
	}
}
