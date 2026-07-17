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
use serde::{Serialize, de::DeserializeOwned};

use crate::{
	util::memory::{HeapSize, StateMemory},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKey, GroupMeta, MetaKey, config::WindowEngineConfig, load_buffer,
			meta_key_for, persist_buffer, rolling::RollingBuckets, sweep_stale_meta,
		},
		span::Slot,
		state::StateCache,
		store::WindowStore,
	},
};

pub type MultiRollingBuffer<C, Accumulator> = BTreeMap<C, Accumulator>;

pub type MultiRollingEmit<SK, Output> = BTreeMap<SK, Output>;

pub enum MultiEmit<Output> {
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

type MetaLoaded<G, C> = HashMap<G, GroupMeta<C>>;
type StateRows<G> = HashMap<G, RowNumber>;

struct GroupSlot<C, Accumulator, SK, Output> {
	state_row_number: RowNumber,
	buffer: MultiRollingBuffer<C, Accumulator>,
	loaded_coords: Vec<u64>,
	dirty: BTreeSet<u64>,
	prior_emit: MultiRollingEmit<SK, Output>,
	buffer_changed: bool,
}

pub struct MultiRollingEngine<G, C, Accumulator, SK, Output> {
	last_emit: StateCache<EmitKey, MultiRollingEmit<SK, Output>>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	meta_low_water: Option<u64>,
	_pd: PhantomData<(G, C, Accumulator)>,
}

impl<G, C, Accumulator, SK, Output> MultiRollingEngine<G, C, Accumulator, SK, Output>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Accumulator: WindowAccumulator,
	SK: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	Output: Clone + Debug + PartialEq + Serialize + DeserializeOwned,
	for<'a> &'a G: IntoEncodedKey,
{
	pub fn new(config: WindowEngineConfig) -> Self {
		Self {
			last_emit: StateCache::<EmitKey, MultiRollingEmit<SK, Output>>::new_internal(
				config.state_cache_capacity(),
			),
			meta: StateCache::<MetaKey, GroupMeta<C>>::new_internal(config.internal_state_cache_capacity()),
			meta_low_water: None,
			_pd: PhantomData,
		}
	}

	pub fn approximate_memory(&self) -> StateMemory
	where
		SK: HeapSize,
		Output: HeapSize,
	{
		self.last_emit.approximate_memory() + self.meta.approximate_memory()
	}

	pub fn expire_meta<S: WindowStore>(&mut self, store: &mut S, threshold: u64) -> Result<usize> {
		sweep_stale_meta(store, &mut self.meta, threshold, &mut self.meta_low_water)
	}

	pub fn apply<S, SKF, RKF, CB>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, Accumulator::Contribution>,
		capacity: usize,
		state_key: SKF,
		row_key: RKF,
		combine: CB,
	) -> Result<Vec<MultiEmit<Output>>>
	where
		S: WindowStore,
		SKF: Fn(&G) -> EncodedKey,
		RKF: Fn(&G, &SK) -> EncodedKey,
		CB: Fn(&G, &MultiRollingBuffer<C, Accumulator>) -> MultiRollingEmit<SK, Output>,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
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

	pub fn flush<S: WindowStore>(&mut self, store: &mut S) -> Result<()> {
		self.last_emit.flush(store)?;
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
				let m = self.meta.take(store, &meta_key_for(group))?.unwrap_or_default();
				meta_loaded.insert(group.clone(), m);
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
		S: WindowStore,
		SKF: Fn(&G) -> EncodedKey,
	{
		let mut state_rows: StateRows<G> = HashMap::new();
		let mut resolve_order: Vec<G> = Vec::new();
		let mut state_lookup_keys: Vec<EncodedKey> = Vec::new();
		let mut seen: BTreeSet<G> = BTreeSet::new();
		for (group, coord) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.high_water);
			if initial_high_water.is_none_or(|hw| *coord >= hw) && seen.insert(group.clone()) {
				resolve_order.push(group.clone());
				state_lookup_keys.push(state_key(group));
			}
		}
		let resolved_rows = store.get_or_create_row_numbers(&state_lookup_keys)?;
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
		let emit_keys: Vec<EmitKey> = resolved_rows.iter().map(|(rn, _)| EmitKey(*rn)).collect();
		for (group, (state_row_number, _)) in resolve_order.into_iter().zip(resolved_rows) {
			state_rows.insert(group, state_row_number);
		}
		self.last_emit.warm(store, &emit_keys)?;
		Ok(state_rows)
	}

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
		S: WindowStore,
		SKF: Fn(&G) -> EncodedKey,
	{
		let mut group_slots: BTreeMap<G, GroupSlot<C, Accumulator, SK, Output>> = BTreeMap::new();

		for ((group, coord), events) in buckets {
			let meta = meta_loaded.entry(group.clone()).or_default();

			let slot = match group_slots.get_mut(&group) {
				Some(s) => s,
				None => {
					let state_row_number = match state_rows.get(&group) {
						Some(&rn) => rn,
						None => {
							let key = state_key(&group);
							let (rn, _is_new) = store.get_or_create_row_number(&key)?;
							rn
						}
					};
					let (buffer, loaded_coords): (MultiRollingBuffer<C, Accumulator>, Vec<u64>) =
						load_buffer(store, state_row_number)?;
					let prior_emit = self
						.last_emit
						.get(store, &EmitKey(state_row_number))?
						.unwrap_or_default();
					group_slots.insert(
						group.clone(),
						GroupSlot {
							state_row_number,
							buffer,
							loaded_coords,
							dirty: BTreeSet::new(),
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
			slot.dirty.insert(coord.order_key());

			let next_high_water = match meta.high_water {
				Some(hw) if hw > coord => hw,
				_ => coord,
			};
			reifydb_assertions! {
				assert!(
					next_high_water >= coord,
					"high_water regressed below the window coord it just admitted, so the next batch would \
					 treat an already-processed window as late and silently drop its events (coord={coord:?}, \
					 prev_high_water={prev:?}, next_high_water={next_high_water:?})",
					prev = meta.high_water
				);
				if let Some(prev) = meta.high_water {
					assert!(
						next_high_water >= prev,
						"high_water moved backwards across an admit, breaking the monotonic late-event \
						 cutoff that buried-window dropping relies on (coord={coord:?}, prev_high_water={prev:?}, \
						 next_high_water={next_high_water:?})"
					);
				}
			}
			meta.high_water = Some(next_high_water);
		}

		Ok(group_slots)
	}

	fn diff_emits<S, RKF, CB>(
		&mut self,
		store: &mut S,
		group_slots: BTreeMap<G, GroupSlot<C, Accumulator, SK, Output>>,
		row_key: &RKF,
		combine: &CB,
	) -> Result<Vec<MultiEmit<Output>>>
	where
		S: WindowStore,
		RKF: Fn(&G, &SK) -> EncodedKey,
		CB: Fn(&G, &MultiRollingBuffer<C, Accumulator>) -> MultiRollingEmit<SK, Output>,
	{
		let mut emits: Vec<MultiEmit<Output>> = Vec::new();

		for (group, slot) in group_slots {
			if !slot.buffer_changed {
				continue;
			}
			let new_emit = combine(&group, &slot.buffer);

			for (sk, new_out) in &new_emit {
				let key = row_key(&group, sk);
				let (rn, _is_new_alloc) = store.get_or_create_row_number(&key)?;
				match slot.prior_emit.get(sk) {
					Some(prior_out) => {
						if prior_out != new_out {
							emits.push(MultiEmit::Update {
								row_number: rn,
								prior: prior_out.clone(),
								value: new_out.clone(),
							});
						}
					}
					None => {
						emits.push(MultiEmit::Insert {
							row_number: rn,
							value: new_out.clone(),
						});
					}
				}
			}
			for (sk, prior_out) in &slot.prior_emit {
				if !new_emit.contains_key(sk) {
					let key = row_key(&group, sk);
					let (rn, _is_new_alloc) = store.get_or_create_row_number(&key)?;
					emits.push(MultiEmit::Remove {
						row_number: rn,
						value: prior_out.clone(),
					});
					store.drop_row_number(&key)?;
				}
			}

			persist_buffer(store, slot.state_row_number, &slot.buffer, &slot.loaded_coords, &slot.dirty)?;
			if new_emit.is_empty() {
				self.last_emit.remove(store, &EmitKey(slot.state_row_number))?;
			} else {
				self.last_emit.put(store, &EmitKey(slot.state_row_number), new_emit)?;
			}
		}

		Ok(emits)
	}

	fn persist_meta<S: WindowStore>(&mut self, store: &mut S, meta_loaded: MetaLoaded<G, C>) -> Result<()> {
		for (group, meta) in meta_loaded {
			self.meta.put(store, &meta_key_for(&group), meta)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_codec::key::encoded::EncodedKey;

	use super::{MultiEmit, MultiRollingBuffer, MultiRollingEngine};
	use crate::window::engine::{
		AccumulatorEvent,
		config::WindowEngineConfig,
		rolling::RollingBuckets,
		test_support::{MockStore, SumAccumulator},
	};

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder().state_cache_capacity(8).internal_state_cache_capacity(64).build()
	}

	fn state_key(group: &u32) -> EncodedKey {
		EncodedKey::builder().u32(*group).build()
	}

	fn row_key(group: &u32, sk: &u32) -> EncodedKey {
		EncodedKey::builder().u32(*group).u32(*sk).build()
	}

	fn combine(_group: &u32, buffer: &MultiRollingBuffer<u64, SumAccumulator>) -> BTreeMap<u32, i64> {
		let mut out = BTreeMap::new();
		if !buffer.is_empty() {
			out.insert(0u32, buffer.values().map(|a| a.sum).sum());
		}
		out
	}

	#[test]
	fn group_state_survives_restart() {
		// multi_rolling bundles a group's rolling buffer and its last emitted ranking into one
		// persisted GroupState. When the group empties under retraction, the vanishing ranked key is
		// withdrawn using the persisted `last_emit`. Dropping the engine between the publish and the
		// retraction (a restart) forces the GroupState to be reloaded from the store. It would fail if
		// the GroupState (buffer + last_emit) failed to round-trip - a serialization break, or
		// last_emit not being persisted.
		let mut store = MockStore::default();

		let mut engine = MultiRollingEngine::<u32, u64, SumAccumulator, u32, i64>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Add(5)]);
		let published = engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(published.len(), 1);
		let published_row = match &published[0] {
			MultiEmit::Insert {
				row_number,
				value,
			} => {
				assert_eq!(*value, 5);
				*row_number
			}
			_ => panic!("expected an Insert for the newly published group"),
		};

		// Restart: a brand new engine with empty caches, forced to reload the persisted GroupState.
		let mut engine = MultiRollingEngine::<u32, u64, SumAccumulator, u32, i64>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Remove(5)]);
		let withdrawn = engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(withdrawn.len(), 1, "emptying the group emits exactly one terminal diff");
		match &withdrawn[0] {
			MultiEmit::Remove {
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
	fn withdrawn_ranking_reclaims_its_row_number_mapping() {
		// Every ranked (group, secondary) mints a row-number mapping ('M') via get_or_create_row_number.
		// When the ranking is withdrawn (its secondary drops out of the emit) that mapping must be
		// reclaimed, or 'M' grows per distinct ranked key ever seen - a leak the emitted Remove alone
		// does not close, since Remove only withdraws the view row, not the internal mapping.
		let mut store = MockStore::default();
		// `combine` publishes the group's ranking under secondary key 0 (see the helper below), so the
		// ranked row's mapping is row_key(group=1, sk=0) - distinct from the rolling coord (10).
		let ranked_key = row_key(&1, &0);

		let mut engine = MultiRollingEngine::<u32, u64, SumAccumulator, u32, i64>::new(test_config());
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Add(5)]);
		engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert!(store.contains_row_mapping(&ranked_key), "publishing the ranking mints its mapping");

		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Remove(5)]);
		engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
		engine.flush(&mut store).unwrap();
		assert!(
			!store.contains_row_mapping(&ranked_key),
			"withdrawing the ranking must reclaim its row-number mapping, not leak it"
		);
	}

	#[test]
	fn group_state_survives_lru_eviction() {
		// The other way the GroupState is read back is LRU eviction, no restart needed: the group cache
		// holds only 8 groups, so tracking more evicts the oldest and the next access re-reads it from
		// the store. We publish 11 groups so group 1 is evicted, flush, then retract group 1 and assert
		// its GroupState reloads and the vanishing ranked key is withdrawn with the persisted value.
		let mut store = MockStore::default();
		let mut engine = MultiRollingEngine::<u32, u64, SumAccumulator, u32, i64>::new(test_config());

		let mut published_row_1 = None;
		for group in 1u32..=11u32 {
			let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
			buckets.insert((group, 10u64), vec![AccumulatorEvent::Add(i64::from(group))]);
			let out = engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
			if group == 1 {
				assert_eq!(out.len(), 1);
				published_row_1 = match &out[0] {
					MultiEmit::Insert {
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

		// Group 1 was published first and pushed out of the 8-slot group cache by the later groups, so
		// the same engine must re-read its GroupState from the store to apply this retraction.
		let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, 10u64), vec![AccumulatorEvent::Remove(1)]);
		let withdrawn = engine.apply(&mut store, buckets, 4, state_key, row_key, combine).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(withdrawn.len(), 1, "emptying the evicted group emits exactly one terminal diff");
		match &withdrawn[0] {
			MultiEmit::Remove {
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
		// After the storage split the buffer lives as per-coord entries and the
		// ranking as a separate last_emit entry. The engine must still emit exactly
		// what a from-scratch recombine would across a seeded workload of adds,
		// retractions, and capacity eviction. A single ranked key (SK 0 = sum over
		// the live buffer) makes the visible state one value we compare against an
		// independent live-buffer oracle after every batch. Storing blobs, dropping
		// or keeping the wrong coords on eviction, or mis-persisting last_emit would
		// surface as a divergence at the exact round.
		const CAP: usize = 4;
		let mut store = MockStore::default();
		let mut engine = MultiRollingEngine::<u32, u64, SumAccumulator, u32, i64>::new(test_config());

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

			let mut buckets: RollingBuckets<u32, u64, i64> = BTreeMap::new();
			for &(coord, value, is_add) in &plan {
				let ev = if is_add {
					AccumulatorEvent::Add(value)
				} else {
					AccumulatorEvent::Remove(value)
				};
				buckets.entry((1u32, coord)).or_default().push(ev);
			}
			let emits = engine.apply(&mut store, buckets, CAP, state_key, row_key, combine).unwrap();
			engine.flush(&mut store).unwrap();
			for e in &emits {
				match e {
					MultiEmit::Insert {
						value,
						..
					}
					| MultiEmit::Update {
						value,
						..
					} => visible = Some(*value),
					MultiEmit::Remove {
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
