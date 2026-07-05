// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::{self, Debug, Formatter},
	hash::Hash,
	marker::PhantomData,
};

use reifydb_codec::key::encoded::{EncodedKey, IntoEncodedKey};
use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::window::{
	accumulator::WindowAccumulator,
	engine::{
		AccumulatorEvent, GroupMeta, MetaKey, WindowStateKey, config::WindowEngineConfig, meta_key_for,
		rolling::RollingBuckets,
	},
	span::Slot,
	state::StateCache,
	store::WindowStore,
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

#[derive(Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize + Ord, Accumulator: Serialize, SK: Serialize + Ord, Output: Serialize",
	deserialize = "C: serde::de::DeserializeOwned + Ord, Accumulator: serde::de::DeserializeOwned, \
	               SK: serde::de::DeserializeOwned + Ord, Output: serde::de::DeserializeOwned"
))]
struct GroupState<C, Accumulator, SK, Output> {
	buffer: MultiRollingBuffer<C, Accumulator>,
	last_emit: MultiRollingEmit<SK, Output>,
}

impl<C: Ord, Accumulator, SK: Ord, Output> Default for GroupState<C, Accumulator, SK, Output> {
	fn default() -> Self {
		Self {
			buffer: BTreeMap::new(),
			last_emit: BTreeMap::new(),
		}
	}
}

impl<C: Ord + Clone, Accumulator: Clone, SK: Ord + Clone, Output: Clone> Clone
	for GroupState<C, Accumulator, SK, Output>
{
	fn clone(&self) -> Self {
		Self {
			buffer: self.buffer.clone(),
			last_emit: self.last_emit.clone(),
		}
	}
}

impl<C, Accumulator, SK, Output> Debug for GroupState<C, Accumulator, SK, Output> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("GroupState")
			.field("buffer_len", &self.buffer.len())
			.field("last_emit_len", &self.last_emit.len())
			.finish()
	}
}

type MetaLoaded<G, C> = HashMap<G, GroupMeta<C>>;
type StateRows<G> = HashMap<G, RowNumber>;

struct GroupSlot<C, Accumulator, SK, Output> {
	state_row_number: RowNumber,
	buffer: MultiRollingBuffer<C, Accumulator>,
	prior_emit: MultiRollingEmit<SK, Output>,
	buffer_changed: bool,
}

pub struct MultiRollingEngine<G, C, Accumulator, SK, Output> {
	groups: StateCache<WindowStateKey, GroupState<C, Accumulator, SK, Output>>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	_pd: PhantomData<G>,
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
			groups: StateCache::<WindowStateKey, GroupState<C, Accumulator, SK, Output>>::new_internal(
				config.state_cache_capacity(),
			),
			meta: StateCache::<MetaKey, GroupMeta<C>>::new_internal(config.internal_state_cache_capacity()),
			_pd: PhantomData,
		}
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
		self.groups.flush(store)?;
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
		let state_keys: Vec<WindowStateKey> = resolved_rows.iter().map(|(rn, _)| WindowStateKey(*rn)).collect();
		for (group, (state_row_number, _)) in resolve_order.into_iter().zip(resolved_rows) {
			state_rows.insert(group, state_row_number);
		}
		self.groups.warm(store, &state_keys)?;
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
					let GroupState {
						buffer,
						last_emit: prior_emit,
					} = self.groups.get(store, &WindowStateKey(state_row_number))?.unwrap_or_default();
					group_slots.insert(
						group.clone(),
						GroupSlot {
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
				}
			}

			let combined = GroupState {
				buffer: slot.buffer,
				last_emit: new_emit,
			};
			self.groups.put(store, &WindowStateKey(slot.state_row_number), combined)?;
		}

		Ok(emits)
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
}
