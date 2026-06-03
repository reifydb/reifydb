// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::{self, Debug, Formatter},
	hash::Hash,
	marker::PhantomData,
};

use reifydb_value::reifydb_assertions;
use reifydb_value::{Result, value::row_number::RowNumber};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	window::{
		accumulator::WindowAccumulator,
		engine::{AccEvent, GroupMeta, MetaKey, meta_key_for, rolling::RollingBuckets},
		span::Slot,
		state::StateCache,
		store::WindowStore,
	},
};

/// The rolling buffer for one group (most recent windows, each an invertible
/// accumulator).
pub type MultiRollingBuffer<C, WAcc> = BTreeMap<C, WAcc>;

/// The rows a group currently emits, keyed by secondary key (e.g. rank).
pub type MultiRollingEmit<SK, Output> = BTreeMap<SK, Output>;

/// One emitted-row decision after diffing the new emit against the prior one.
pub enum MultiEmit<Output> {
	Insert { row_number: RowNumber, value: Output },
	Update { row_number: RowNumber, prior: Output, value: Output },
	Remove { row_number: RowNumber, value: Output },
}

#[derive(Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize + Ord, WAcc: Serialize, SK: Serialize + Ord, Output: Serialize",
	deserialize = "C: serde::de::DeserializeOwned + Ord, WAcc: serde::de::DeserializeOwned, \
	               SK: serde::de::DeserializeOwned + Ord, Output: serde::de::DeserializeOwned"
))]
struct GroupState<C, WAcc, SK, Output> {
	buffer: MultiRollingBuffer<C, WAcc>,
	last_emit: MultiRollingEmit<SK, Output>,
}

impl<C: Ord, WAcc, SK: Ord, Output> Default for GroupState<C, WAcc, SK, Output> {
	fn default() -> Self {
		Self {
			buffer: BTreeMap::new(),
			last_emit: BTreeMap::new(),
		}
	}
}

impl<C: Ord + Clone, WAcc: Clone, SK: Ord + Clone, Output: Clone> Clone for GroupState<C, WAcc, SK, Output> {
	fn clone(&self) -> Self {
		Self {
			buffer: self.buffer.clone(),
			last_emit: self.last_emit.clone(),
		}
	}
}

impl<C, WAcc, SK, Output> Debug for GroupState<C, WAcc, SK, Output> {
	fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
		f.debug_struct("GroupState")
			.field("buffer_len", &self.buffer.len())
			.field("last_emit_len", &self.last_emit.len())
			.finish()
	}
}

type MetaLoaded<G, C> = HashMap<G, GroupMeta<C>>;
type StateRows<G> = HashMap<G, RowNumber>;

struct GroupSlot<C, WAcc, SK, Output> {
	state_row_number: RowNumber,
	buffer: MultiRollingBuffer<C, WAcc>,
	prior_emit: MultiRollingEmit<SK, Output>,
	buffer_changed: bool,
}

/// Rolling windows that emit multiple rows per group (top-K), diffing the new
/// emit set against the prior one to produce Insert/Update/Remove per secondary
/// key. Buffer and prior emit are persisted together as one group state.
pub struct MultiRollingEngine<G, C, WAcc, SK, Output> {
	groups: StateCache<RowNumber, GroupState<C, WAcc, SK, Output>>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	_pd: PhantomData<G>,
}

impl<G, C, WAcc, SK, Output> Default for MultiRollingEngine<G, C, WAcc, SK, Output>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	WAcc: WindowAccumulator,
	SK: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	Output: Clone + Debug + PartialEq + Serialize + DeserializeOwned,
	for<'a> &'a G: IntoEncodedKey,
{
	fn default() -> Self {
		Self::new()
	}
}

impl<G, C, WAcc, SK, Output> MultiRollingEngine<G, C, WAcc, SK, Output>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	WAcc: WindowAccumulator,
	SK: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	Output: Clone + Debug + PartialEq + Serialize + DeserializeOwned,
	for<'a> &'a G: IntoEncodedKey,
{
	pub fn new() -> Self {
		Self {
			groups: StateCache::<RowNumber, GroupState<C, WAcc, SK, Output>>::new(8),
			meta: StateCache::<MetaKey, GroupMeta<C>>::new_internal(64),
			_pd: PhantomData,
		}
	}

	/// `state_key` maps a group to its (single) group-state row key; `row_key`
	/// maps a (group, secondary key) to an output row key; `combine` produces the
	/// new emit set from the group's buffer.
	pub fn apply<S, SKF, RKF, CB>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, WAcc::Contribution>,
		capacity: usize,
		state_key: SKF,
		row_key: RKF,
		combine: CB,
	) -> Result<Vec<MultiEmit<Output>>>
	where
		S: WindowStore,
		SKF: Fn(&G) -> EncodedKey,
		RKF: Fn(&G, &SK) -> EncodedKey,
		CB: Fn(&G, &MultiRollingBuffer<C, WAcc>) -> MultiRollingEmit<SK, Output>,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let state_rows = self.resolve_state_rows(store, &buckets, &meta_loaded, &state_key)?;
		let group_slots =
			self.apply_events_into_buffers(store, buckets, &mut meta_loaded, &state_rows, &state_key, capacity)?;
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
		buckets: &RollingBuckets<G, C, WAcc::Contribution>,
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
		buckets: &RollingBuckets<G, C, WAcc::Contribution>,
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
		let state_keys: Vec<RowNumber> = resolved_rows.iter().map(|(rn, _)| *rn).collect();
		for (group, (state_row_number, _)) in resolve_order.into_iter().zip(resolved_rows) {
			state_rows.insert(group, state_row_number);
		}
		self.groups.warm(store, &state_keys)?;
		Ok(state_rows)
	}

	fn apply_events_into_buffers<S, SKF>(
		&mut self,
		store: &mut S,
		buckets: RollingBuckets<G, C, WAcc::Contribution>,
		meta_loaded: &mut MetaLoaded<G, C>,
		state_rows: &StateRows<G>,
		state_key: &SKF,
		capacity: usize,
	) -> Result<BTreeMap<G, GroupSlot<C, WAcc, SK, Output>>>
	where
		S: WindowStore,
		SKF: Fn(&G) -> EncodedKey,
	{
		let mut group_slots: BTreeMap<G, GroupSlot<C, WAcc, SK, Output>> = BTreeMap::new();

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
					} = self.groups.get(store, &state_row_number)?.unwrap_or_default();
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
		group_slots: BTreeMap<G, GroupSlot<C, WAcc, SK, Output>>,
		row_key: &RKF,
		combine: &CB,
	) -> Result<Vec<MultiEmit<Output>>>
	where
		S: WindowStore,
		RKF: Fn(&G, &SK) -> EncodedKey,
		CB: Fn(&G, &MultiRollingBuffer<C, WAcc>) -> MultiRollingEmit<SK, Output>,
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
			self.groups.put(store, &slot.state_row_number, combined)?;
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
