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
	row::operator::state::OperatorState,
};
use reifydb_core::{key::operator_state::GroupId, metrics::heap::HeapSize, state::timer::StateStore};
use reifydb_value::Result;

use crate::{
	operator::state_access::{get_classified, put},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, BatchMeta, EmitKind, GroupMeta, MetaSweep, RunningKey, WindowStateKey,
			config::WindowEngineConfig,
			load_batch_meta, meta_key_for, persist_batch_meta,
			rolling::{RollingBuckets, RollingBuffer, RollingResult},
		},
		span::Slot,
	},
};

type MetaLoaded<G, S> = HashMap<G, BatchMeta<S>>;
type BufferRows<G> = HashMap<G, (GroupId, EncodedKey)>;

struct GroupSlot<S, Accumulator, Running, Output> {
	group_id: GroupId,
	key: EncodedKey,
	buffer: RollingBuffer<S, Accumulator>,
	running: Running,
	buffer_changed: bool,
	prior_output: Option<Output>,
}

pub struct RollingIncrementalEngine<G, S, Accumulator, Running> {
	meta_sweep: MetaSweep,
	_pd: PhantomData<(G, S, Accumulator, Running)>,
}

impl<G, S, Accumulator, Running> RollingIncrementalEngine<G, S, Accumulator, Running>
where
	G: Clone + Eq + Ord + Hash + Debug,
	S: Slot + Hash,
	Accumulator: WindowAccumulator,
	Running: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
	S: HeapSize,
	GroupMeta<S>: OperatorState,
	RollingBuffer<S, Accumulator>: OperatorState + HeapSize,
{
	pub fn new(_config: WindowEngineConfig) -> Self {
		Self {
			meta_sweep: MetaSweep::default(),
			_pd: PhantomData,
		}
	}

	pub fn expire_meta(&mut self, store: &mut dyn StateStore, threshold: u64) -> Result<usize> {
		self.meta_sweep.sweep::<GroupMeta<S>>(store, threshold)
	}

	#[allow(clippy::too_many_arguments)]
	pub fn apply<K, WC, CR, Output>(
		&mut self,
		store: &mut dyn StateStore,
		buckets: RollingBuckets<G, S, Accumulator::Contribution>,
		capacity: usize,
		row_key: K,
		window_contribution: WC,
		combine_running: CR,
	) -> Result<Vec<RollingResult<G, Output>>>
	where
		K: Fn(&G) -> EncodedKey,
		WC: Fn(&Accumulator::Output) -> Running::Contribution,
		CR: Fn(&G, &Running, &Accumulator::Output, S) -> Option<Output>,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let mut meta_loaded = self.load_meta(store, &buckets)?;
		let buffer_rows = self.resolve_buffer_rows(&buckets, &meta_loaded, &row_key)?;

		let mut group_slots: BTreeMap<G, GroupSlot<S, Accumulator, Running, Output>> = BTreeMap::new();

		for ((group, slot), events) in buckets {
			let meta = meta_loaded.entry(group.clone()).or_default();

			let group_slot = match group_slots.get_mut(&group) {
				Some(s) => s,
				None => {
					let (group_id, key) = match buffer_rows.get(&group) {
						Some(resolved) => resolved.clone(),
						None => {
							let key = row_key(&group);
							let group_id = GroupId::of(&key);
							(group_id, key)
						}
					};
					let buffer: RollingBuffer<S, Accumulator> =
						get_classified(store, &WindowStateKey::new(group_id, key.clone()))?
							.unwrap_or_default();
					let running: Running =
						get_classified(store, &RunningKey::new(group_id, key.clone()))?
							.unwrap_or_default();
					let prior_output = match buffer.iter().next_back() {
						Some((slot, accumulator)) => {
							accumulator.finalize().and_then(|newest| {
								combine_running(&group, &running, &newest, *slot)
							})
						}
						None => None,
					};
					group_slots.insert(
						group.clone(),
						GroupSlot {
							group_id,
							key,
							buffer,
							running,
							buffer_changed: false,
							prior_output,
						},
					);
					group_slots.get_mut(&group).expect("just inserted")
				}
			};

			let mut accumulator = group_slot.buffer.remove(&slot).unwrap_or_default();
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
				group_slot.running.remove(&window_contribution(old));
			}
			if let Some(new) = &new_value {
				group_slot.running.add(&window_contribution(new));
			}

			if !accumulator.is_empty() {
				group_slot.buffer.insert(slot, accumulator);
			}
			while group_slot.buffer.len() > capacity {
				if let Some((_, evicted)) = group_slot.buffer.pop_first()
					&& let Some(value) = evicted.finalize()
				{
					group_slot.running.remove(&window_contribution(&value));
				}
			}
			group_slot.buffer_changed = true;

			meta.observe(slot);
		}

		let mut pairs: Vec<(GroupId, EncodedKey)> = Vec::new();
		let mut pending: Vec<(G, Output, bool)> = Vec::new();
		for (group, group_slot) in group_slots {
			if !group_slot.buffer_changed {
				continue;
			}
			let output = match group_slot.buffer.iter().next_back() {
				Some((slot, accumulator)) => accumulator.finalize().and_then(|newest| {
					combine_running(&group, &group_slot.running, &newest, *slot)
				}),
				None => None,
			};
			put(
				store,
				&WindowStateKey::new(group_slot.group_id, group_slot.key.clone()),
				group_slot.buffer,
			)?;
			put(store, &RunningKey::new(group_slot.group_id, group_slot.key.clone()), group_slot.running)?;

			if let Some(out) = output {
				pairs.push((group_slot.group_id, group_slot.key));
				pending.push((group, out, false));
			} else if let Some(prior) = group_slot.prior_output {
				pairs.push((group_slot.group_id, group_slot.key));
				pending.push((group, prior, true));
			}
		}

		let mut results: Vec<RollingResult<G, Output>> = Vec::with_capacity(pending.len());
		if !pairs.is_empty() {
			let rows = store.get_or_create_row_numbers_for_pairs(&pairs)?;
			for (((group, value, withdrawn), (group_id, key)), (row_number, is_new)) in
				pending.into_iter().zip(pairs).zip(rows)
			{
				if withdrawn {
					store.remove_row_number(group_id, &key)?;
					results.push(RollingResult {
						row_number,
						group,
						value,
						prior: None,
						kind: EmitKind::Remove,
					});
				} else {
					let kind = if is_new {
						EmitKind::Insert
					} else {
						EmitKind::Update
					};
					results.push(RollingResult {
						row_number,
						group,
						value,
						prior: None,
						kind,
					});
				}
			}
		}
		self.persist_meta(store, meta_loaded)?;
		Ok(results)
	}

	fn load_meta(
		&mut self,
		store: &mut dyn StateStore,
		buckets: &RollingBuckets<G, S, Accumulator::Contribution>,
	) -> Result<MetaLoaded<G, S>> {
		let mut meta_loaded: MetaLoaded<G, S> = HashMap::new();
		for (group, _) in buckets.keys() {
			if !meta_loaded.contains_key(group) {
				let batch = load_batch_meta(store, &meta_key_for(group))?;
				meta_loaded.insert(group.clone(), batch);
			}
		}
		Ok(meta_loaded)
	}

	fn resolve_buffer_rows<K>(
		&mut self,
		buckets: &RollingBuckets<G, S, Accumulator::Contribution>,
		meta_loaded: &MetaLoaded<G, S>,
		row_key: &K,
	) -> Result<BufferRows<G>>
	where
		K: Fn(&G) -> EncodedKey,
	{
		let mut buffer_rows: BufferRows<G> = HashMap::new();
		let mut resolve_order: Vec<G> = Vec::new();
		let mut group_keys: Vec<EncodedKey> = Vec::new();
		let mut seen: BTreeSet<G> = BTreeSet::new();
		for (group, slot) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.initial);
			if initial_high_water.is_none_or(|hw| *slot >= hw) && seen.insert(group.clone()) {
				resolve_order.push(group.clone());
				group_keys.push(row_key(group));
			}
		}
		for (group, key) in resolve_order.into_iter().zip(group_keys) {
			let group_id = GroupId::of(&key);
			buffer_rows.insert(group, (group_id, key));
		}
		Ok(buffer_rows)
	}

	fn persist_meta(&mut self, store: &mut dyn StateStore, meta_loaded: MetaLoaded<G, S>) -> Result<()> {
		persist_batch_meta(store, meta_loaded)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::key::operator_state::GroupId;
	use reifydb_value::{factory::time::at_millis, value::datetime::DateTime};

	use crate::{
		operator::state::mock::MockStore,
		window::{
			accumulator::{WindowAccumulator, mock::SumAccumulator},
			engine::{
				AccumulatorEvent, EmitKind,
				config::WindowEngineConfig,
				rolling::{RollingBuckets, RollingResult},
				rolling_incremental::RollingIncrementalEngine,
			},
		},
	};

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder().build()
	}

	fn row_key(group: &u32) -> EncodedKey {
		EncodedKey::builder().u32(*group).build()
	}

	fn running_sum(_group: &u32, running: &SumAccumulator, _newest: &i64, _coord: DateTime) -> Option<i64> {
		running.finalize()
	}

	#[test]
	fn buffer_survives_restart_without_running_collision() {
		// `buffers` and `running` are keyed by the same RowNumber, so sharing a keyspace lets
		// `running` (flushed last) clobber the buffer's store group_slot. A live engine hides it by
		// serving both from memory; a restart is one of the two ways a read reaches the store.
		let mut store = MockStore::default();

		let mut engine =
			RollingIncrementalEngine::<u32, DateTime, SumAccumulator, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(5)]);
		let published: Vec<RollingResult<u32, i64>> =
			engine.apply(&mut store, buckets, 4, row_key, |v: &i64| *v, running_sum).unwrap();
		assert_eq!(published.len(), 1);
		assert!(matches!(published[0].kind, EmitKind::Insert));
		assert_eq!(published[0].value, 5);

		// A brand new engine with empty caches, forced to read the persisted buffer and running
		// accumulator back from the store.
		let mut engine =
			RollingIncrementalEngine::<u32, DateTime, SumAccumulator, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Remove(5)]);
		let withdrawn: Vec<RollingResult<u32, i64>> =
			engine.apply(&mut store, buckets, 4, row_key, |v: &i64| *v, running_sum).unwrap();

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
	fn a_group_whose_state_was_reclaimed_updates_its_row_rather_than_inserting_a_second() {
		// The row-number mapping sits in an identity keyspace, so erasing the buffer and the running
		// total leaves the published row addressable and the next event must reach it rather than
		// mint a second one. Insert is decided from the mapping, not from state a data sweep takes.
		let mut store = MockStore::default();
		let mut engine =
			RollingIncrementalEngine::<u32, DateTime, SumAccumulator, SumAccumulator>::new(test_config());

		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(5)]);
		let published: Vec<RollingResult<u32, i64>> =
			engine.apply(&mut store, buckets, 4, row_key, |v: &i64| *v, running_sum).unwrap();
		assert_eq!(published.len(), 1);
		assert!(matches!(published[0].kind, EmitKind::Insert), "precondition: the group publishes once");

		let group_id = GroupId::of(&row_key(&1));
		assert!(store.drop_group_data_entries() > 0, "precondition: the sweep must have erased something");
		assert!(
			store.contains_row_mapping(group_id, &row_key(&1)),
			"precondition: the identity half must survive the data phase"
		);

		let mut engine =
			RollingIncrementalEngine::<u32, DateTime, SumAccumulator, SumAccumulator>::new(test_config());
		let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, at_millis(10)), vec![AccumulatorEvent::Add(3)]);
		let republished: Vec<RollingResult<u32, i64>> =
			engine.apply(&mut store, buckets, 4, row_key, |v: &i64| *v, running_sum).unwrap();

		assert_eq!(republished.len(), 1);
		assert_eq!(
			republished[0].kind,
			EmitKind::Update,
			"the published row survived the sweep, so this is an update and not a second insert"
		);
		assert_eq!(
			republished[0].row_number, published[0].row_number,
			"the woken group keeps the row it published"
		);
	}

	#[test]
	fn buffer_survives_lru_eviction_without_running_collision() {
		// The second way a read reaches the store is LRU eviction, with no restart: the cache holds
		// 8 groups, so tracking more evicts the oldest and the next access re-reads it. Same
		// buffers/running keyspace collision as the restart test, inside one long-lived engine.
		let mut store = MockStore::default();
		let mut engine =
			RollingIncrementalEngine::<u32, DateTime, SumAccumulator, SumAccumulator>::new(test_config());

		let mut published_group_1: Vec<RollingResult<u32, i64>> = Vec::new();
		for group in 1u32..=11u32 {
			let mut buckets: RollingBuckets<u32, DateTime, i64> = BTreeMap::new();
			buckets.insert((group, at_millis(10)), vec![AccumulatorEvent::Add(i64::from(group))]);
			let out: Vec<RollingResult<u32, i64>> =
				engine.apply(&mut store, buckets, 4, row_key, |v: &i64| *v, running_sum).unwrap();
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
			engine.apply(&mut store, buckets, 4, row_key, |v: &i64| *v, running_sum).unwrap();

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
