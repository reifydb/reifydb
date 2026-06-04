// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
};

use reifydb_value::reifydb_assertions;
use reifydb_value::{Result, value::row_number::RowNumber};
use serde::{Serialize, de::DeserializeOwned};

use crate::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	window::{
		accumulator::WindowAccumulator,
		engine::{AccumulatorEvent, EmitKind, GroupMeta, LatePolicy, MetaKey, WindowResult, meta_key_for},
		span::{Slot, WindowSpan},
		state::StateCache,
		store::WindowStore,
	},
};

pub type TumblingBuckets<G, C, Contribution> = BTreeMap<(G, WindowSpan<C>), Vec<AccumulatorEvent<Contribution>>>;

type MetaLoaded<G, C> = HashMap<G, GroupMeta<C>>;
type SlotResolved = Vec<Option<(RowNumber, bool)>>;

pub struct TumblingEngine<G, C, Accumulator> {
	accumulators: StateCache<RowNumber, Accumulator>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	late_policy: LatePolicy,
	_pd: PhantomData<G>,
}

impl<G, C, Accumulator> Default for TumblingEngine<G, C, Accumulator>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Accumulator: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
{
	fn default() -> Self {
		Self::new()
	}
}

impl<G, C, Accumulator> TumblingEngine<G, C, Accumulator>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Accumulator: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
{
	pub fn new() -> Self {
		Self::with_late_policy(LatePolicy::Drop)
	}

	pub fn with_late_policy(late_policy: LatePolicy) -> Self {
		Self {
			accumulators: StateCache::<RowNumber, Accumulator>::new(8),
			meta: StateCache::<MetaKey, GroupMeta<C>>::new_internal(64),
			late_policy,
			_pd: PhantomData,
		}
	}

	pub fn apply<S, K, NA>(
		&mut self,
		store: &mut S,
		buckets: TumblingBuckets<G, C, Accumulator::Contribution>,
		row_key: K,
		new_accumulator: NA,
	) -> Result<Vec<WindowResult<G, C, Accumulator::Output>>>
	where
		S: WindowStore,
		K: Fn(&G, C) -> EncodedKey,
		NA: Fn() -> Accumulator,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let slot_resolved = self.resolve_survivor_rows(store, &buckets, &meta_loaded, &row_key)?;
		let results =
			self.apply_events(store, buckets, slot_resolved, &mut meta_loaded, &row_key, &new_accumulator)?;
		self.persist_meta(store, meta_loaded)?;
		Ok(results)
	}

	pub fn flush<S: WindowStore>(&mut self, store: &mut S) -> Result<()> {
		self.accumulators.flush(store)?;
		self.meta.flush(store)?;
		Ok(())
	}

	fn warm_and_load_meta<S: WindowStore>(
		&mut self,
		store: &mut S,
		buckets: &TumblingBuckets<G, C, Accumulator::Contribution>,
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

	fn resolve_survivor_rows<S, K>(
		&mut self,
		store: &mut S,
		buckets: &TumblingBuckets<G, C, Accumulator::Contribution>,
		meta_loaded: &MetaLoaded<G, C>,
		row_key: &K,
	) -> Result<SlotResolved>
	where
		S: WindowStore,
		K: Fn(&G, C) -> EncodedKey,
	{
		let mut survivor_keys: Vec<EncodedKey> = Vec::new();
		let mut slot_survives: Vec<bool> = Vec::with_capacity(buckets.len());
		for (group, span) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.high_water);
			let survives = initial_high_water.is_none_or(|hw| span.start >= hw);
			slot_survives.push(survives);
			if survives {
				survivor_keys.push(row_key(group, span.start));
			}
		}
		let resolved_rows = store.get_or_create_row_numbers(&survivor_keys)?;
		reifydb_assertions! {
			let survivors = survivor_keys.len();
			let resolved = resolved_rows.len();
			assert!(
				resolved == survivors,
				"get_or_create_row_numbers must return exactly one row per survivor key; a short batch would \
				 leave a surviving slot with no resolved row, so the slot_resolved zip below pairs it with None \
				 and apply_events silently re-creates a fresh row instead of reusing the existing window \
				 state, double-counting it (survivor_keys={survivors}, resolved_rows={resolved})"
			);
		}
		let accumulator_keys: Vec<RowNumber> = resolved_rows.iter().map(|(rn, _)| *rn).collect();
		self.accumulators.warm(store, &accumulator_keys)?;
		let mut resolved_rows = resolved_rows.into_iter();
		let slot_resolved: SlotResolved = slot_survives
			.into_iter()
			.map(|survives| {
				if survives {
					resolved_rows.next()
				} else {
					None
				}
			})
			.collect();
		Ok(slot_resolved)
	}

	fn apply_events<S, K, NA>(
		&mut self,
		store: &mut S,
		buckets: TumblingBuckets<G, C, Accumulator::Contribution>,
		slot_resolved: SlotResolved,
		meta_loaded: &mut MetaLoaded<G, C>,
		row_key: &K,
		new_accumulator: &NA,
	) -> Result<Vec<WindowResult<G, C, Accumulator::Output>>>
	where
		S: WindowStore,
		K: Fn(&G, C) -> EncodedKey,
		NA: Fn() -> Accumulator,
	{
		let mut results: Vec<WindowResult<G, C, Accumulator::Output>> = Vec::new();

		for (((group, span), events), slot_pre) in buckets.into_iter().zip(slot_resolved) {
			let entry = meta_loaded.entry(group.clone()).or_default();
			match entry.high_water {
				Some(hw) if span.start < hw => {
					if matches!(self.late_policy, LatePolicy::Drop) {
						continue;
					}
				}
				Some(hw) if span.start > hw => entry.high_water = Some(span.start),
				Some(_) => {}
				None => entry.high_water = Some(span.start),
			}

			let (row_number, is_new) = match slot_pre {
				Some(resolved) => resolved,
				None => {
					let key = row_key(&group, span.start);
					store.get_or_create_row_number(&key)?
				}
			};

			let mut accumulator: Accumulator =
				self.accumulators.get(store, &row_number)?.unwrap_or_else(new_accumulator);
			let was_empty_before = accumulator.is_empty();
			let prior = if was_empty_before {
				None
			} else {
				accumulator.finalize()
			};

			for event in events {
				match event {
					AccumulatorEvent::Add(c) => accumulator.add(&c),
					AccumulatorEvent::Remove(c) => accumulator.remove(&c),
				}
			}

			let value = accumulator.finalize();
			self.accumulators.put(store, &row_number, accumulator)?;

			match value {
				Some(value) => {
					let kind = if is_new || was_empty_before {
						EmitKind::Insert
					} else {
						EmitKind::Update
					};
					results.push(WindowResult {
						row_number,
						group,
						span,
						value,
						prior,
						kind,
					});
				}
				None => {
					if let Some(p) = prior.clone() {
						results.push(WindowResult {
							row_number,
							group,
							span,
							value: p,
							prior,
							kind: EmitKind::Remove,
						});
					}
				}
			}
		}
		Ok(results)
	}

	fn persist_meta<S: WindowStore>(&mut self, store: &mut S, meta_loaded: MetaLoaded<G, C>) -> Result<()> {
		for (group, meta) in meta_loaded {
			self.meta.set(store, &meta_key_for(&group), &meta)?;
		}
		Ok(())
	}
}
