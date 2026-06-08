// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
};

use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
	encoded::key::{EncodedKey, IntoEncodedKey},
	window::{
		accumulator::WindowAccumulator,
		engine::{AccumulatorEvent, EmitKind, MetaKey, WindowResult, meta_key_for, tumbling::TumblingBuckets},
		span::{Slot, WindowSpan},
		state::StateCache,
		store::WindowStore,
	},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize + Ord, Carry: Serialize",
	deserialize = "C: serde::de::DeserializeOwned + Ord, Carry: serde::de::DeserializeOwned"
))]
struct WindowEntry<C, Carry> {
	row_number: RowNumber,
	span: WindowSpan<C>,
	carry_out: Option<Carry>,
	has_output: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize + Ord, Carry: Serialize",
	deserialize = "C: serde::de::DeserializeOwned + Ord, Carry: serde::de::DeserializeOwned"
))]
struct CarryMeta<C, Carry> {
	high_water: Option<C>,
	windows: BTreeMap<C, WindowEntry<C, Carry>>,
}

impl<C, Carry> Default for CarryMeta<C, Carry> {
	fn default() -> Self {
		Self {
			high_water: None,
			windows: BTreeMap::new(),
		}
	}
}

type MetaLoaded<G, C, Carry> = HashMap<G, CarryMeta<C, Carry>>;
type SlotResolved = Vec<Option<(RowNumber, bool)>>;

pub struct TumblingCarryEngine<G, C, Accumulator, Carry> {
	accumulators: StateCache<RowNumber, Accumulator>,
	meta: StateCache<MetaKey, CarryMeta<C, Carry>>,
	_pd: PhantomData<G>,
}

impl<G, C, Accumulator, Carry> Default for TumblingCarryEngine<G, C, Accumulator, Carry>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Accumulator: WindowAccumulator,
	Carry: Clone + Debug + Serialize + DeserializeOwned,
	for<'a> &'a G: IntoEncodedKey,
{
	fn default() -> Self {
		Self::new()
	}
}

impl<G, C, Accumulator, Carry> TumblingCarryEngine<G, C, Accumulator, Carry>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Accumulator: WindowAccumulator,
	Carry: Clone + Debug + Serialize + DeserializeOwned,
	for<'a> &'a G: IntoEncodedKey,
{
	pub fn new() -> Self {
		Self {
			accumulators: StateCache::<RowNumber, Accumulator>::new(8),
			meta: StateCache::<MetaKey, CarryMeta<C, Carry>>::new_internal(64),
			_pd: PhantomData,
		}
	}

	#[allow(clippy::too_many_arguments)]
	pub fn apply<S, K, NA, BO, CF, Output>(
		&mut self,
		store: &mut S,
		buckets: TumblingBuckets<G, C, Accumulator::Contribution>,
		row_key: K,
		new_accumulator: NA,
		build_output: BO,
		carry_forward: CF,
	) -> Result<Vec<WindowResult<G, C, Output>>>
	where
		S: WindowStore,
		K: Fn(&G, C) -> EncodedKey,
		NA: Fn() -> Accumulator,
		BO: Fn(&G, WindowSpan<C>, &Accumulator::Output, Option<&Carry>) -> Option<Output>,
		CF: Fn(&Accumulator::Output, Option<&Carry>) -> Option<Carry>,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let slot_resolved = self.resolve_survivor_rows(store, &buckets, &meta_loaded, &row_key)?;

		let mut earliest_affected: HashMap<G, C> = HashMap::new();
		for (((group, span), events), slot_pre) in buckets.into_iter().zip(slot_resolved) {
			let entry = meta_loaded.entry(group.clone()).or_default();
			let drop_adds = matches!(entry.high_water, Some(hw) if span.start < hw);

			let row_number = match entry.windows.get(&span.start).map(|w| w.row_number) {
				Some(rn) => rn,
				None => match slot_pre {
					Some((rn, _)) => rn,
					None => continue,
				},
			};

			let mut accumulator: Accumulator =
				self.accumulators.get(store, &row_number)?.unwrap_or_else(&new_accumulator);
			let mut changed = false;
			for event in events {
				match event {
					AccumulatorEvent::Add(c) => {
						if drop_adds {
							continue;
						}
						accumulator.add(&c);
						changed = true;
					}
					AccumulatorEvent::Remove(c) => {
						if accumulator.is_empty() {
							continue;
						}
						accumulator.remove(&c);
						changed = true;
					}
				}
			}
			if !changed {
				continue;
			}
			self.accumulators.put(store, &row_number, accumulator)?;

			entry.windows.entry(span.start).or_insert_with(|| WindowEntry {
				row_number,
				span,
				carry_out: None,
				has_output: false,
			});
			if entry.high_water.is_none_or(|hw| span.start > hw) {
				entry.high_water = Some(span.start);
			}

			let e = earliest_affected.entry(group).or_insert(span.start);
			if span.start < *e {
				*e = span.start;
			}
		}

		let mut results: Vec<WindowResult<G, C, Output>> = Vec::new();
		for (group, start) in earliest_affected {
			let meta = meta_loaded.get_mut(&group).expect("affected group has meta");

			let mut prev_carry: Option<Carry> =
				meta.windows.range(..start).next_back().and_then(|(_, w)| w.carry_out.clone());

			let coords: Vec<C> = meta.windows.range(start..).map(|(c, _)| *c).collect();
			let mut emptied: Vec<C> = Vec::new();
			for coord in coords {
				let (row_number, span, had_output) = {
					let w = meta.windows.get(&coord).expect("window entry present");
					(w.row_number, w.span, w.has_output)
				};
				let value = self.accumulators.get(store, &row_number)?.and_then(|a| a.finalize());
				match value.as_ref().and_then(|v| build_output(&group, span, v, prev_carry.as_ref())) {
					Some(out) => {
						let new_carry = value
							.as_ref()
							.and_then(|v| carry_forward(v, prev_carry.as_ref()));
						let kind = if had_output {
							EmitKind::Update
						} else {
							EmitKind::Insert
						};
						results.push(WindowResult {
							row_number,
							group: group.clone(),
							span,
							value: out,
							prior: None,
							kind,
						});
						let w = meta.windows.get_mut(&coord).expect("window entry present");
						w.carry_out = new_carry.clone();
						w.has_output = true;
						if new_carry.is_some() {
							prev_carry = new_carry;
						}
					}
					None => emptied.push(coord),
				}
			}
			for coord in emptied {
				meta.windows.remove(&coord);
			}
		}

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
	) -> Result<MetaLoaded<G, C, Carry>> {
		let meta_keys: Vec<MetaKey> = buckets
			.keys()
			.map(|(group, _)| group)
			.collect::<BTreeSet<_>>()
			.into_iter()
			.map(meta_key_for)
			.collect();
		self.meta.warm(store, &meta_keys)?;

		let mut meta_loaded: MetaLoaded<G, C, Carry> = HashMap::new();
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
		meta_loaded: &MetaLoaded<G, C, Carry>,
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
				 and apply silently re-creates a fresh row instead of reusing the existing window state, \
				 double-counting it (survivor_keys={survivors}, resolved_rows={resolved})"
			);
		}
		let accumulator_keys: Vec<RowNumber> = resolved_rows.iter().map(|(rn, _)| *rn).collect();
		self.accumulators.warm(store, &accumulator_keys)?;
		let mut resolved_rows = resolved_rows.into_iter();
		Ok(slot_survives
			.into_iter()
			.map(|survives| {
				if survives {
					resolved_rows.next()
				} else {
					None
				}
			})
			.collect())
	}

	fn persist_meta<S: WindowStore>(&mut self, store: &mut S, meta_loaded: MetaLoaded<G, C, Carry>) -> Result<()> {
		for (group, meta) in meta_loaded {
			self.meta.set(store, &meta_key_for(&group), &meta)?;
		}
		Ok(())
	}
}
