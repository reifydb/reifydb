// SPDX-License-Identifier: AGPL-3.0-or-later
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeSet, HashMap},
	fmt::Debug,
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
		engine::{AccumulatorEvent, EmitKind, MetaKey, WindowResult, meta_key_for, tumbling::TumblingBuckets},
		span::{Slot, WindowSpan},
		state::StateCache,
		store::WindowStore,
	},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
	serialize = "C: Serialize, Carry: Serialize",
	deserialize = "C: serde::de::DeserializeOwned, Carry: serde::de::DeserializeOwned"
))]
struct CarryMeta<C, Carry> {
	high_water: Option<C>,
	carry_for_current: Option<Carry>,
	current_window_carry: Option<Carry>,
}

impl<C, Carry> Default for CarryMeta<C, Carry> {
	fn default() -> Self {
		Self {
			high_water: None,
			carry_for_current: None,
			current_window_carry: None,
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

		let mut results: Vec<WindowResult<G, C, Output>> = Vec::new();

		for (((group, span), events), slot_pre) in buckets.into_iter().zip(slot_resolved) {
			let entry = meta_loaded.entry(group.clone()).or_default();
			match entry.high_water {
				Some(hw) if span.start < hw => continue,
				Some(hw) if span.start > hw => {
					entry.carry_for_current = entry.current_window_carry.take();
					entry.high_water = Some(span.start);
				}
				Some(_) => {}
				None => entry.high_water = Some(span.start),
			}
			reifydb_assertions! {
				let recorded = entry.high_water;
				assert!(
					recorded == Some(span.start),
					"tumbling-carry high_water must equal the accepted span start after the advance match (recorded={recorded:?}, span.start={:?}); a lagging high_water would rotate prev_carry against a window that has not actually closed, carrying the wrong prior close into this window",
					span.start
				);
			}
			let prev_carry = entry.carry_for_current.clone();

			let (row_number, is_new) = match slot_pre {
				Some(resolved) => resolved,
				None => {
					let key = row_key(&group, span.start);
					store.get_or_create_row_number(&key)?
				}
			};

			let mut accumulator: Accumulator =
				self.accumulators.get(store, &row_number)?.unwrap_or_else(&new_accumulator);
			let was_empty_before = accumulator.is_empty();

			for event in events {
				match event {
					AccumulatorEvent::Add(c) => accumulator.add(&c),
					AccumulatorEvent::Remove(c) => accumulator.remove(&c),
				}
			}

			let value = accumulator.finalize();
			let output = value.as_ref().and_then(|v| build_output(&group, span, v, prev_carry.as_ref()));

			if output.is_some()
				&& let Some(v) = value.as_ref()
				&& let Some(new_carry) = carry_forward(v, prev_carry.as_ref())
			{
				meta_loaded.entry(group.clone()).or_default().current_window_carry = Some(new_carry);
			}

			self.accumulators.put(store, &row_number, accumulator)?;

			if let Some(out) = output {
				let kind = if is_new || was_empty_before {
					EmitKind::Insert
				} else {
					EmitKind::Update
				};
				results.push(WindowResult {
					row_number,
					group,
					span,
					value: out,
					prior: None,
					kind,
				});
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
