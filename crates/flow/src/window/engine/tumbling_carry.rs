// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, HashMap},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
};

use reifydb_codec::{
	key::encoded::EncodedKey,
	row::{
		operator::state::{OperatorState, StateCodec, decode, decode_body, encode},
		pod::EncodedPodRow,
	},
};
use reifydb_core::{
	key::operator::state::{GroupId, GroupStateKey, IntoGroupStateKey},
	metrics::heap::HeapSize,
	state::timer::StateStore,
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, reifydb_assertions};

use crate::{
	operator::{
		state::seal::rule::is_sealed,
		state_access::{get, get_classified, put, remove},
	},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, KeyspaceFamily, MetaFate, MetaHighWater, MetaSweep, WindowResult,
			WindowStateKey, config::TumblingCarryConfig, group_hash, meta_key_for,
			tumbling::TumblingBuckets,
		},
		span::{SlotSpan, WindowAnchor, WindowSpan},
	},
};

#[operator_state]
#[derive(Debug, Clone)]
pub struct WindowEntry<S, Carry, Output> {
	span: WindowSpan<S>,
	carry_out: Option<Carry>,
	last_output: Option<Output>,
}

impl<S: HeapSize, Carry: HeapSize, Output: HeapSize> HeapSize for WindowEntry<S, Carry, Output> {
	fn heap_size(&self) -> usize {
		self.span.heap_size() + self.carry_out.heap_size() + self.last_output.heap_size()
	}
}

#[operator_state]
#[derive(Debug, Clone)]
pub struct CarryMeta<S, Carry, Output> {
	group: Option<Vec<u8>>,
	high_water: Option<S>,
	sealed_up_to: Option<S>,
	sealed_carry: Option<Carry>,
	windows: BTreeMap<S, WindowEntry<S, Carry, Output>>,
}

impl<S: HeapSize, Carry: HeapSize, Output: HeapSize> HeapSize for CarryMeta<S, Carry, Output> {
	fn heap_size(&self) -> usize {
		self.group.heap_size()
			+ self.high_water.heap_size()
			+ self.sealed_up_to.heap_size()
			+ self.sealed_carry.heap_size()
			+ self.windows.heap_size()
	}
}

impl<S, Carry, Output> Default for CarryMeta<S, Carry, Output> {
	fn default() -> Self {
		Self {
			group: None,
			high_water: None,
			sealed_up_to: None,
			sealed_carry: None,
			windows: BTreeMap::new(),
		}
	}
}

impl<S: WindowAnchor, Carry, Output> MetaHighWater for CarryMeta<S, Carry, Output>
where
	Self: OperatorState,
{
	fn high_water_order(&self) -> Option<u64> {
		self.high_water.map(|hw| hw.order_key().to_order())
	}
}

type MetaLoaded<G, S, Carry, Output> = HashMap<G, CarryMeta<S, Carry, Output>>;
type SlotResolved = Vec<Option<(GroupId, EncodedKey)>>;

struct PendingCarry<S, Output> {
	group_id: GroupId,
	key: EncodedKey,
	span: WindowSpan<S>,
	value: Output,
	withdraw: bool,
}

pub struct TumblingCarryEngine<G, S: WindowAnchor, Accumulator, Carry, Output> {
	family: KeyspaceFamily,
	meta_sweep: MetaSweep,
	retention: Option<SlotSpan<S>>,
	_pd: PhantomData<(G, Accumulator, Carry, Output)>,
}

impl<G, S, Accumulator, Carry, Output> TumblingCarryEngine<G, S, Accumulator, Carry, Output>
where
	G: Clone + Eq + Ord + Hash + Debug,
	S: WindowAnchor + Hash,
	Accumulator: WindowAccumulator,
	Carry: Clone + Debug,
	Output: Clone + Debug,
	G: StateCodec,
	S: HeapSize,
	Carry: HeapSize,
	Output: HeapSize,
	CarryMeta<S, Carry, Output>: OperatorState,
{
	pub fn new(config: TumblingCarryConfig<S>) -> Self {
		Self {
			family: config.base().family(),
			meta_sweep: MetaSweep::default(),
			retention: config.retention(),
			_pd: PhantomData,
		}
	}

	pub fn expire<K>(&mut self, store: &mut dyn StateStore, horizon: S, row_key: K) -> Result<usize>
	where
		K: Fn(&G, S) -> EncodedKey,
	{
		let family = self.family;
		let threshold = horizon.to_order();
		self.meta_sweep.sweep_with(store, threshold, |store, key, bytes| {
			let mut meta = decode::<CarryMeta<S, Carry, Output>>(bytes)?;
			let sealed: Vec<S> =
				meta.windows.keys().copied().take_while(|first| is_sealed(*first, horizon)).collect();
			let folded = !sealed.is_empty();
			if folded {
				let recorded =
					meta.group.clone().expect("a carry meta with live windows records its group");
				let group: G = decode_body(&EncodedPodRow::new(&recorded))?;
				Self::fold_windows(family, store, &mut meta, &group, sealed, &row_key)?;
			}
			let Some(hw) = meta.high_water_order() else {
				return Ok(MetaFate::Ignored);
			};
			if hw < threshold {
				return Ok(MetaFate::Stale);
			}
			let earliest = meta.windows.keys().next().map_or(hw, |first| first.to_order());
			if folded {
				put(store, &key, meta)?;
			}
			Ok(MetaFate::Survives(earliest))
		})
	}

	fn fold_windows<K>(
		family: KeyspaceFamily,
		store: &mut dyn StateStore,
		meta: &mut CarryMeta<S, Carry, Output>,
		group: &G,
		sealed: Vec<S>,
		row_key: &K,
	) -> Result<()>
	where
		K: Fn(&G, S) -> EncodedKey,
	{
		let sealed_keys: Vec<EncodedKey> = sealed.iter().map(|first| row_key(group, *first)).collect();
		for (first, sealed_key) in sealed.into_iter().zip(sealed_keys) {
			let carry_out =
				meta.windows.get(&first).expect("sealed window entry present").carry_out.clone();
			meta.windows.remove(&first);
			meta.sealed_up_to = Some(first);
			meta.sealed_carry = carry_out;
			let sealed_group = GroupId::of(&sealed_key);
			remove(store, &WindowStateKey::new(family, sealed_group, sealed_key.clone()))?;
			store.remove_row_number_for_group(sealed_group)?;
		}
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	pub fn apply<K, NA, BO, CF>(
		&mut self,
		store: &mut dyn StateStore,
		buckets: TumblingBuckets<G, S, Accumulator::Contribution>,
		row_key: K,
		new_accumulator: NA,
		build_output: BO,
		carry_forward: CF,
	) -> Result<Vec<WindowResult<G, S, Output>>>
	where
		K: Fn(&G, S) -> EncodedKey,
		NA: Fn() -> Accumulator,
		BO: Fn(&G, WindowSpan<S>, &Accumulator::Output, Option<&Carry>) -> Option<Output>,
		CF: Fn(&Accumulator::Output, Option<&Carry>) -> Option<Carry>,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let retention = self.retention;
		let mut meta_loaded = self.load_meta(store, &buckets)?;
		let slot_resolved = self.resolve_survivor_rows(&buckets, &meta_loaded, &row_key)?;

		let mut earliest_affected: HashMap<G, S> = HashMap::new();
		for (((group, span), events), slot_pre) in buckets.into_iter().zip(slot_resolved) {
			let entry = meta_loaded.entry(group.clone()).or_default();
			if entry.group.is_none() {
				entry.group = Some(encode(&group)?.body().to_vec());
			}
			if matches!(entry.sealed_up_to, Some(s) if span.start <= s) {
				continue;
			}
			let slot_key = row_key(&group, span.start);
			let group_id = match &slot_pre {
				Some((gid, _)) => *gid,
				None => GroupId::of(&slot_key),
			};
			if !entry.windows.contains_key(&span.start) && slot_pre.is_none() {
				continue;
			}

			let mut accumulator: Accumulator =
				get_classified(store, &WindowStateKey::new(self.family, group_id, slot_key.clone()))?
					.unwrap_or_else(&new_accumulator);
			let mut changed = false;
			for event in events {
				match event {
					AccumulatorEvent::Add(c) => {
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
			put(store, &WindowStateKey::new(self.family, group_id, slot_key), accumulator)?;

			entry.windows.entry(span.start).or_insert_with(|| WindowEntry {
				span,
				carry_out: None,
				last_output: None,
			});
			if entry.high_water.is_none_or(|hw| span.start > hw) {
				entry.high_water = Some(span.start);
			}

			let e = earliest_affected.entry(group).or_insert(span.start);
			if span.start < *e {
				*e = span.start;
			}
		}

		let mut results: Vec<WindowResult<G, S, Output>> = Vec::new();
		for (group, start) in earliest_affected {
			let meta = meta_loaded.get_mut(&group).expect("affected group has meta");

			let mut prev_carry: Option<Carry> = match meta.windows.range(..start).next_back() {
				Some((_, w)) => w.carry_out.clone(),
				None => meta.sealed_carry.clone(),
			};

			let slots: Vec<S> = meta.windows.range(start..).map(|(c, _)| *c).collect();
			let slot_keys: Vec<EncodedKey> = slots.iter().map(|slot| row_key(&group, *slot)).collect();
			let mut emptied: Vec<S> = Vec::new();
			let mut pending: Vec<PendingCarry<S, Output>> = Vec::new();
			for (slot, slot_key) in slots.into_iter().zip(slot_keys) {
				let span = meta.windows.get(&slot).expect("window entry present").span;
				let slot_group = GroupId::of(&slot_key);
				let finalized = get::<_, Accumulator>(
					store,
					&WindowStateKey::new(self.family, slot_group, slot_key.clone()),
				)?
				.and_then(|a| a.finalize())
				.map(|value| (slot_group, value));
				let emitted = finalized.as_ref().and_then(|(slot_group, value)| {
					build_output(&group, span, value, prev_carry.as_ref())
						.map(|out| (*slot_group, value, out))
				});
				match emitted {
					Some((slot_group, value, out)) => {
						let new_carry = carry_forward(value, prev_carry.as_ref());
						let w = meta.windows.get_mut(&slot).expect("window entry present");
						w.carry_out = new_carry.clone();
						w.last_output = Some(out.clone());
						if new_carry.is_some() {
							prev_carry = new_carry;
						}
						pending.push(PendingCarry {
							group_id: slot_group,
							key: slot_key,
							span,
							value: out,
							withdraw: false,
						});
					}
					None => {
						if let Some(prev) =
							meta.windows.get(&slot).and_then(|w| w.last_output.clone())
						{
							pending.push(PendingCarry {
								group_id: slot_group,
								key: slot_key,
								span,
								value: prev,
								withdraw: true,
							});
						}
						emptied.push(slot);
					}
				}
			}

			let pairs: Vec<(GroupId, EncodedKey)> =
				pending.iter().map(|p| (p.group_id, p.key.clone())).collect();
			let rows = store.get_or_create_row_numbers_for_groups(
				&pairs.iter().map(|(group, _)| *group).collect::<Vec<_>>(),
			)?;
			reifydb_assertions! {
				let requested = pairs.len();
				let returned = rows.len();
				assert!(
					returned == requested,
					"the identity batch must return one row per publishing window; a short batch makes \
					 the zip below drop the tail, so those windows publish nothing while their carry \
					 meta already advanced (requested={requested}, returned={returned})"
				);
			}
			for (emit, (row_number, is_new)) in pending.into_iter().zip(rows) {
				let kind = if emit.withdraw {
					store.remove_row_number_for_group(emit.group_id)?;
					EmitKind::Remove
				} else if is_new {
					EmitKind::Insert
				} else {
					EmitKind::Update
				};
				results.push(WindowResult {
					row_number,
					group: group.clone(),
					span: emit.span,
					value: emit.value,
					prior: None,
					kind,
				});
			}

			for slot in emptied {
				meta.windows.remove(&slot);
			}

			if let (Some(retention), Some(hw)) = (retention, meta.high_water) {
				let to_seal: Vec<S> = meta
					.windows
					.keys()
					.copied()
					.take_while(|first| hw.span_since(*first) > retention)
					.collect();
				Self::fold_windows(self.family, store, meta, &group, to_seal, &row_key)?;
			}
		}

		self.persist_meta(store, meta_loaded)?;
		Ok(results)
	}

	fn load_meta(
		&mut self,
		store: &mut dyn StateStore,
		buckets: &TumblingBuckets<G, S, Accumulator::Contribution>,
	) -> Result<MetaLoaded<G, S, Carry, Output>> {
		let mut meta_loaded: MetaLoaded<G, S, Carry, Output> = HashMap::new();
		let mut by_key: HashMap<GroupStateKey, G> = HashMap::new();
		for (group, _) in buckets.keys() {
			if meta_loaded.contains_key(group) {
				continue;
			}
			meta_loaded.insert(group.clone(), CarryMeta::default());
			by_key.insert((&meta_key_for(group_hash(group)?)).into_group_state_key(), group.clone());
		}
		let keys: Vec<GroupStateKey> = by_key.keys().cloned().collect();
		store.state_get_many_visit(&keys, &mut |key, bytes| {
			if let Some(group) = by_key.get(&key) {
				meta_loaded.insert(group.clone(), decode::<CarryMeta<S, Carry, Output>>(&bytes)?);
			}
			Ok(())
		})?;
		Ok(meta_loaded)
	}

	fn resolve_survivor_rows<K>(
		&mut self,
		buckets: &TumblingBuckets<G, S, Accumulator::Contribution>,
		meta_loaded: &MetaLoaded<G, S, Carry, Output>,
		row_key: &K,
	) -> Result<SlotResolved>
	where
		K: Fn(&G, S) -> EncodedKey,
	{
		let mut survivor_keys: Vec<EncodedKey> = Vec::new();
		let mut slot_survives: Vec<bool> = Vec::with_capacity(buckets.len());
		for (group, span) in buckets.keys() {
			let meta = meta_loaded.get(group);
			let sealed = matches!(meta.and_then(|m| m.sealed_up_to), Some(s) if span.start <= s);
			let survives = !sealed;
			slot_survives.push(survives);
			if survives {
				survivor_keys.push(row_key(group, span.start));
			}
		}
		let mut resolved_rows = survivor_keys.into_iter().map(|key| (GroupId::of(&key), key));
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

	fn persist_meta(
		&mut self,
		store: &mut dyn StateStore,
		meta_loaded: MetaLoaded<G, S, Carry, Output>,
	) -> Result<()> {
		for (group, meta) in meta_loaded {
			put(store, &meta_key_for(group_hash(&group)?), meta)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::{collections::HashMap, ops::Bound};

	use reifydb_codec::{
		key::encoded::EncodedKeyRange,
		row::{operator::state::decode, pod::EncodedPodRow},
	};
	use reifydb_core::{
		key::operator::state::{GroupStateKey, KeyspaceId, OperatorStateKey},
		state::timer::{TimerKind, TimerStore},
	};
	use reifydb_value::{
		factory::time::{at_millis, millis},
		value::{datetime::DateTime, duration::Duration, row_number::RowNumber},
	};

	use super::*;
	use crate::{
		operator::state::seal::coord::Coord,
		window::{
			accumulator::invertible::retained_map::RetainedAccumulator, engine::config::WindowEngineConfig,
		},
	};

	// Allocates a distinct row number per key; the state.rs mock collapses every key onto row 1,
	// which would alias all window accumulators and defeat a storage-bound test.
	#[derive(Default)]
	struct CountingStore {
		data: HashMap<Vec<u8>, EncodedPodRow>,
		rows: HashMap<(GroupId, Vec<u8>), RowNumber>,
		next_row: u64,
	}

	impl CountingStore {
		fn keyspace_count(&self, keyspace: KeyspaceId) -> usize {
			self.data
				.keys()
				.filter(|k| {
					OperatorStateKey::decode_inner(k).is_some_and(|(_, found, _)| found == keyspace)
				})
				.count()
		}

		fn accumulator_count(&self) -> usize {
			// Meta and the expiry index share the store, so count only the accumulator keyspace.
			self.keyspace_count(KeyspaceId::ACCUMULATOR)
		}

		fn meta_entry_count(&self) -> usize {
			self.keyspace_count(KeyspaceId::WINDOW_META)
		}

		fn row_mapping_count(&self) -> usize {
			// One mapping is minted per (group, window), so this is what proves a sealed window
			// reclaims its own.
			self.rows.len()
		}

		fn drop_group_data_entries(&mut self) -> usize {
			// Phase-1 reclamation clears every data keyspace inside a real group but leaves the root group
			// alone; row-number mappings live outside `data` and survive it the same way production does.
			let keys: Vec<Vec<u8>> = self
				.data
				.keys()
				.filter(|k| {
					OperatorStateKey::decode_inner(k)
						.is_some_and(|(group, found, _)| !group.is_root() && found.is_data())
				})
				.cloned()
				.collect();
			for key in &keys {
				self.data.remove(key);
			}
			keys.len()
		}
	}

	impl TimerStore for CountingStore {
		fn arm_timer(&mut self, _due: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			unreachable!("the window engine never arms timers; only the shell above it does")
		}

		fn disarm_timer(&mut self, _due: DateTime, _kind: TimerKind, _key: &EncodedKey) -> Result<()> {
			unreachable!("the window engine never disarms timers; only the shell above it does")
		}

		fn flow_watermark(&mut self) -> Result<Option<DateTime>> {
			Ok(None)
		}
	}

	impl CountingStore {
		fn row_number_for(&mut self, group: GroupId, key: &EncodedKey) -> (RowNumber, bool) {
			let slot_key = (group, key.as_bytes().to_vec());
			if let Some(rn) = self.rows.get(&slot_key) {
				return (*rn, false);
			}
			self.next_row += 1;
			let rn = RowNumber(self.next_row);
			self.rows.insert(slot_key, rn);
			(rn, true)
		}
	}

	impl StateStore for CountingStore {
		fn state_get(&mut self, key: &GroupStateKey) -> Result<Option<EncodedPodRow>> {
			Ok(self.data.get(key.as_slice()).cloned())
		}
		fn state_get_many_visit(
			&mut self,
			keys: &[GroupStateKey],
			visit: &mut dyn FnMut(GroupStateKey, EncodedPodRow) -> Result<()>,
		) -> Result<()> {
			for key in keys {
				if let Some(b) = self.data.get(key.as_slice()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}
		fn state_set(&mut self, key: &GroupStateKey, payload: EncodedPodRow) -> Result<()> {
			self.data.insert(key.as_slice().to_vec(), payload);
			Ok(())
		}
		fn state_remove(&mut self, key: &GroupStateKey) -> Result<()> {
			self.data.remove(key.as_slice());
			Ok(())
		}
		fn state_page_inner(
			&mut self,
			range: EncodedKeyRange,
			limit: Option<usize>,
		) -> Result<Vec<(GroupStateKey, EncodedPodRow)>> {
			let after_start = |k: &[u8]| match &range.start {
				Bound::Included(s) => k >= s.as_bytes(),
				Bound::Excluded(s) => k > s.as_bytes(),
				Bound::Unbounded => true,
			};
			let before_end = |k: &[u8]| match &range.end {
				Bound::Included(e) => k <= e.as_bytes(),
				Bound::Excluded(e) => k < e.as_bytes(),
				Bound::Unbounded => true,
			};
			let mut matched: Vec<(Vec<u8>, EncodedPodRow)> = self
				.data
				.iter()
				.filter(|(k, _)| after_start(k) && before_end(k))
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect();
			matched.sort_by(|a, b| a.0.cmp(&b.0));
			if let Some(limit) = limit {
				matched.truncate(limit);
			}
			Ok(matched
				.into_iter()
				.map(|(k, b)| {
					let k = GroupStateKey::from_framed(EncodedKey::new(k))
						.expect("fake store holds an unframed state key");
					(k, b)
				})
				.collect())
		}
		fn get_or_create_row_numbers(
			&mut self,
			group: GroupId,
			keys: &[EncodedKey],
		) -> Result<Vec<(RowNumber, bool)>> {
			Ok(keys.iter().map(|key| self.row_number_for(group, key)).collect())
		}
		fn get_or_create_row_numbers_for_groups(
			&mut self,
			groups: &[GroupId],
		) -> Result<Vec<(RowNumber, bool)>> {
			Ok(groups
				.iter()
				.map(|group| self.row_number_for(*group, &EncodedKey::new(Vec::new())))
				.collect())
		}
		fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
			self.rows.remove(&(group, key.as_bytes().to_vec()));
			Ok(())
		}
		fn remove_row_number_for_group(&mut self, group: GroupId) -> Result<()> {
			self.rows.remove(&(group, Vec::new()));
			Ok(())
		}
		fn written_at(&self) -> DateTime {
			DateTime::EPOCH
		}
	}

	type Engine = TumblingCarryEngine<String, DateTime, RetainedAccumulator<u64, f64>, f64, f64>;

	const WINDOW: u64 = 60;

	fn order(millis: u64) -> u64 {
		at_millis(millis).to_order()
	}

	fn carry_config(retention: Option<Duration>) -> TumblingCarryConfig<DateTime> {
		TumblingCarryConfig::builder(WindowEngineConfig::builder().build()).retention(retention).build()
	}

	fn feed(engine: &mut Engine, store: &mut CountingStore, ws: DateTime, price: f64) {
		// One event per batch, so the high-water mark advances one window per call.
		let _ = feed_group(engine, store, "BTC", ws, price);
	}

	// Distinct groups get distinct accumulator rows, which is what the eviction test needs to
	// overflow the 8-slot accumulator cache.
	fn feed_group(
		engine: &mut Engine,
		store: &mut CountingStore,
		group: &str,
		ws: DateTime,
		price: f64,
	) -> Vec<WindowResult<String, DateTime, f64>> {
		let mut buckets: TumblingBuckets<String, DateTime, (u64, f64)> = BTreeMap::new();
		let span = WindowSpan::for_coord(ws, millis(WINDOW));
		buckets.insert((group.to_string(), span), vec![AccumulatorEvent::Add((ws.to_order(), price))]);
		engine.apply(
			store,
			buckets,
			|g: &String, w: DateTime| EncodedKey::builder().str(g).u64(w.to_order()).build(),
			RetainedAccumulator::<u64, f64>::default,
			|_g: &String, _s: WindowSpan<DateTime>, v: &BTreeMap<u64, f64>, _p: Option<&f64>| {
				(!v.is_empty()).then(|| v.values().sum::<f64>())
			},
			|v: &BTreeMap<u64, f64>, _p: Option<&f64>| v.last_key_value().map(|(_, val)| *val),
		)
		.expect("apply")
	}

	#[test]
	fn retention_seals_old_windows_and_reclaims_accumulator_rows() {
		// With a 2-window retention horizon, older windows must seal to the O(1) carry scalar and
		// drop their accumulator row, so the live row count stays bounded by the horizon rather
		// than growing with the number of windows seen.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(Some(millis(2 * WINDOW))));
		for i in 0..60u64 {
			feed(&mut engine, &mut store, at_millis(i * WINDOW), i as f64);
		}
		assert!(
			store.accumulator_count() <= 4,
			"sealed windows must reclaim their accumulator rows; found {} live rows after 60 windows",
			store.accumulator_count()
		);
	}

	#[test]
	fn retention_seals_old_windows_and_reclaims_row_number_mappings() {
		// The per-(group, window) mapping is keyed by row_key, not row_number, so accumulator
		// eviction does not reclaim it. Sealing past retention must drop it alongside the
		// accumulator or the mapping keyspace grows per window forever.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(Some(millis(2 * WINDOW))));
		for i in 0..60u64 {
			feed(&mut engine, &mut store, at_millis(i * WINDOW), i as f64);
		}
		assert!(
			store.row_mapping_count() <= 4,
			"sealed windows must reclaim their row-number mappings; found {} live mappings after 60 windows",
			store.row_mapping_count()
		);
	}

	#[test]
	fn a_window_whose_state_was_reclaimed_updates_its_row_rather_than_inserting_a_second() {
		// The data phase takes the accumulator, the carry meta and last_output at once while the
		// mapping stays addressable, so what comes back has no memory of what it published but the
		// sink still holds that row. A second insert would lay a duplicate row over a live one.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(None));
		let published = feed_group(&mut engine, &mut store, "BTC", at_millis(0), 5.0);
		assert_eq!(published.len(), 1);
		assert!(matches!(published[0].kind, EmitKind::Insert), "precondition: the window publishes once");

		assert!(store.drop_group_data_entries() > 0, "precondition: the sweep must have erased something");
		assert_eq!(store.row_mapping_count(), 1, "precondition: the identity half must survive the data phase");

		let mut engine = Engine::new(carry_config(None));
		let republished = feed_group(&mut engine, &mut store, "BTC", at_millis(0), 3.0);

		assert_eq!(republished.len(), 1);
		assert_eq!(
			republished[0].kind,
			EmitKind::Update,
			"the published row survived the sweep, so this is an update and not a second insert"
		);
		assert_eq!(
			republished[0].row_number, published[0].row_number,
			"the woken window keeps the row it published"
		);
	}

	#[test]
	fn every_successive_window_emits_its_own_result() {
		// Production symptom: a `apply twap { window_duration: '1m' }` ladder over an advancing
		// event-time stream published output for the first window only and then froze, while its
		// source view kept advancing. Every existing test here feeds many windows but only asserts
		// on reclamation counts, so a driver that stops emitting after the first window passes them
		// all. Each successive window carries its own events, so each must publish its own row.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(None));
		let mut emitted_windows = Vec::new();
		for i in 0..5u64 {
			let out = feed_group(&mut engine, &mut store, "BTC", at_millis(i * WINDOW), i as f64 + 1.0);
			println!(
				"[win-probe] fed window_start={} results={} kinds={:?}",
				i * WINDOW,
				out.len(),
				out.iter().map(|r| (r.span.start, r.kind)).collect::<Vec<_>>()
			);
			if !out.is_empty() {
				emitted_windows.push(i * WINDOW);
			}
		}
		assert_eq!(
			emitted_windows,
			vec![0, WINDOW, 2 * WINDOW, 3 * WINDOW, 4 * WINDOW],
			"each window that received an event must publish; a ladder that stops after the first \
			 window is the production freeze"
		);
	}

	fn seal_key(group: &String, window: DateTime) -> EncodedKey {
		EncodedKey::builder().str(group).u64(window.to_order()).build()
	}

	fn feed_carrying(
		engine: &mut Engine,
		store: &mut CountingStore,
		window_start: DateTime,
		ts: u64,
		price: f64,
	) -> Vec<WindowResult<String, DateTime, f64>> {
		let mut buckets: TumblingBuckets<String, DateTime, (u64, f64)> = BTreeMap::new();
		let span = WindowSpan::for_coord(window_start, millis(WINDOW));
		buckets.insert(("BTC".to_string(), span), vec![AccumulatorEvent::Add((ts, price))]);
		engine.apply(
			store,
			buckets,
			seal_key,
			RetainedAccumulator::<u64, f64>::default,
			|_g: &String, _s: WindowSpan<DateTime>, v: &BTreeMap<u64, f64>, p: Option<&f64>| {
				Some(v.values().sum::<f64>() + p.copied().unwrap_or(1000.0))
			},
			|v: &BTreeMap<u64, f64>, _p: Option<&f64>| v.last_key_value().map(|(_, val)| *val),
		)
		.expect("apply")
	}

	#[test]
	fn a_seal_folds_every_window_below_the_horizon_of_every_group_and_frees_its_rows() {
		// Without retention only the seal can free a window; every group's windows below the horizon must
		// lose both the accumulator row and the row-number mapping, and later windows must be left alone.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(None));
		for i in 0..10u64 {
			feed_group(&mut engine, &mut store, "BTC", at_millis(i * WINDOW), i as f64);
			feed_group(&mut engine, &mut store, "ETH", at_millis(i * WINDOW), i as f64);
		}
		assert_eq!((store.accumulator_count(), store.row_mapping_count()), (20, 20));

		let dropped = engine.expire(&mut store, at_millis(5 * WINDOW), seal_key).unwrap();

		assert_eq!(dropped, 0, "both groups still have windows at or past the horizon");
		assert_eq!(store.accumulator_count(), 10, "windows 5..9 of each group must keep their accumulators");
		assert_eq!(store.row_mapping_count(), 10, "a folded window must give back its row-number mapping");
		assert_eq!(store.meta_entry_count(), 2, "a group with live windows keeps its meta");
	}

	#[test]
	fn a_seal_keeps_the_window_that_starts_exactly_at_the_horizon() {
		// A window is sealed only when it starts strictly below the horizon, the same test the driver applies
		// to incoming rows; folding the boundary window would drop a window that still takes rows.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(None));
		for i in 0..3u64 {
			feed(&mut engine, &mut store, at_millis(i * WINDOW), i as f64);
		}

		engine.expire(&mut store, at_millis(WINDOW), seal_key).unwrap();

		assert_eq!(store.accumulator_count(), 2, "only the window below the horizon folds");
	}

	#[test]
	fn a_seal_frees_the_windows_of_a_dead_group_before_it_drops_the_meta() {
		// Dropping the meta first would orphan every accumulator row the group still holds, with nothing left
		// that can find them.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(None));
		for i in 0..10u64 {
			feed(&mut engine, &mut store, at_millis(i * WINDOW), i as f64);
		}

		let dropped = engine.expire(&mut store, at_millis(100 * WINDOW), seal_key).unwrap();

		assert_eq!(dropped, 1);
		assert_eq!(store.accumulator_count(), 0, "a dead group must not leave accumulator rows behind");
		assert_eq!(store.row_mapping_count(), 0, "a dead group must not leave row-number mappings behind");
		assert_eq!(store.meta_entry_count(), 0);
	}

	#[test]
	fn a_window_after_the_seal_still_carries_in_from_the_folded_windows() {
		// The carry a folded window produced is what its successor starts from; a fold that lost it would make
		// the first live window compute as if the group had just begun.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(None));
		for i in 0..10u64 {
			feed_carrying(&mut engine, &mut store, at_millis(i * WINDOW), i * WINDOW, i as f64);
		}
		engine.expire(&mut store, at_millis(9 * WINDOW), seal_key).unwrap();

		let results = feed_carrying(&mut engine, &mut store, at_millis(9 * WINDOW), 9 * WINDOW + 1, 100.0);

		let window_9 =
			results.iter().find(|r| r.span.start == at_millis(9 * WINDOW)).expect("window 9 recomputed");
		assert_eq!(window_9.value, 109.0 + 8.0, "window 9 must carry in window 8's close, not restart");
	}

	#[test]
	fn a_window_folded_by_a_seal_takes_no_further_events() {
		// Without the fold persisted, a late event recreates the folded window's rows and nothing frees them.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(None));
		for i in 0..10u64 {
			feed_carrying(&mut engine, &mut store, at_millis(i * WINDOW), i * WINDOW, i as f64);
		}
		engine.expire(&mut store, at_millis(5 * WINDOW), seal_key).unwrap();
		let before = (store.accumulator_count(), store.row_mapping_count());

		let results = feed_carrying(&mut engine, &mut store, at_millis(2 * WINDOW), 2 * WINDOW + 1, 100.0);

		assert!(results.is_empty(), "a folded window must not publish again");
		assert_eq!(
			(store.accumulator_count(), store.row_mapping_count()),
			before,
			"a late event for a folded window must not recreate its rows"
		);
	}

	#[test]
	fn successive_seals_keep_folding_as_the_horizon_rises() {
		// A pass that found nothing more to fold must not stop later passes: the horizon only moves up, and
		// each move seals more of the same group's windows.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(None));
		for i in 0..10u64 {
			feed(&mut engine, &mut store, at_millis(i * WINDOW), i as f64);
		}

		engine.expire(&mut store, at_millis(3 * WINDOW), seal_key).unwrap();
		assert_eq!(store.accumulator_count(), 7);
		engine.expire(&mut store, at_millis(3 * WINDOW), seal_key).unwrap();
		assert_eq!(store.accumulator_count(), 7, "a repeat at the same horizon changes nothing");
		engine.expire(&mut store, at_millis(6 * WINDOW), seal_key).unwrap();

		assert_eq!(store.accumulator_count(), 4, "a higher horizon must fold the newly sealed windows");
	}

	#[test]
	fn without_retention_every_window_accumulator_is_retained() {
		// The contrast that proves the bound above comes from sealing and not some other cap: with
		// no retention configured the engine keeps every window's accumulator forever.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(None));
		for i in 0..60u64 {
			feed(&mut engine, &mut store, at_millis(i * WINDOW), i as f64);
		}
		assert_eq!(
			store.accumulator_count(),
			60,
			"with no retention the carry engine retains every window's accumulator row"
		);
	}

	#[test]
	fn terminal_remove_after_restart_uses_persisted_last_output() {
		// A carry window's withdrawn value cannot be recomputed from its own surviving state: an
		// emptied accumulator finalizes to nothing, and the output also depended on the value
		// carried in from earlier windows. `last_output` must therefore be durable, not ephemeral.
		let mut store = CountingStore::default();

		let mut engine = Engine::new(carry_config(None));
		feed(&mut engine, &mut store, at_millis(0), 5.0);

		let mut engine = Engine::new(carry_config(None));
		let span = WindowSpan::for_coord(at_millis(0), millis(WINDOW));
		let mut buckets: TumblingBuckets<String, DateTime, (u64, f64)> = BTreeMap::new();
		buckets.insert(("BTC".to_string(), span), vec![AccumulatorEvent::Remove((0, 5.0))]);
		let withdrawn: Vec<WindowResult<String, DateTime, f64>> = engine
			.apply(
				&mut store,
				buckets,
				|g: &String, w: DateTime| EncodedKey::builder().str(g).u64(w.to_order()).build(),
				RetainedAccumulator::<u64, f64>::default,
				|_g: &String, _s: WindowSpan<DateTime>, v: &BTreeMap<u64, f64>, _p: Option<&f64>| {
					(!v.is_empty()).then(|| v.values().sum::<f64>())
				},
				|v: &BTreeMap<u64, f64>, _p: Option<&f64>| v.last_key_value().map(|(_, val)| *val),
			)
			.expect("apply");

		assert_eq!(withdrawn.len(), 1, "emptying the window emits exactly one terminal diff");
		assert!(
			matches!(withdrawn[0].kind, EmitKind::Remove),
			"the window emptied under retraction, so the last published row must be withdrawn"
		);
		assert_eq!(
			withdrawn[0].value, 5.0,
			"the withdrawn value is the persisted last_output, recovered across the restart"
		);
	}

	#[test]
	fn last_output_survives_lru_eviction() {
		// The other way the persisted state is read back is LRU eviction, with no restart: the
		// accumulator cache holds 8 windows, so tracking more evicts the oldest and the next access
		// re-reads it from the store.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(None));

		let mut published_g00: Vec<WindowResult<String, DateTime, f64>> = Vec::new();
		for i in 0..11u64 {
			let group = format!("G{i:02}");
			let out = feed_group(&mut engine, &mut store, &group, at_millis(0), (i + 1) as f64);
			if i == 0 {
				published_g00 = out;
			}
		}
		assert_eq!(published_g00.len(), 1);
		assert!(matches!(published_g00[0].kind, EmitKind::Insert));
		assert_eq!(published_g00[0].value, 1.0);

		// G00's window was pushed out of the 8-slot accumulator cache by the later groups, so the
		// engine must re-read its accumulator from the store to apply this retraction.
		let span = WindowSpan::for_coord(at_millis(0), millis(WINDOW));
		let mut buckets: TumblingBuckets<String, DateTime, (u64, f64)> = BTreeMap::new();
		buckets.insert(("G00".to_string(), span), vec![AccumulatorEvent::Remove((0, 1.0))]);
		let withdrawn: Vec<WindowResult<String, DateTime, f64>> = engine
			.apply(
				&mut store,
				buckets,
				|g: &String, w: DateTime| EncodedKey::builder().str(g).u64(w.to_order()).build(),
				RetainedAccumulator::<u64, f64>::default,
				|_g: &String, _s: WindowSpan<DateTime>, v: &BTreeMap<u64, f64>, _p: Option<&f64>| {
					(!v.is_empty()).then(|| v.values().sum::<f64>())
				},
				|v: &BTreeMap<u64, f64>, _p: Option<&f64>| v.last_key_value().map(|(_, val)| *val),
			)
			.expect("apply");

		assert_eq!(withdrawn.len(), 1, "emptying the evicted window emits exactly one terminal diff");
		assert!(
			matches!(withdrawn[0].kind, EmitKind::Remove),
			"the evicted window emptied under retraction, so the last published row must be withdrawn"
		);
		assert_eq!(
			withdrawn[0].value, 1.0,
			"the withdrawn value is the persisted last_output for G00, recovered after eviction"
		);
		assert_eq!(
			withdrawn[0].row_number, published_g00[0].row_number,
			"the withdrawal targets the same row that was published for G00"
		);
	}

	#[test]
	fn carry_meta_projects_its_high_water_independently_of_its_window_map() {
		// The meta sweep reclaims on this projection alone, so window entries must never skew it.
		let mut meta: CarryMeta<DateTime, i64, i64> = CarryMeta::default();
		let empty_bytes = meta.encode_state().unwrap();
		assert_eq!(
			decode::<CarryMeta<DateTime, i64, i64>>(&empty_bytes).unwrap().high_water_order(),
			None,
			"a default CarryMeta has no high water"
		);

		meta.high_water = Some(at_millis(99));
		meta.windows.insert(
			at_millis(10),
			WindowEntry {
				span: WindowSpan::new(at_millis(10), at_millis(20)),
				carry_out: Some(7i64),
				last_output: Some(3i64),
			},
		);
		let bytes = meta.encode_state().unwrap();
		let projected = decode::<CarryMeta<DateTime, i64, i64>>(&bytes).unwrap().high_water_order();
		assert_eq!(projected, Some(order(99)), "the populated window map must not disturb the high water");
	}
}
