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
use reifydb_macro::operator_state;
use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};
use rkyv::{Archive, with::AsVec};

use crate::{
	key::operator_state::GroupId,
	metrics::heap::{HeapSize, StateCompleteness, StateMemory},
	state::{
		cache::{StateCache, StateView},
		store::StateStore,
	},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, EmitKind, MetaHighWater, MetaKey, WindowResult, WindowStateKey,
			accumulator_range, config::TumblingCarryConfig, decode_meta_key, decode_window_state_key,
			meta_key_for, meta_range, sweep_stale_meta, tumbling::TumblingBuckets,
		},
		span::{Slot, WindowSpan},
	},
};

#[operator_state]
#[derive(Debug, Clone)]
pub struct WindowEntry<C, Carry, Output> {
	row_number: RowNumber,
	span: WindowSpan<C>,
	carry_out: Option<Carry>,
	has_output: bool,
	last_output: Option<Output>,
}

impl<C: HeapSize, Carry: HeapSize, Output: HeapSize> HeapSize for WindowEntry<C, Carry, Output> {
	fn heap_size(&self) -> usize {
		self.span.heap_size() + self.carry_out.heap_size() + self.last_output.heap_size()
	}
}

#[operator_state]
#[derive(Debug, Clone)]
pub struct CarryMeta<C, Carry, Output> {
	high_water: Option<C>,
	sealed_up_to: Option<C>,
	sealed_carry: Option<Carry>,
	#[rkyv(with = AsVec)]
	windows: BTreeMap<C, WindowEntry<C, Carry, Output>>,
}

impl<C: HeapSize, Carry: HeapSize, Output: HeapSize> HeapSize for CarryMeta<C, Carry, Output> {
	fn heap_size(&self) -> usize {
		self.high_water.heap_size()
			+ self.sealed_up_to.heap_size()
			+ self.sealed_carry.heap_size()
			+ self.windows.heap_size()
	}
}

impl<C, Carry, Output> Default for CarryMeta<C, Carry, Output> {
	fn default() -> Self {
		Self {
			high_water: None,
			sealed_up_to: None,
			sealed_carry: None,
			windows: BTreeMap::new(),
		}
	}
}

impl<C: Slot, Carry: Archive, Output: Archive> MetaHighWater for CarryMeta<C, Carry, Output>
where
	Self: OperatorState<Archived = ArchivedCarryMeta<C, Carry, Output>>,
{
	fn archived_high_water_order(archived: &Self::Archived) -> Option<u64> {
		archived.high_water.as_ref().map(C::archived_order_key)
	}
}

type MetaLoaded<G, C, Carry, Output> = HashMap<G, CarryMeta<C, Carry, Output>>;
type SlotResolved = Vec<Option<(RowNumber, bool)>>;

pub struct TumblingCarryEngine<G, C: Slot, Accumulator, Carry, Output> {
	accumulators: StateCache<WindowStateKey, Accumulator>,
	meta: StateCache<MetaKey, CarryMeta<C, Carry, Output>>,
	meta_low_water: Option<u64>,
	retention: Option<C::Duration>,
	hydrated: bool,
	_pd: PhantomData<G>,
}

impl<G, C, Accumulator, Carry, Output> TumblingCarryEngine<G, C, Accumulator, Carry, Output>
where
	G: Clone + Eq + Ord + Hash + Debug,
	C: Slot + Hash,
	Accumulator: WindowAccumulator,
	Carry: Clone + Debug,
	Output: Clone + Debug,
	for<'a> &'a G: IntoEncodedKey,
	C: HeapSize,
	Carry: HeapSize,
	Output: HeapSize,
	Carry: Archive,
	Output: Archive,
	CarryMeta<C, Carry, Output>: OperatorState<Archived = ArchivedCarryMeta<C, Carry, Output>>,
{
	pub fn new(config: TumblingCarryConfig<C>) -> Self {
		let base = config.base();
		Self {
			accumulators: StateCache::<WindowStateKey, Accumulator>::new(base.budget()),
			meta: StateCache::<MetaKey, CarryMeta<C, Carry, Output>>::new(base.budget()),
			meta_low_water: None,
			retention: config.retention(),
			hydrated: false,
			_pd: PhantomData,
		}
	}

	fn hydrate_once<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		if self.hydrated {
			return Ok(());
		}
		self.accumulators.hydrate(store, accumulator_range(), decode_window_state_key)?;
		self.meta.hydrate(store, meta_range(), decode_meta_key)?;
		self.hydrated = true;
		Ok(())
	}

	pub fn approximate_memory(&self) -> StateMemory {
		self.accumulators.approximate_memory() + self.meta.approximate_memory()
	}

	pub fn dirty_memory(&self) -> StateMemory {
		self.accumulators.dirty_memory() + self.meta.dirty_memory()
	}

	pub fn membership_memory(&self) -> StateMemory {
		self.accumulators.membership_memory() + self.meta.membership_memory()
	}

	pub fn completeness(&self) -> StateCompleteness {
		self.accumulators.completeness().merge(self.meta.completeness())
	}

	pub fn expire_meta<S: StateStore>(&mut self, store: &mut S, threshold: u64) -> Result<usize> {
		self.hydrate_once(store)?;
		sweep_stale_meta(store, &mut self.meta, threshold, &mut self.meta_low_water)
	}

	#[allow(clippy::too_many_arguments)]
	pub fn apply<S, K, NA, BO, CF>(
		&mut self,
		store: &mut S,
		buckets: TumblingBuckets<G, C, Accumulator::Contribution>,
		row_key: K,
		new_accumulator: NA,
		build_output: BO,
		carry_forward: CF,
	) -> Result<Vec<WindowResult<G, C, Output>>>
	where
		S: StateStore,
		K: Fn(&G, C) -> EncodedKey,
		NA: Fn() -> Accumulator,
		BO: Fn(&G, WindowSpan<C>, &Accumulator::Output, Option<&Carry>) -> Option<Output>,
		CF: Fn(&Accumulator::Output, Option<&Carry>) -> Option<Carry>,
	{
		self.hydrate_once(store)?;
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let retention = self.retention;
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let slot_resolved = self.resolve_survivor_rows(store, &buckets, &meta_loaded, &row_key)?;

		let mut earliest_affected: HashMap<G, C> = HashMap::new();
		for (((group, span), events), slot_pre) in buckets.into_iter().zip(slot_resolved) {
			let entry = meta_loaded.entry(group.clone()).or_default();
			if matches!(entry.sealed_up_to, Some(s) if span.start <= s) {
				continue;
			}
			let row_number = match entry.windows.get(&span.start).map(|w| w.row_number) {
				Some(rn) => rn,
				None => match slot_pre {
					Some((rn, _)) => rn,
					None => continue,
				},
			};

			let mut accumulator: Accumulator = self
				.accumulators
				.get(store, &WindowStateKey::node_scoped(row_number))?
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
			self.accumulators.put(store, &WindowStateKey::node_scoped(row_number), accumulator)?;

			entry.windows.entry(span.start).or_insert_with(|| WindowEntry {
				row_number,
				span,
				carry_out: None,
				has_output: false,
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

		let mut results: Vec<WindowResult<G, C, Output>> = Vec::new();
		for (group, start) in earliest_affected {
			let meta = meta_loaded.get_mut(&group).expect("affected group has meta");

			let mut prev_carry: Option<Carry> = match meta.windows.range(..start).next_back() {
				Some((_, w)) => w.carry_out.clone(),
				None => meta.sealed_carry.clone(),
			};

			let coords: Vec<C> = meta.windows.range(start..).map(|(c, _)| *c).collect();
			let mut emptied: Vec<C> = Vec::new();
			for coord in coords {
				let (row_number, span, had_output) = {
					let w = meta.windows.get(&coord).expect("window entry present");
					(w.row_number, w.span, w.has_output)
				};
				let value = self
					.accumulators
					.read(store, &WindowStateKey::node_scoped(row_number), |view| match view {
						StateView::Native(a) => Ok(a.finalize()),
						StateView::Archived(archived) => {
							Accumulator::materialize(archived).map(|a| a.finalize())
						}
					})?
					.transpose()?
					.flatten();
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
						let w = meta.windows.get_mut(&coord).expect("window entry present");
						w.carry_out = new_carry.clone();
						w.has_output = true;
						w.last_output = Some(out.clone());
						if new_carry.is_some() {
							prev_carry = new_carry;
						}
						results.push(WindowResult {
							row_number,
							group: group.clone(),
							span,
							value: out,
							prior: None,
							kind,
						});
					}
					None => {
						if had_output
							&& let Some(prev) = meta
								.windows
								.get(&coord)
								.and_then(|w| w.last_output.clone())
						{
							results.push(WindowResult {
								row_number,
								group: group.clone(),
								span,
								value: prev,
								prior: None,
								kind: EmitKind::Remove,
							});
						}
						emptied.push(coord);
					}
				}
			}
			for coord in emptied {
				meta.windows.remove(&coord);
			}

			if let (Some(retention), Some(hw)) = (retention, meta.high_water) {
				loop {
					let Some((&first, w)) = meta.windows.iter().next() else {
						break;
					};
					if hw - first <= retention {
						break;
					}
					let carry_out = w.carry_out.clone();
					let row_number = w.row_number;
					meta.windows.remove(&first);
					meta.sealed_up_to = Some(first);
					meta.sealed_carry = carry_out;
					self.accumulators.remove(store, &WindowStateKey::node_scoped(row_number))?;
					store.remove_row_number(GroupId::NODE_SCOPE, &row_key(&group, first))?;
				}
			}
		}

		self.persist_meta(store, meta_loaded)?;
		Ok(results)
	}

	pub fn flush<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		self.accumulators.flush(store)?;
		self.meta.flush(store)?;
		Ok(())
	}

	fn warm_and_load_meta<S: StateStore>(
		&mut self,
		store: &mut S,
		buckets: &TumblingBuckets<G, C, Accumulator::Contribution>,
	) -> Result<MetaLoaded<G, C, Carry, Output>> {
		let meta_keys: Vec<MetaKey> = buckets
			.keys()
			.map(|(group, _)| group)
			.collect::<BTreeSet<_>>()
			.into_iter()
			.map(meta_key_for)
			.collect();
		self.meta.warm(store, &meta_keys)?;

		let mut meta_loaded: MetaLoaded<G, C, Carry, Output> = HashMap::new();
		for (group, _) in buckets.keys() {
			if !meta_loaded.contains_key(group) {
				let m = self.meta.take(store, &meta_key_for(group))?.unwrap_or_default();
				meta_loaded.insert(group.clone(), m);
			}
		}
		Ok(meta_loaded)
	}

	fn resolve_survivor_rows<S, K>(
		&mut self,
		store: &mut S,
		buckets: &TumblingBuckets<G, C, Accumulator::Contribution>,
		meta_loaded: &MetaLoaded<G, C, Carry, Output>,
		row_key: &K,
	) -> Result<SlotResolved>
	where
		S: StateStore,
		K: Fn(&G, C) -> EncodedKey,
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
		let resolved_rows = store.get_or_create_row_numbers(GroupId::NODE_SCOPE, &survivor_keys)?;
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
		let accumulator_keys: Vec<WindowStateKey> =
			resolved_rows.iter().map(|(rn, _)| WindowStateKey::node_scoped(*rn)).collect();
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

	fn persist_meta<S: StateStore>(
		&mut self,
		store: &mut S,
		meta_loaded: MetaLoaded<G, C, Carry, Output>,
	) -> Result<()> {
		for (group, meta) in meta_loaded {
			self.meta.put(store, &meta_key_for(&group), meta)?;
		}
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::HashMap,
		ops::Bound,
		sync::atomic::{AtomicUsize, Ordering},
	};

	use reifydb_codec::{key::encoded::EncodedKeyRange, state::StateBytes};

	use super::*;
	use crate::{
		key::operator_state::{Keyspace, OperatorStateKey},
		state::budget::OperatorStateBudgetHandle,
		window::{accumulator::invertible::RetainedAccumulator, engine::config::WindowEngineConfig},
	};

	// In-memory store that allocates a distinct row number per key (the state.rs
	// mock collapses every key onto row 1, which would alias all window
	// accumulators and defeat a storage-bound test).
	#[derive(Default)]
	struct CountingStore {
		data: HashMap<Vec<u8>, StateBytes>,
		rows: HashMap<(GroupId, Vec<u8>), RowNumber>,
		next_row: u64,
	}

	impl CountingStore {
		fn keyspace_count(&self, keyspace: Keyspace) -> usize {
			self.data
				.keys()
				.filter(|k| {
					OperatorStateKey::decode_inner(k).is_some_and(|(_, found, _)| found == keyspace)
				})
				.count()
		}

		// Live per-window accumulator rows sit in the ACCUMULATOR keyspace of the
		// internal store (alongside meta and the expiry index); count only those.
		fn accumulator_count(&self) -> usize {
			self.keyspace_count(Keyspace::ACCUMULATOR)
		}

		fn meta_entry_count(&self) -> usize {
			self.keyspace_count(Keyspace::WINDOW_META)
		}

		// One row-number mapping ('M') is minted per (group, window) via get_or_create_row_number;
		// this counts the live mappings so a test can prove sealed windows reclaim theirs.
		fn row_mapping_count(&self) -> usize {
			self.rows.len()
		}
	}

	impl StateStore for CountingStore {
		fn state_get(&mut self, key: &EncodedKey) -> Result<Option<StateBytes>> {
			Ok(self.data.get(key.as_bytes()).cloned())
		}
		fn state_get_many_visit(
			&mut self,
			keys: &[EncodedKey],
			visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
		) -> Result<()> {
			for key in keys {
				if let Some(b) = self.data.get(key.as_bytes()) {
					visit(key.clone(), b.clone())?;
				}
			}
			Ok(())
		}
		fn state_set(&mut self, key: &EncodedKey, payload: StateBytes) -> Result<()> {
			self.data.insert(key.as_bytes().to_vec(), payload);
			Ok(())
		}
		fn state_remove(&mut self, key: &EncodedKey) -> Result<()> {
			self.data.remove(key.as_bytes());
			Ok(())
		}
		fn state_range_visit(
			&mut self,
			range: EncodedKeyRange,
			limit: Option<usize>,
			visit: &mut dyn FnMut(EncodedKey, StateBytes) -> Result<()>,
		) -> Result<()> {
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
			let mut matched: Vec<(Vec<u8>, StateBytes)> = self
				.data
				.iter()
				.filter(|(k, _)| after_start(k) && before_end(k))
				.map(|(k, v)| (k.clone(), v.clone()))
				.collect();
			matched.sort_by(|a, b| a.0.cmp(&b.0));
			if let Some(limit) = limit {
				matched.truncate(limit);
			}
			for (k, b) in matched {
				visit(EncodedKey::new(k), b)?;
			}
			Ok(())
		}
		fn get_or_create_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<(RowNumber, bool)> {
			let slot = (group, key.as_bytes().to_vec());
			if let Some(rn) = self.rows.get(&slot) {
				return Ok((*rn, false));
			}
			self.next_row += 1;
			let rn = RowNumber(self.next_row);
			self.rows.insert(slot, rn);
			Ok((rn, true))
		}
		fn get_or_create_row_numbers(
			&mut self,
			group: GroupId,
			keys: &[EncodedKey],
		) -> Result<Vec<(RowNumber, bool)>> {
			keys.iter().map(|k| self.get_or_create_row_number(group, k)).collect()
		}
		fn remove_row_number(&mut self, group: GroupId, key: &EncodedKey) -> Result<()> {
			self.rows.remove(&(group, key.as_bytes().to_vec()));
			Ok(())
		}
		fn clock_now_nanos(&self) -> u64 {
			0
		}
	}

	type Engine = TumblingCarryEngine<String, u64, RetainedAccumulator<u64, f64>, f64, f64>;

	const WINDOW: u64 = 60;

	fn carry_config(retention: Option<u64>) -> TumblingCarryConfig<u64> {
		TumblingCarryConfig::builder(WindowEngineConfig::builder(OperatorStateBudgetHandle::default()).build())
			.retention(retention)
			.build()
	}

	// Feed one event into window `ws` for group "BTC" as its own batch, so the
	// high-water mark advances one window per call.
	fn feed(engine: &mut Engine, store: &mut CountingStore, ws: u64, price: f64) {
		let _ = feed_group(engine, store, "BTC", ws, price);
	}

	// Same as `feed` but for an explicit group, returning the emitted results so a caller can capture
	// the published row. Distinct groups get distinct accumulator rows, which is what the eviction test
	// needs to overflow the 8-slot accumulator cache.
	fn feed_group(
		engine: &mut Engine,
		store: &mut CountingStore,
		group: &str,
		ws: u64,
		price: f64,
	) -> Vec<WindowResult<String, u64, f64>> {
		let mut buckets: TumblingBuckets<String, u64, (u64, f64)> = BTreeMap::new();
		let span = WindowSpan::for_slot(ws, WINDOW);
		buckets.insert((group.to_string(), span), vec![AccumulatorEvent::Add((ws, price))]);
		engine.apply(
			store,
			buckets,
			|g: &String, w: u64| EncodedKey::builder().str(g).u64(w).build(),
			RetainedAccumulator::<u64, f64>::default,
			|_g: &String, _s: WindowSpan<u64>, v: &BTreeMap<u64, f64>, _p: Option<&f64>| {
				(!v.is_empty()).then(|| v.values().sum::<f64>())
			},
			|v: &BTreeMap<u64, f64>, _p: Option<&f64>| v.last_key_value().map(|(_, val)| *val),
		)
		.expect("apply")
	}

	#[test]
	fn retention_seals_old_windows_and_reclaims_accumulator_rows() {
		// With a 2-window retention horizon, only the windows within `hw - 120`
		// stay live; every older window must seal to the O(1) carry scalar and
		// have its accumulator row removed from the store. After 60 windows the
		// live accumulator-row count must stay bounded by the horizon, not grow
		// with the number of windows seen.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(Some(2 * WINDOW)));
		for i in 0..60u64 {
			feed(&mut engine, &mut store, i * WINDOW, i as f64);
		}
		engine.flush(&mut store).expect("flush");
		assert!(
			store.accumulator_count() <= 4,
			"sealed windows must reclaim their accumulator rows; found {} live rows after 60 windows",
			store.accumulator_count()
		);
	}

	#[test]
	fn retention_seals_old_windows_and_reclaims_row_number_mappings() {
		// The per-(group, window) row-number mapping ('M') is minted for every window but is not
		// reclaimed by accumulator eviction (it is keyed by row_key, not row_number). When a window
		// seals past retention its mapping must be dropped alongside its accumulator, or 'M' grows
		// per-window forever - a larger leak than the per-group meta. After 60 windows the mapping
		// count must stay bounded by the retention horizon, not track the number of windows seen.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(Some(2 * WINDOW)));
		for i in 0..60u64 {
			feed(&mut engine, &mut store, i * WINDOW, i as f64);
		}
		engine.flush(&mut store).expect("flush");
		assert!(
			store.row_mapping_count() <= 4,
			"sealed windows must reclaim their row-number mappings; found {} live mappings after 60 windows",
			store.row_mapping_count()
		);
	}

	#[test]
	fn meta_survives_while_group_high_water_at_or_after_threshold() {
		// Safety boundary: an active group whose high water is at or beyond the threshold must keep
		// its meta ('W') - the carry it holds is still needed to seed the next window.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(Some(2 * WINDOW)));
		for i in 0..3u64 {
			feed(&mut engine, &mut store, i * WINDOW, i as f64);
		}
		engine.flush(&mut store).expect("flush");
		let dropped = engine.expire_meta(&mut store, WINDOW).unwrap();
		assert_eq!(dropped, 0, "high water (2*WINDOW) is not below the threshold (WINDOW)");
		assert_eq!(store.meta_entry_count(), 1, "an active group within the horizon keeps its meta");
		assert!(store.accumulator_count() > 0, "live windows within retention keep their accumulators");
	}

	#[test]
	fn meta_reclaimed_when_group_stale_past_threshold() {
		// Invariant: a carry group that has gone quiet (high water below the threshold) is dead;
		// the sweep reclaims its meta and the sealed carry it held, bounding the per-group
		// internal-state growth that `persist_meta` would otherwise leak one key per group forever.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(Some(2 * WINDOW)));
		for i in 0..3u64 {
			feed(&mut engine, &mut store, i * WINDOW, i as f64);
		}
		engine.flush(&mut store).expect("flush");
		assert_eq!(store.meta_entry_count(), 1);

		let dropped = engine.expire_meta(&mut store, 100 * WINDOW).unwrap();
		assert_eq!(dropped, 1, "the quiet group's high water is far below the threshold");
		assert_eq!(store.meta_entry_count(), 0, "a dead carry group must not leak its meta");
	}

	#[test]
	fn without_retention_every_window_accumulator_is_retained() {
		// The contrast that proves the bound above is the sealing, not some
		// other cap: with no retention configured the engine keeps every window's
		// accumulator forever (the pre-sealing behavior).
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(None));
		for i in 0..60u64 {
			feed(&mut engine, &mut store, i * WINDOW, i as f64);
		}
		engine.flush(&mut store).expect("flush");
		assert_eq!(
			store.accumulator_count(),
			60,
			"with no retention the carry engine retains every window's accumulator row"
		);
	}

	#[test]
	fn terminal_remove_after_restart_uses_persisted_last_output() {
		// Unlike rolling, a carry window's withdrawn value cannot be recomputed from the window's own
		// surviving state: once it empties, its accumulator finalizes to nothing, and the output also
		// depended on the value carried in from earlier windows. So the engine persists `last_output`
		// in the WindowEntry. This test publishes a window, drops the engine (a restart / panic
		// recovery), then retracts the only contribution with a fresh engine that holds no in-memory
		// WindowEntry, and asserts the terminal Remove still carries the originally published value.
		// That proves `last_output` is durable, not ephemeral; if it were reverted to in-memory-only
		// state the second engine would have nothing to withdraw and this test would fail.
		let mut store = CountingStore::default();

		let mut engine = Engine::new(carry_config(None));
		feed(&mut engine, &mut store, 0, 5.0);
		engine.flush(&mut store).expect("flush");

		let mut engine = Engine::new(carry_config(None));
		let span = WindowSpan::for_slot(0, WINDOW);
		let mut buckets: TumblingBuckets<String, u64, (u64, f64)> = BTreeMap::new();
		buckets.insert(("BTC".to_string(), span), vec![AccumulatorEvent::Remove((0, 5.0))]);
		let withdrawn: Vec<WindowResult<String, u64, f64>> = engine
			.apply(
				&mut store,
				buckets,
				|g: &String, w: u64| EncodedKey::builder().str(g).u64(w).build(),
				RetainedAccumulator::<u64, f64>::default,
				|_g: &String, _s: WindowSpan<u64>, v: &BTreeMap<u64, f64>, _p: Option<&f64>| {
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
		// The other way the persisted state is read back is LRU eviction, no restart needed: the
		// accumulator cache holds only 8 windows, so tracking more evicts the oldest and the next
		// access re-reads it from the store. We publish 11 single-window groups so group G00 is evicted,
		// flush, then retract G00 and assert its accumulator reloads and the terminal Remove carries the
		// persisted last_output. It would fail if last_output stopped being persisted, or the
		// accumulator failed to round-trip through the store.
		let mut store = CountingStore::default();
		let mut engine = Engine::new(carry_config(None));

		let mut published_g00: Vec<WindowResult<String, u64, f64>> = Vec::new();
		for i in 0..11u64 {
			let group = format!("G{i:02}");
			let out = feed_group(&mut engine, &mut store, &group, 0, (i + 1) as f64);
			if i == 0 {
				published_g00 = out;
			}
		}
		engine.flush(&mut store).expect("flush");
		assert_eq!(published_g00.len(), 1);
		assert!(matches!(published_g00[0].kind, EmitKind::Insert));
		assert_eq!(published_g00[0].value, 1.0);

		// G00's window was published first and pushed out of the 8-slot accumulator cache by the later
		// groups, so the engine must re-read its accumulator from the store to apply this retraction.
		let span = WindowSpan::for_slot(0, WINDOW);
		let mut buckets: TumblingBuckets<String, u64, (u64, f64)> = BTreeMap::new();
		buckets.insert(("G00".to_string(), span), vec![AccumulatorEvent::Remove((0, 1.0))]);
		let withdrawn: Vec<WindowResult<String, u64, f64>> = engine
			.apply(
				&mut store,
				buckets,
				|g: &String, w: u64| EncodedKey::builder().str(g).u64(w).build(),
				RetainedAccumulator::<u64, f64>::default,
				|_g: &String, _s: WindowSpan<u64>, v: &BTreeMap<u64, f64>, _p: Option<&f64>| {
					(!v.is_empty()).then(|| v.values().sum::<f64>())
				},
				|v: &BTreeMap<u64, f64>, _p: Option<&f64>| v.last_key_value().map(|(_, val)| *val),
			)
			.expect("apply");
		engine.flush(&mut store).expect("flush");

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
	fn carry_meta_projects_its_high_water_without_touching_its_window_map() {
		// CarryMeta is the reason the archived projection exists: it carries a
		// whole BTreeMap of windows that the meta sweep has no use for, and the
		// old sweep deserialized all of it per group to read one u64. The
		// projection must agree with the owned path no matter how much
		// unrelated state the value holds.
		let mut meta: CarryMeta<u64, i64, i64> = CarryMeta::default();
		let empty_bytes = meta.encode_state(0).unwrap();
		assert_eq!(
			CarryMeta::<u64, i64, i64>::archived_high_water_order(
				CarryMeta::<u64, i64, i64>::archived(&empty_bytes).unwrap()
			),
			None,
			"a default CarryMeta has no high water"
		);

		meta.high_water = Some(99);
		meta.windows.insert(
			10,
			WindowEntry {
				row_number: RowNumber(1),
				span: WindowSpan::new(10u64, 20),
				carry_out: Some(7i64),
				has_output: true,
				last_output: Some(3i64),
			},
		);
		let bytes = meta.encode_state(0).unwrap();
		let projected = CarryMeta::<u64, i64, i64>::archived_high_water_order(
			CarryMeta::<u64, i64, i64>::archived(&bytes).unwrap(),
		);
		assert_eq!(projected, Some(99), "the populated window map must not disturb the high water");
	}

	// Fixed-size probe accumulator whose Clone increments a counter, so a test
	// can prove the carry chain finalizes loaded windows from the cache view
	// instead of deep-cloning them.
	static COUNTING_CARRY_CLONES: AtomicUsize = AtomicUsize::new(0);

	#[operator_state]
	#[derive(Debug, Default)]
	struct CountingCarryAcc {
		sum: f64,
		count: u64,
	}

	impl Clone for CountingCarryAcc {
		fn clone(&self) -> Self {
			COUNTING_CARRY_CLONES.fetch_add(1, Ordering::SeqCst);
			Self {
				sum: self.sum,
				count: self.count,
			}
		}
	}

	impl HeapSize for CountingCarryAcc {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl WindowAccumulator for CountingCarryAcc {
		type Contribution = f64;
		type Output = f64;

		fn add(&mut self, contribution: &f64) {
			self.sum += *contribution;
			self.count += 1;
		}
		fn remove(&mut self, contribution: &f64) {
			self.sum -= *contribution;
			self.count = self.count.saturating_sub(1);
		}
		fn finalize(&self) -> Option<f64> {
			(self.count > 0).then_some(self.sum)
		}
		fn is_empty(&self) -> bool {
			self.count == 0
		}
		fn merge(&mut self, other: &Self) {
			self.sum += other.sum;
			self.count += other.count;
		}
		fn unmerge(&mut self, other: &Self) {
			self.sum -= other.sum;
			self.count = self.count.saturating_sub(other.count);
		}
	}

	type ProbeEngine = TumblingCarryEngine<String, u64, CountingCarryAcc, f64, f64>;

	fn feed_probe(
		engine: &mut ProbeEngine,
		store: &mut CountingStore,
		ws: u64,
		value: f64,
	) -> Vec<WindowResult<String, u64, f64>> {
		let mut buckets: TumblingBuckets<String, u64, f64> = BTreeMap::new();
		let span = WindowSpan::for_slot(ws, WINDOW);
		buckets.insert(("BTC".to_string(), span), vec![AccumulatorEvent::Add(value)]);
		engine.apply(
			store,
			buckets,
			|g: &String, w: u64| EncodedKey::builder().str(g).u64(w).build(),
			CountingCarryAcc::default,
			|_g: &String, _s: WindowSpan<u64>, v: &f64, _p: Option<&f64>| Some(*v),
			|v: &f64, _p: Option<&f64>| Some(*v),
		)
		.expect("apply")
	}

	#[test]
	fn carry_chain_finalizes_loaded_windows_without_cloning() {
		// A batch touching an early window makes the carry chain re-finalize
		// every later window. With a fresh engine those accumulators load from
		// the store and must finalize via the archived view; the touched
		// window itself is dirty and must finalize via the Native view.
		// Exactly one clone is expected for the whole apply: the get() of the
		// touched window's accumulator on the read-modify-write path. The two
		// chain-loaded windows must contribute zero (the old get()-based chain
		// cloned all three).
		let mut store = CountingStore::default();
		let mut engine = ProbeEngine::new(carry_config(None));
		feed_probe(&mut engine, &mut store, 0, 1.0);
		feed_probe(&mut engine, &mut store, 60, 2.0);
		feed_probe(&mut engine, &mut store, 120, 3.0);
		engine.flush(&mut store).expect("flush");

		let mut fresh = ProbeEngine::new(carry_config(None));
		let before = COUNTING_CARRY_CLONES.load(Ordering::SeqCst);
		let results = feed_probe(&mut fresh, &mut store, 0, 1.0);
		assert!(
			results.iter().any(|w| w.span.start == 0 && w.value == 2.0),
			"the touched window re-emits its updated sum through the Native view"
		);
		assert!(
			results.iter().any(|w| w.span.start == 120 && w.value == 3.0),
			"a chain-loaded window re-emits its archived-view finalize output"
		);
		assert_eq!(
			COUNTING_CARRY_CLONES.load(Ordering::SeqCst) - before,
			1,
			"only the touched window's read-modify-write get() may clone; the carry chain must not"
		);
	}
}
