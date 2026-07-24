// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
};

use reifydb_codec::{
	key::{
		encode_u64,
		encoded::{EncodedKey, IntoEncodedKey},
	},
	state::OperatorState,
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};

use crate::{
	key::flow_node_internal_state::FlowNodeInternalStateKey,
	metrics::heap::{StateCompleteness, StateMemory},
	state::{
		cache::{StateCache, StateView},
		store::StateStore,
	},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, BatchMeta, EmitKind, GroupMeta, MetaKey, WindowResult, WindowStateKey,
			config::WindowEngineConfig, decode_meta_key, decode_window_state_key, expiry::ExpiryIndex,
			expiry_key, load_batch_meta, meta_key_for, persist_batch_meta, sweep_stale_meta, tag_range,
		},
		span::{Slot, WindowSpan},
	},
};

pub type TumblingBuckets<G, C, Contribution> = BTreeMap<(G, WindowSpan<C>), Vec<AccumulatorEvent<Contribution>>>;

type MetaLoaded<G, C> = HashMap<G, BatchMeta<C>>;
type SlotResolved = Vec<Option<(RowNumber, bool)>>;

pub struct ExpiredWindow<G, C, Output> {
	pub row_number: RowNumber,
	pub group: G,
	pub window_start: C,
	pub value: Option<Output>,
	pub accumulator_present: bool,
}

#[operator_state]
#[derive(Clone)]
pub struct TumblingIndexEntry<G, C> {
	group: G,
	window_start: C,
	row_number: u64,
}

pub struct TumblingEngine<G, C, Accumulator> {
	accumulators: StateCache<WindowStateKey, Accumulator>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	expiry: ExpiryIndex<TumblingIndexEntry<G, C>>,
	meta_low_water: Option<u64>,
	expire_batch: usize,
	hydrated: bool,
	_pd: PhantomData<G>,
}

impl<G, C, Accumulator> TumblingEngine<G, C, Accumulator>
where
	G: Clone + Eq + Ord + Hash + Debug,
	C: Slot + Hash,
	Accumulator: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
	GroupMeta<C>: OperatorState,
	TumblingIndexEntry<G, C>: OperatorState,
{
	pub fn new(config: WindowEngineConfig) -> Self {
		Self {
			accumulators: StateCache::<WindowStateKey, Accumulator>::new_internal(config.budget()),
			meta: StateCache::<MetaKey, GroupMeta<C>>::new_internal(config.budget()),
			expiry: ExpiryIndex::new(),
			meta_low_water: None,
			expire_batch: config.expire_batch(),
			hydrated: false,
			_pd: PhantomData,
		}
	}

	fn hydrate_once<S: StateStore>(&mut self, store: &mut S) -> Result<()> {
		if self.hydrated {
			return Ok(());
		}
		self.accumulators.hydrate(
			store,
			tag_range(FlowNodeInternalStateKey::WINDOW_ROW_STATE_TAG),
			decode_window_state_key,
		)?;
		self.meta.hydrate(store, tag_range(FlowNodeInternalStateKey::WINDOW_META_TAG), decode_meta_key)?;
		self.hydrated = true;
		Ok(())
	}

	pub fn reindex_window<S: StateStore>(
		&mut self,
		store: &mut S,
		group: &G,
		window_start: C,
		row_number: RowNumber,
		prior: Option<u64>,
		new: Option<u64>,
	) -> Result<()> {
		if prior == new {
			return Ok(());
		}
		let suffix = encode_u64(window_start.order_key());
		if let Some(old) = prior {
			self.expiry.drop_key(store, &expiry_key(old, group, &suffix))?;
		}
		if let Some(new) = new {
			let entry = TumblingIndexEntry {
				group: group.clone(),
				window_start,
				row_number: row_number.0,
			};
			self.expiry.set(store, expiry_key(new, group, &suffix), entry)?;
		}
		Ok(())
	}

	pub fn approximate_memory(&self) -> StateMemory {
		self.accumulators.approximate_memory()
			+ self.meta.approximate_memory()
			+ self.expiry.approximate_memory()
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

	pub fn apply<S, K, NA>(
		&mut self,
		store: &mut S,
		buckets: TumblingBuckets<G, C, Accumulator::Contribution>,
		row_key: K,
		new_accumulator: NA,
	) -> Result<Vec<WindowResult<G, C, Accumulator::Output>>>
	where
		S: StateStore,
		K: Fn(&G, C) -> EncodedKey,
		NA: Fn() -> Accumulator,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		self.hydrate_once(store)?;
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let slot_resolved = self.resolve_survivor_rows(store, &buckets, &meta_loaded, &row_key)?;
		let results =
			self.apply_events(store, buckets, slot_resolved, &mut meta_loaded, &row_key, &new_accumulator)?;
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

	fn resolve_survivor_rows<S, K>(
		&mut self,
		store: &mut S,
		buckets: &TumblingBuckets<G, C, Accumulator::Contribution>,
		meta_loaded: &MetaLoaded<G, C>,
		row_key: &K,
	) -> Result<SlotResolved>
	where
		S: StateStore,
		K: Fn(&G, C) -> EncodedKey,
	{
		let mut survivor_keys: Vec<EncodedKey> = Vec::new();
		let mut slot_survives: Vec<bool> = Vec::with_capacity(buckets.len());
		for (group, span) in buckets.keys() {
			let initial_high_water = meta_loaded.get(group).and_then(|m| m.initial);
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
		let accumulator_keys: Vec<WindowStateKey> =
			resolved_rows.iter().map(|(rn, _)| WindowStateKey(*rn)).collect();
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
		S: StateStore,
		K: Fn(&G, C) -> EncodedKey,
		NA: Fn() -> Accumulator,
	{
		let mut results: Vec<WindowResult<G, C, Accumulator::Output>> = Vec::new();

		for (((group, span), events), slot_pre) in buckets.into_iter().zip(slot_resolved) {
			meta_loaded.entry(group.clone()).or_default().observe(span.start);

			let (row_number, is_new) = match slot_pre {
				Some(resolved) => resolved,
				None => {
					let key = row_key(&group, span.start);
					store.get_or_create_row_number(&key)?
				}
			};

			let mut accumulator: Accumulator = self
				.accumulators
				.get(store, &WindowStateKey(row_number))?
				.unwrap_or_else(new_accumulator);
			let was_empty_before = accumulator.is_empty();
			let prior = if was_empty_before {
				None
			} else {
				accumulator.finalize()
			};

			for event in events {
				match event {
					AccumulatorEvent::Add(c) => {
						accumulator.add(&c);
					}
					AccumulatorEvent::Remove(c) => {
						if accumulator.is_empty() {
							continue;
						}
						accumulator.remove(&c);
					}
				}
			}

			let value = accumulator.finalize();
			self.accumulators.put(store, &WindowStateKey(row_number), accumulator)?;

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

	pub fn expire<S: StateStore>(
		&mut self,
		store: &mut S,
		threshold: u64,
	) -> Result<Vec<ExpiredWindow<G, C, Accumulator::Output>>> {
		self.hydrate_once(store)?;
		let due = self.expiry.due(store, threshold, self.expire_batch)?;

		let mut out: Vec<ExpiredWindow<G, C, Accumulator::Output>> = Vec::new();
		for (index_key, entry) in due {
			let row_number = RowNumber(entry.row_number);
			self.expiry.drop_key(store, &index_key)?;
			let found = self.accumulators.read(store, &WindowStateKey(row_number), |view| match view {
				StateView::Native(accumulator) => Ok(accumulator.finalize()),
				StateView::Archived(archived) => {
					Accumulator::materialize(archived).map(|accumulator| accumulator.finalize())
				}
			})?;
			let accumulator_present = found.is_some();
			let value = found.transpose()?.flatten();
			self.accumulators.remove(store, &WindowStateKey(row_number))?;
			out.push(ExpiredWindow {
				row_number,
				group: entry.group,
				window_start: entry.window_start,
				value,
				accumulator_present,
			});
		}
		Ok(out)
	}

	fn persist_meta<S: StateStore>(&mut self, store: &mut S, meta_loaded: MetaLoaded<G, C>) -> Result<()> {
		persist_batch_meta(store, &mut self.meta, meta_loaded)
	}

	pub fn expire_meta<S: StateStore>(&mut self, store: &mut S, threshold: u64) -> Result<usize> {
		sweep_stale_meta(store, &mut self.meta, threshold, &mut self.meta_low_water)
	}
}

#[cfg(test)]
mod tests {
	use std::{
		collections::BTreeMap,
		sync::atomic::{AtomicUsize, Ordering},
	};

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_macro::operator_state;
	use reifydb_value::{Result, count::Count, value::row_number::RowNumber};

	use crate::{
		metrics::heap::HeapSize,
		state::{budget::OperatorStateBudgetHandle, cache::StateView},
		window::{
			accumulator::WindowAccumulator,
			engine::{
				AccumulatorEvent, EmitKind, GroupMeta, MetaHighWater, WindowResult,
				config::WindowEngineConfig,
				meta_key_for,
				test_support::{MockStore, SumAccumulator},
				tumbling::{TumblingBuckets, TumblingEngine},
			},
			span::WindowSpan,
		},
	};

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder(OperatorStateBudgetHandle::default()).build()
	}

	fn row_key(group: &u32, window_start: u64) -> EncodedKey {
		EncodedKey::builder().u32(*group).u64(window_start).build()
	}

	// The faces reindex through their long-lived engine; these tests seed windows with
	// throwaway engines, so reindex through a fresh one - the write-through store copy
	// is what the expiring engine's mirror later hydrates from, which is exactly the
	// restart path under test.
	fn reindex_window(
		store: &mut MockStore,
		group: &u32,
		window_start: u64,
		row_number: RowNumber,
		prior: Option<u64>,
		new: Option<u64>,
	) -> Result<()> {
		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		engine.reindex_window(store, group, window_start, row_number, prior, new)
	}

	fn seed_window(store: &mut MockStore, window_start: u64, contribution: i64) -> WindowResult<u32, u64, i64> {
		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert(
			(1u32, WindowSpan::new(window_start, window_start + 1)),
			vec![AccumulatorEvent::Add(contribution)],
		);
		let mut results = engine.apply(store, buckets, row_key, SumAccumulator::default).expect("apply");
		engine.flush(store).expect("flush");
		results.pop().expect("one window")
	}

	fn apply_event(store: &mut MockStore, window_start: u64, event: AccumulatorEvent<i64>) {
		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(window_start, window_start + 1)), vec![event]);
		engine.apply(store, buckets, row_key, SumAccumulator::default).expect("apply");
		engine.flush(store).expect("flush");
	}

	#[test]
	fn expire_returns_only_due_windows_and_clears_their_state() {
		let mut store = MockStore::default();
		// Two live windows; the face indexes each by its last_event_time (10 and 90).
		let w0 = seed_window(&mut store, 0, 5);
		reindex_window(&mut store, &w0.group, w0.span.start, w0.row_number, None, Some(10)).unwrap();
		let w100 = seed_window(&mut store, 100, 7);
		reindex_window(&mut store, &w100.group, w100.span.start, w100.row_number, None, Some(90)).unwrap();
		assert_eq!(store.index_entry_count(), 2, "both live windows are indexed");

		// Threshold 10: only the window whose expiry (10) is at/under the threshold is due.
		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let expired = engine.expire(&mut store, 10).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(expired.len(), 1, "exactly one window is due, not the whole population");
		assert_eq!(expired[0].window_start, 0);
		assert_eq!(expired[0].value, Some(5));
		assert_eq!(store.index_entry_count(), 1, "the due window's index entry is gone, the other remains");

		// The surviving window finalizes correctly once the threshold reaches it.
		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let later = engine.expire(&mut store, 1000).unwrap();
		assert_eq!(later.len(), 1);
		assert_eq!(later[0].window_start, 100);
		assert_eq!(later[0].value, Some(7));
		assert_eq!(store.index_entry_count(), 0);
	}

	#[test]
	fn an_expiry_entry_whose_state_was_reclaimed_reports_its_accumulator_absent() {
		// The expiry index is ordered by due time, so a group's entries sit OUTSIDE its key range
		// and survive phase-1 reclamation; they are left to drain on their own. When such a stale
		// entry drains it must be inert, and the only way a caller can tell is this signal. Drivers
		// key row-number removal off it: reporting a reclaimed group's accumulator as present would
		// delete the identity of a group that may still own a live sink row, which for a coord-less
		// operator mints a duplicate row on the next wake (landmine L2).
		let mut store = MockStore::default();
		let w = seed_window(&mut store, 0, 5);
		reindex_window(&mut store, &w.group, w.span.start, w.row_number, None, Some(10)).unwrap();
		assert_eq!(store.index_entry_count(), 1, "precondition: the window is indexed");
		assert_eq!(store.drop_accumulator_entries(), 1, "precondition: reclaim erased the accumulator");

		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let expired = engine.expire(&mut store, 10).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(expired.len(), 1, "the stale entry must still drain, or the index would grow forever");
		assert!(
			!expired[0].accumulator_present,
			"the accumulator is gone, so the entry names state that no longer exists and must be \
			 reported as such"
		);
		assert_eq!(store.index_entry_count(), 0, "a stale entry drops itself on the scan that finds it");
	}

	#[test]
	fn an_emptied_window_still_reports_its_accumulator_present() {
		// Presence must not be inferred from `value`. An accumulator drained to zero by retractions
		// still EXISTS and still owns its row-number mapping, but finalizes to none - exactly the same
		// `value` a reclaimed group produces. Collapsing the two signals would strand one mapping per
		// emptied window forever, which is the leak this whole step exists to close.
		let mut store = MockStore::default();
		let w = seed_window(&mut store, 0, 5);
		apply_event(&mut store, 0, AccumulatorEvent::Remove(5));
		reindex_window(&mut store, &w.group, w.span.start, w.row_number, None, Some(10)).unwrap();

		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let expired = engine.expire(&mut store, 10).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(expired.len(), 1);
		assert_eq!(expired[0].value, None, "an emptied accumulator finalizes to nothing");
		assert!(
			expired[0].accumulator_present,
			"the accumulator row still exists, so its identity must still be released on expiry"
		);
	}

	#[test]
	fn meta_reclaimed_when_group_stale_past_threshold() {
		// Invariant: a group whose high water has fallen below the staleness threshold has stopped
		// advancing and its per-group GroupMeta ('W') must be reclaimed. `persist_meta` writes one
		// meta per group and never removes it, so without the sweep one internal-state key leaks per
		// distinct group (mint pair) forever - the unbounded tail behind the jupiter memory growth.
		let mut store = MockStore::default();
		seed_window(&mut store, 0, 5);
		assert_eq!(store.meta_entry_count(), 1, "applying a window persisted the group's meta");

		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let dropped = engine.expire_meta(&mut store, 100).unwrap();
		assert_eq!(dropped, 1, "the group's high water (0) is below the threshold (100)");
		assert_eq!(store.meta_entry_count(), 0, "a stale group must not leak its GroupMeta");
	}

	#[test]
	fn meta_survives_while_group_high_water_at_or_after_threshold() {
		// Safety boundary: a group whose high water is at or beyond the threshold is still live
		// (its late-event horizon has not passed) and must keep its meta.
		let mut store = MockStore::default();
		seed_window(&mut store, 100, 7);
		assert_eq!(store.meta_entry_count(), 1);

		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let dropped = engine.expire_meta(&mut store, 50).unwrap();
		assert_eq!(dropped, 0, "high water (100) is not below the threshold (50)");
		assert_eq!(store.meta_entry_count(), 1, "a group within the staleness horizon keeps its meta");
	}

	#[test]
	fn meta_sweep_leaves_row_number_mappings_intact() {
		// Scoping guard: the sweep targets only meta keys ('W'). It must not touch the write-once
		// row-number mappings ('M') that share the OperatorInternal tier - deleting those would
		// corrupt the operator.
		let mut store = MockStore::default();
		seed_window(&mut store, 0, 5);
		store.seed_mapping_key(0x01);
		assert_eq!(store.mapping_entry_count(), 1);

		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		engine.expire_meta(&mut store, 100).unwrap();
		assert_eq!(store.meta_entry_count(), 0, "the stale group's meta is swept");
		assert_eq!(store.mapping_entry_count(), 1, "the sweep must not touch row-number mapping keys");
	}

	#[test]
	fn meta_sweep_skips_then_reclaims_as_threshold_advances() {
		// The low-water guard must skip the scan while the smallest high water is at or above the
		// threshold, yet still reclaim the group once the threshold advances past it - the guard is
		// an optimization to avoid scanning every apply, never a correctness hole.
		let mut store = MockStore::default();
		seed_window(&mut store, 100, 7);

		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		// Below the group's high water: nothing stale; the sweep records the low-water bound (100).
		assert_eq!(engine.expire_meta(&mut store, 50).unwrap(), 0);
		assert_eq!(store.meta_entry_count(), 1);
		// Threshold equals the bound: still nothing strictly below it, a no-op skip.
		assert_eq!(engine.expire_meta(&mut store, 100).unwrap(), 0);
		assert_eq!(store.meta_entry_count(), 1);
		// Threshold crosses the group's high water: it is now stale and reclaimed.
		assert_eq!(engine.expire_meta(&mut store, 101).unwrap(), 1);
		assert_eq!(store.meta_entry_count(), 0, "the guard must not permanently skip a group that goes stale");
	}

	#[test]
	fn expire_threshold_is_inclusive() {
		let mut store = MockStore::default();
		let w = seed_window(&mut store, 0, 4);
		reindex_window(&mut store, &w.group, w.span.start, w.row_number, None, Some(50)).unwrap();

		// One below the expiry: not due, and the scan leaves the index intact.
		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		assert!(engine.expire(&mut store, 49).unwrap().is_empty());
		engine.flush(&mut store).unwrap();
		assert_eq!(store.index_entry_count(), 1);

		// Exactly at the expiry: due (the face folds the strict close boundary into the threshold).
		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		assert_eq!(engine.expire(&mut store, 50).unwrap().len(), 1);
	}

	#[test]
	fn expire_processes_at_most_expire_batch_then_resumes_next_tick() {
		// Guard rail from the jupiter/pump incident: expire used to drain every due window in
		// one tick, so a due-window burst on one bloated operator stalled the whole flow actor
		// pass (all node ticks run serialized; tick p99 exceeded 100ms). The batch cap bounds
		// one tick's work; the remainder stays in the due index and drains on later ticks, so
		// nothing is lost, only deferred. The due index sorts by inverted expiry (encode_u64),
		// so the scan yields the newest-due windows first and the oldest backlog defers.
		let mut store = MockStore::default();
		for (start, due) in [(0u64, 10u64), (100, 20), (200, 30)] {
			let w = seed_window(&mut store, start, 1);
			reindex_window(&mut store, &w.group, w.span.start, w.row_number, None, Some(due)).unwrap();
		}
		assert_eq!(store.index_entry_count(), 3);

		let capped = WindowEngineConfig::builder(OperatorStateBudgetHandle::default()).expire_batch(2).build();

		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(capped.clone());
		let first = engine.expire(&mut store, 1000).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(first.len(), 2, "one tick drains at most expire_batch windows");
		assert_eq!(first[0].window_start, 200, "inverted key order: newest due drains first");
		assert_eq!(first[1].window_start, 100);
		assert_eq!(store.index_entry_count(), 1, "the deferred window keeps its index entry");

		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(capped);
		let second = engine.expire(&mut store, 1000).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(second.len(), 1, "the next tick picks up the deferred backlog");
		assert_eq!(second[0].window_start, 0);
		assert_eq!(second[0].value, Some(1), "a deferred window still finalizes with its state intact");
		assert_eq!(store.index_entry_count(), 0);
	}

	#[test]
	fn reindex_rekeys_without_leaving_a_stale_entry() {
		let mut store = MockStore::default();
		let w = seed_window(&mut store, 0, 9);
		// Index at 10, then a later event advances the window's expiry to 80.
		reindex_window(&mut store, &w.group, w.span.start, w.row_number, None, Some(10)).unwrap();
		reindex_window(&mut store, &w.group, w.span.start, w.row_number, Some(10), Some(80)).unwrap();
		assert_eq!(store.index_entry_count(), 1, "re-keying must not leave the old entry behind");

		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		assert!(engine.expire(&mut store, 10).unwrap().is_empty(), "no longer due at the old expiry");
		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		assert_eq!(engine.expire(&mut store, 80).unwrap().len(), 1, "due at the new expiry");
	}

	#[test]
	fn accumulator_survives_restart() {
		// When a tumbling window empties under retraction it emits a terminal Remove carrying the value
		// it last published; that value is the window accumulator's pre-batch finalize, read back from
		// the store. Dropping the engine between the publish and the retraction (a restart) forces the
		// accumulator to be reloaded from the store rather than served from the in-memory cache. It
		// would fail if the accumulator failed to round-trip through the store (a serialization break,
		// or a second Data cache colliding on the same RowNumber).
		let mut store = MockStore::default();

		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(0, 1)), vec![AccumulatorEvent::Add(5)]);
		let published: Vec<WindowResult<u32, u64, i64>> =
			engine.apply(&mut store, buckets, row_key, SumAccumulator::default).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(published.len(), 1);
		assert!(matches!(published[0].kind, EmitKind::Insert));
		assert_eq!(published[0].value, 5);

		// Restart: a brand new engine with empty caches, forced to read the persisted accumulator back.
		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(0, 1)), vec![AccumulatorEvent::Remove(5)]);
		let withdrawn: Vec<WindowResult<u32, u64, i64>> =
			engine.apply(&mut store, buckets, row_key, SumAccumulator::default).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(withdrawn.len(), 1, "emptying the window emits exactly one terminal diff");
		assert!(
			matches!(withdrawn[0].kind, EmitKind::Remove),
			"the window emptied under retraction, so the last published row must be withdrawn"
		);
		assert_eq!(withdrawn[0].value, 5, "the withdrawn value is the reloaded pre-batch accumulator output");
		assert_eq!(
			withdrawn[0].row_number, published[0].row_number,
			"the withdrawal targets the same row that was published"
		);
	}

	#[test]
	fn accumulator_survives_lru_eviction() {
		// The other way a read reaches the store is LRU eviction, no restart needed: the accumulator
		// cache holds only 8 windows, so more than that evicts the oldest and the next access re-reads
		// it from the store. We publish 11 single-window groups so group 1 is evicted, flush, then
		// retract group 1 and assert its accumulator is read back intact.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());

		let mut published_group_1: Vec<WindowResult<u32, u64, i64>> = Vec::new();
		for group in 1u32..=11u32 {
			let mut buckets: TumblingBuckets<u32, u64, i64> = BTreeMap::new();
			buckets.insert((group, WindowSpan::new(0, 1)), vec![AccumulatorEvent::Add(i64::from(group))]);
			let out: Vec<WindowResult<u32, u64, i64>> =
				engine.apply(&mut store, buckets, row_key, SumAccumulator::default).unwrap();
			if group == 1 {
				published_group_1 = out;
			}
		}
		engine.flush(&mut store).unwrap();
		assert_eq!(published_group_1.len(), 1);
		assert!(matches!(published_group_1[0].kind, EmitKind::Insert));
		assert_eq!(published_group_1[0].value, 1);

		// Group 1's window was published first and pushed out of the 8-slot cache by the later groups,
		// so the same engine must re-read its accumulator from the store to apply this retraction.
		let mut buckets: TumblingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(0, 1)), vec![AccumulatorEvent::Remove(1)]);
		let withdrawn: Vec<WindowResult<u32, u64, i64>> =
			engine.apply(&mut store, buckets, row_key, SumAccumulator::default).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(withdrawn.len(), 1, "emptying the evicted window emits exactly one terminal diff");
		assert!(
			matches!(withdrawn[0].kind, EmitKind::Remove),
			"the evicted window emptied under retraction, so the last published row must be withdrawn"
		);
		assert_eq!(withdrawn[0].value, 1, "the withdrawn value is the reloaded accumulator output for group 1");
		assert_eq!(
			withdrawn[0].row_number, published_group_1[0].row_number,
			"the withdrawal targets the same row that was published for group 1"
		);
	}

	// SumAccumulator twin whose Clone increments a counter, so a test can prove
	// expire() finalizes from the cache-resident view instead of deep-cloning.
	static COUNTING_ACC_CLONES: AtomicUsize = AtomicUsize::new(0);

	#[operator_state]
	#[derive(Debug, Default)]
	struct CountingAcc {
		sum: i64,
		count: u64,
	}

	impl Clone for CountingAcc {
		fn clone(&self) -> Self {
			COUNTING_ACC_CLONES.fetch_add(1, Ordering::SeqCst);
			Self {
				sum: self.sum,
				count: self.count,
			}
		}
	}

	impl HeapSize for CountingAcc {
		fn heap_size(&self) -> usize {
			0
		}
	}

	impl WindowAccumulator for CountingAcc {
		type Contribution = i64;
		type Output = i64;

		fn add(&mut self, contribution: &i64) {
			self.sum += *contribution;
			self.count += 1;
		}
		fn remove(&mut self, contribution: &i64) {
			self.sum -= *contribution;
			self.count = self.count.saturating_sub(1);
		}
		fn finalize(&self) -> Option<i64> {
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

	#[test]
	fn expire_finalizes_from_resident_views_without_cloning() {
		// expire() must serve finalize() from the cache view: a same-engine
		// (Native-resident) entry finalizes by reference, and a store-loaded
		// entry finalizes via the archived form without ever creating native
		// residency. The old get()-based path deep-cloned the accumulator in
		// both cases; the clone counter pins zero clones during expire, and the
		// emitted values pin that both view arms produce the same output.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, u64, CountingAcc>::new(test_config());
		let mut buckets: TumblingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(0, 1)), vec![AccumulatorEvent::Add(5)]);
		buckets.insert((1u32, WindowSpan::new(100, 101)), vec![AccumulatorEvent::Add(7)]);
		let results = engine.apply(&mut store, buckets, row_key, CountingAcc::default).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(results.len(), 2);
		for w in &results {
			let expiry = if w.span.start == 0 {
				10
			} else {
				90
			};
			reindex_window(&mut store, &w.group, w.span.start, w.row_number, None, Some(expiry)).unwrap();
		}

		let before = COUNTING_ACC_CLONES.load(Ordering::SeqCst);

		// Same engine: the window-0 accumulator is clean Native after flush.
		let expired = engine.expire(&mut store, 10).unwrap();
		assert_eq!(expired.len(), 1);
		assert_eq!(expired[0].value, Some(5), "Native-view finalize output");

		// Fresh engine: the window-100 accumulator loads from the store and
		// finalizes through the archived view.
		let mut fresh = TumblingEngine::<u32, u64, CountingAcc>::new(test_config());
		let expired = fresh.expire(&mut store, 1000).unwrap();
		assert_eq!(expired.len(), 1);
		assert_eq!(expired[0].value, Some(7), "archived-view finalize output");

		assert_eq!(
			COUNTING_ACC_CLONES.load(Ordering::SeqCst) - before,
			0,
			"expire must not clone accumulators on either the Native or the archived path"
		);
	}

	fn read_high_water(
		engine: &mut TumblingEngine<u32, u64, SumAccumulator>,
		store: &mut MockStore,
		group: u32,
	) -> Option<u64> {
		engine.meta
			.read(store, &meta_key_for(&group), |view| match view {
				StateView::Archived(meta) => GroupMeta::<u64>::archived_high_water_order(meta),
				StateView::Native(meta) => meta.high_water,
			})
			.unwrap()
			.flatten()
	}

	#[test]
	fn warmed_meta_high_water_advances_through_the_sealed_path() {
		// A store-warmed GroupMeta must never materialize while batches only
		// advance its high water: the load is a read() snapshot and the
		// persist a sealed in-place write. Pinned three ways: the pending
		// bump is held as archived bytes (StateView::Archived on the dirty
		// slot), the seal paid exactly the one CoW for the store-shared row,
		// and the bumped value round-trips to a third engine.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(100, 101)), vec![AccumulatorEvent::Add(5)]);
		engine.apply(&mut store, buckets, row_key, SumAccumulator::default).unwrap();
		engine.flush(&mut store).unwrap();

		let mut fresh = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(200, 201)), vec![AccumulatorEvent::Add(7)]);
		fresh.apply(&mut store, buckets, row_key, SumAccumulator::default).unwrap();

		assert_eq!(
			fresh.meta.seal_copies(),
			Count::new(1),
			"the bump seals the archived meta in place (one CoW for the store-shared row)"
		);
		let archived_resident = fresh
			.meta
			.read(&mut store, &meta_key_for(&1u32), |view| matches!(view, StateView::Archived(_)))
			.unwrap();
		assert_eq!(
			archived_resident,
			Some(true),
			"the pending bump is sealed archived bytes, not a materialized value"
		);
		fresh.flush(&mut store).unwrap();

		let mut third = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		assert_eq!(
			read_high_water(&mut third, &mut store, 1),
			Some(200),
			"the sealed write round-trips through the store"
		);
	}

	#[test]
	fn persisted_none_meta_takes_the_native_fallback() {
		// Legacy rows: the old unconditional persist wrote GroupMeta with a
		// none high water for retraction-only groups. ArchivedOption cannot
		// express none -> Some through a Seal, so the sealed persist must
		// decline and the native fallback must still land the bump.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		engine.meta
			.put(
				&mut store,
				&meta_key_for(&1u32),
				GroupMeta {
					high_water: None,
				},
			)
			.unwrap();
		engine.meta.flush(&mut store).unwrap();

		let mut fresh = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, u64, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(100, 101)), vec![AccumulatorEvent::Add(7)]);
		fresh.apply(&mut store, buckets, row_key, SumAccumulator::default).unwrap();
		fresh.flush(&mut store).unwrap();

		let mut third = TumblingEngine::<u32, u64, SumAccumulator>::new(test_config());
		assert_eq!(
			read_high_water(&mut third, &mut store, 1),
			Some(100),
			"the declined seal must land the bump via the native fallback"
		);
	}
}
