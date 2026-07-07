// SPDX-License-Identifier: Apache-2.0
// Copyright (c) 2026 ReifyDB

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	fmt::Debug,
	hash::Hash,
	marker::PhantomData,
};

use reifydb_codec::key::{
	encode_u64,
	encoded::{EncodedKey, IntoEncodedKey},
};
use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::window::{
	accumulator::WindowAccumulator,
	engine::{
		AccumulatorEvent, EmitKind, GroupMeta, MetaKey, WindowResult, WindowStateKey,
		config::WindowEngineConfig, expiry_due_range, expiry_key, meta_key_for, sweep_stale_meta,
	},
	span::{Slot, WindowSpan},
	state::StateCache,
	store::WindowStore,
};

pub type TumblingBuckets<G, C, Contribution> = BTreeMap<(G, WindowSpan<C>), Vec<AccumulatorEvent<Contribution>>>;

type MetaLoaded<G, C> = HashMap<G, GroupMeta<C>>;
type SlotResolved = Vec<Option<(RowNumber, bool)>>;

pub struct ExpiredWindow<G, C, Output> {
	pub row_number: RowNumber,
	pub group: G,
	pub window_start: C,
	pub value: Option<Output>,
}

#[derive(Serialize, Deserialize)]
#[serde(bound(serialize = "G: Serialize, C: Serialize", deserialize = "G: DeserializeOwned, C: DeserializeOwned"))]
struct TumblingIndexEntry<G, C> {
	group: G,
	window_start: C,
	row_number: u64,
}

pub fn reindex_window<S, G, C>(
	store: &mut S,
	group: &G,
	window_start: C,
	row_number: RowNumber,
	prior: Option<u64>,
	new: Option<u64>,
) -> Result<()>
where
	S: WindowStore,
	G: Clone + Serialize,
	C: Slot + Serialize,
	for<'a> &'a G: IntoEncodedKey,
{
	if prior == new {
		return Ok(());
	}
	let suffix = encode_u64(window_start.order_key());
	if let Some(old) = prior {
		store.internal_drop(&expiry_key(old, group, &suffix))?;
	}
	if let Some(new) = new {
		store.internal_set(
			&expiry_key(new, group, &suffix),
			&TumblingIndexEntry {
				group: group.clone(),
				window_start,
				row_number: row_number.0,
			},
		)?;
	}
	Ok(())
}

pub struct TumblingEngine<G, C, Accumulator> {
	accumulators: StateCache<WindowStateKey, Accumulator>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	meta_low_water: Option<u64>,
	expire_batch: usize,
	_pd: PhantomData<G>,
}

impl<G, C, Accumulator> TumblingEngine<G, C, Accumulator>
where
	G: Clone + Eq + Ord + Hash + Debug + Serialize + DeserializeOwned,
	C: Slot + Hash + Serialize + DeserializeOwned,
	Accumulator: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
{
	pub fn new(config: WindowEngineConfig) -> Self {
		Self {
			accumulators: StateCache::<WindowStateKey, Accumulator>::new_internal(
				config.state_cache_capacity(),
			),
			meta: StateCache::<MetaKey, GroupMeta<C>>::new_internal(config.internal_state_cache_capacity()),
			meta_low_water: None,
			expire_batch: config.expire_batch(),
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
		S: WindowStore,
		K: Fn(&G, C) -> EncodedKey,
		NA: Fn() -> Accumulator,
	{
		let mut results: Vec<WindowResult<G, C, Accumulator::Output>> = Vec::new();

		for (((group, span), events), slot_pre) in buckets.into_iter().zip(slot_resolved) {
			let entry = meta_loaded.entry(group.clone()).or_default();
			match entry.high_water {
				Some(hw) if span.start > hw => entry.high_water = Some(span.start),
				None => entry.high_water = Some(span.start),
				_ => {}
			}

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

	pub fn expire<S: WindowStore>(
		&mut self,
		store: &mut S,
		threshold: u64,
	) -> Result<Vec<ExpiredWindow<G, C, Accumulator::Output>>> {
		let mut due: Vec<(EncodedKey, TumblingIndexEntry<G, C>)> = Vec::new();
		store.internal_range_visit::<TumblingIndexEntry<G, C>>(
			expiry_due_range(threshold),
			Some(self.expire_batch),
			&mut |key, entry| {
				due.push((key, entry));
				Ok(())
			},
		)?;

		let mut out: Vec<ExpiredWindow<G, C, Accumulator::Output>> = Vec::new();
		for (index_key, entry) in due {
			let row_number = RowNumber(entry.row_number);
			store.internal_drop(&index_key)?;
			let value = self
				.accumulators
				.get(store, &WindowStateKey(row_number))?
				.and_then(|accumulator| accumulator.finalize());
			self.accumulators.remove(store, &WindowStateKey(row_number))?;
			out.push(ExpiredWindow {
				row_number,
				group: entry.group,
				window_start: entry.window_start,
				value,
			});
		}
		Ok(out)
	}

	fn persist_meta<S: WindowStore>(&mut self, store: &mut S, meta_loaded: MetaLoaded<G, C>) -> Result<()> {
		for (group, meta) in meta_loaded {
			self.meta.set(store, &meta_key_for(&group), &meta)?;
		}
		Ok(())
	}

	pub fn expire_meta<S: WindowStore>(&mut self, store: &mut S, threshold: u64) -> Result<usize> {
		sweep_stale_meta(store, &mut self.meta, threshold, &mut self.meta_low_water)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_codec::key::encoded::EncodedKey;

	use crate::window::{
		engine::{
			AccumulatorEvent, EmitKind, WindowResult,
			config::WindowEngineConfig,
			test_support::{MockStore, SumAccumulator},
			tumbling::{TumblingBuckets, TumblingEngine, reindex_window},
		},
		span::WindowSpan,
	};

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder().state_cache_capacity(8).internal_state_cache_capacity(64).build()
	}

	fn row_key(group: &u32, window_start: u64) -> EncodedKey {
		EncodedKey::builder().u32(*group).u64(window_start).build()
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

		let capped = WindowEngineConfig::builder()
			.state_cache_capacity(8)
			.internal_state_cache_capacity(64)
			.expire_batch(2)
			.build();

		let mut engine = TumblingEngine::<u32, u64, SumAccumulator>::new(capped);
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
}
