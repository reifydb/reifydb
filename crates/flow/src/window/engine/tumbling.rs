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
	row::operator::OperatorState,
};
use reifydb_core::{
	key::operator_state::GroupId,
	state::{cache::StateCache, store::StateStore},
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, reifydb_assertions};

use crate::{
	state::expiry::{ExpiryIndex, expiry_key},
	window::{
		accumulator::WindowAccumulator,
		engine::{
			AccumulatorEvent, BatchMeta, EmitKind, GroupMeta, MetaKey, WindowResult, WindowStateKey,
			accumulator_range, config::WindowEngineConfig, decode_meta_key, decode_window_state_key,
			load_batch_meta, meta_key_for, meta_range, note_when_expiry_capped, persist_batch_meta,
			sweep_stale_meta,
		},
		span::{WindowAnchor, WindowSpan},
	},
};

pub type TumblingBuckets<G, C, Contribution> = BTreeMap<(G, WindowSpan<C>), Vec<AccumulatorEvent<Contribution>>>;

type MetaLoaded<G, C> = HashMap<G, BatchMeta<C>>;

#[derive(Clone)]
struct ResolvedSlot {
	group: GroupId,
	key: EncodedKey,
}

type SlotResolved<G, C> = HashMap<(G, WindowSpan<C>), ResolvedSlot>;

pub struct ExpiredWindow<G, C> {
	pub group: G,
	pub group_id: GroupId,
	pub window_start: C,
}

#[operator_state]
#[derive(Clone)]
pub struct TumblingIndexEntry<G, C> {
	group: G,
	window_start: C,
	group_id: u64,
	slot_key: Vec<u8>,
}

pub struct TumblingEngine<G, C, Accumulator> {
	accumulators: StateCache<WindowStateKey, Accumulator>,
	meta: StateCache<MetaKey, GroupMeta<C>>,
	expiry: ExpiryIndex<TumblingIndexEntry<G, C>>,
	meta_low_water: Option<u64>,
	expire_batch: usize,
	hydrated: bool,
	group_scoped: bool,
	_pd: PhantomData<G>,
}

impl<G, C, Accumulator> TumblingEngine<G, C, Accumulator>
where
	G: Clone + Eq + Ord + Hash + Debug,
	C: WindowAnchor + Hash,
	Accumulator: WindowAccumulator,
	for<'a> &'a G: IntoEncodedKey,
	GroupMeta<C>: OperatorState,
	TumblingIndexEntry<G, C>: OperatorState,
{
	pub fn new(config: WindowEngineConfig) -> Self {
		Self::scoped(config, false)
	}

	pub fn group_scoped(config: WindowEngineConfig) -> Self {
		Self::scoped(config, true)
	}

	fn scoped(config: WindowEngineConfig, group_scoped: bool) -> Self {
		Self {
			accumulators: StateCache::<WindowStateKey, Accumulator>::new(),
			meta: StateCache::<MetaKey, GroupMeta<C>>::new(),
			expiry: ExpiryIndex::new(),
			meta_low_water: None,
			expire_batch: config.expire_batch(),
			hydrated: false,
			group_scoped,
			_pd: PhantomData,
		}
	}

	fn hydrate_once<S: StateStore + ?Sized>(&mut self, store: &mut S) -> Result<()> {
		if self.hydrated {
			return Ok(());
		}
		if !self.group_scoped {
			self.accumulators.hydrate(store, accumulator_range(), decode_window_state_key)?;
		}
		self.meta.hydrate(store, meta_range(), decode_meta_key)?;
		self.hydrated = true;
		Ok(())
	}

	#[allow(clippy::too_many_arguments)]
	pub fn reindex_window<S: StateStore + ?Sized>(
		&mut self,
		store: &mut S,
		group: &G,
		window_start: C,
		id: GroupId,
		slot: &EncodedKey,
		prior: Option<u64>,
		new: Option<u64>,
	) -> Result<()> {
		if prior == new {
			return Ok(());
		}
		let suffix = encode_u64(window_start.order_key().to_order());
		if let Some(old) = prior {
			self.expiry.drop_key(store, &expiry_key(old, group, &suffix))?;
		}
		if let Some(new) = new {
			let entry = TumblingIndexEntry {
				group: group.clone(),
				window_start,
				group_id: id.0,
				slot_key: slot.as_bytes().to_vec(),
			};
			self.expiry.set(store, expiry_key(new, group, &suffix), entry)?;
		}
		Ok(())
	}

	pub fn apply<S, K, NA>(
		&mut self,
		store: &mut S,
		buckets: TumblingBuckets<G, C, Accumulator::Contribution>,
		order: &[(G, WindowSpan<C>)],
		slot_key: K,
		new_accumulator: NA,
	) -> Result<Vec<WindowResult<G, C, Accumulator::Output>>>
	where
		S: StateStore + ?Sized,
		K: Fn(&G, C) -> (GroupId, EncodedKey),
		NA: Fn() -> Accumulator,
	{
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		self.hydrate_once(store)?;
		let mut meta_loaded = self.warm_and_load_meta(store, &buckets)?;
		let slot_resolved = self.resolve_slots(store, order, &slot_key)?;
		reifydb_assertions! {
			let ordered = slot_resolved.len();
			let bucketed = buckets.len();
			assert!(
				ordered == bucketed,
				"the resolution order must name every bucket exactly once; a bucket missing from it \
				 gets no row number and would be dropped from this batch, while a duplicate silently \
				 renumbers a window that already published under another row \
				 (order={ordered}, buckets={bucketed})"
			);
		}
		let results =
			self.apply_events(store, buckets, order, &slot_resolved, &mut meta_loaded, &new_accumulator)?;
		self.persist_meta(store, meta_loaded)?;
		Ok(results)
	}

	pub fn flush<S: StateStore + ?Sized>(&mut self, store: &mut S) -> Result<()> {
		self.accumulators.flush(store)?;
		self.meta.flush(store)?;
		Ok(())
	}

	fn warm_and_load_meta<S: StateStore + ?Sized>(
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

	fn resolve_slots<S, K>(
		&mut self,
		store: &mut S,
		order: &[(G, WindowSpan<C>)],
		slot_key: &K,
	) -> Result<SlotResolved<G, C>>
	where
		S: StateStore + ?Sized,
		K: Fn(&G, C) -> (GroupId, EncodedKey),
	{
		let mut resolved: SlotResolved<G, C> = HashMap::with_capacity(order.len());
		let mut resident: Vec<WindowStateKey> = Vec::with_capacity(order.len());
		for (group, span) in order {
			let (id, key) = slot_key(group, span.start);
			resident.push(WindowStateKey::new(id, key.clone()));
			resolved.insert(
				(group.clone(), *span),
				ResolvedSlot {
					group: id,
					key,
				},
			);
		}
		self.accumulators.warm(store, &resident)?;
		Ok(resolved)
	}

	fn apply_events<S, NA>(
		&mut self,
		store: &mut S,
		mut buckets: TumblingBuckets<G, C, Accumulator::Contribution>,
		order: &[(G, WindowSpan<C>)],
		slot_resolved: &SlotResolved<G, C>,
		meta_loaded: &mut MetaLoaded<G, C>,
		new_accumulator: &NA,
	) -> Result<Vec<WindowResult<G, C, Accumulator::Output>>>
	where
		S: StateStore + ?Sized,
		NA: Fn() -> Accumulator,
	{
		let mut results: Vec<WindowResult<G, C, Accumulator::Output>> = Vec::new();

		for ordered in order {
			let Some(events) = buckets.remove(ordered) else {
				continue;
			};
			let (group, span) = ordered.clone();
			meta_loaded.entry(group.clone()).or_default().observe(span.start);

			let Some(ResolvedSlot {
				group: id,
				key,
			}) = slot_resolved.get(&(group.clone(), span)).cloned()
			else {
				continue;
			};
			let state_key = WindowStateKey::new(id, key.clone());

			let mut accumulator: Accumulator =
				self.accumulators.get(store, &state_key)?.unwrap_or_else(new_accumulator);
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
			self.accumulators.put(store, &state_key, accumulator)?;

			match value {
				Some(value) => {
					let (row_number, is_new) = store.get_or_create_row_number(id, &key)?;
					let kind = if is_new {
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
						let (row_number, _is_new) = store.get_or_create_row_number(id, &key)?;
						reifydb_assertions! {
							assert!(
								!_is_new,
								"a window holding a prior output must already own its mapping; minting \
								 one here means the identity was released while the row it addresses \
								 was still live, and this withdrawal names a row no sink can find \
								 (group={id:?}, row={row_number:?})"
							);
						}
						store.remove_row_number(id, &key)?;
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
		reifydb_assertions! {
			assert!(
				buckets.is_empty(),
				"the resolution order must drain every bucket; a leftover bucket's events were \
				 silently dropped and its window never gets a row number (leftovers={})",
				buckets.len()
			);
		}
		Ok(results)
	}

	pub fn expire<S: StateStore + ?Sized>(
		&mut self,
		store: &mut S,
		threshold: u64,
	) -> Result<Vec<ExpiredWindow<G, C>>> {
		self.hydrate_once(store)?;
		let due = self.expiry.due(store, threshold, self.expire_batch)?;

		let mut out: Vec<ExpiredWindow<G, C>> = Vec::new();
		for (index_key, entry) in due {
			self.expiry.drop_key(store, &index_key)?;
			out.push(ExpiredWindow {
				group: entry.group,
				group_id: GroupId(entry.group_id),
				window_start: entry.window_start,
			});
		}
		note_when_expiry_capped(out.len(), self.expire_batch);
		Ok(out)
	}

	fn persist_meta<S: StateStore + ?Sized>(&mut self, store: &mut S, meta_loaded: MetaLoaded<G, C>) -> Result<()> {
		persist_batch_meta(store, &mut self.meta, meta_loaded)
	}

	pub fn expire_meta<S: StateStore + ?Sized>(&mut self, store: &mut S, threshold: u64) -> Result<usize> {
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
	use reifydb_core::{key::operator_state::GroupId, metrics::heap::HeapSize};
	use reifydb_macro::operator_state;
	use reifydb_value::{Result, factory::time::at_millis, value::datetime::DateTime};

	use crate::{
		state::seal::coord::Coord,
		testing::store::{MockStore, SumAccumulator},
		window::{
			accumulator::WindowAccumulator,
			engine::{
				AccumulatorEvent, EmitKind, GroupMeta, MetaHighWater, WindowResult,
				config::WindowEngineConfig,
				meta_key_for,
				tumbling::{TumblingBuckets, TumblingEngine},
			},
			span::WindowSpan,
		},
	};

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder().build()
	}

	fn row_key(group: &u32, window_start: DateTime) -> EncodedKey {
		EncodedKey::builder().u32(*group).u64(window_start.to_order()).build()
	}

	fn slot_key(group: &u32, window_start: DateTime) -> (GroupId, EncodedKey) {
		(GroupId::ROOT, row_key(group, window_start))
	}

	fn order_of<C>(buckets: &TumblingBuckets<u32, DateTime, C>) -> Vec<(u32, WindowSpan<DateTime>)> {
		buckets.keys().cloned().collect()
	}

	fn apply_sums(
		engine: &mut TumblingEngine<u32, DateTime, SumAccumulator>,
		store: &mut MockStore,
		buckets: TumblingBuckets<u32, DateTime, i64>,
	) -> Result<Vec<WindowResult<u32, DateTime, i64>>> {
		let order = order_of(&buckets);
		engine.apply(store, buckets, &order, slot_key, SumAccumulator::default)
	}

	fn apply_counting(
		engine: &mut TumblingEngine<u32, DateTime, CountingAcc>,
		store: &mut MockStore,
		buckets: TumblingBuckets<u32, DateTime, i64>,
	) -> Result<Vec<WindowResult<u32, DateTime, i64>>> {
		let order = order_of(&buckets);
		engine.apply(store, buckets, &order, slot_key, CountingAcc::default)
	}

	// Faces reindex through a long-lived engine, but these tests seed with throwaway ones, so the
	// write-through store copy is what the expiring engine's mirror hydrates from - the restart path.
	fn reindex_window(
		store: &mut MockStore,
		group: &u32,
		window_start: DateTime,
		prior: Option<u64>,
		new: Option<u64>,
	) -> Result<()> {
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		engine.reindex_window(
			store,
			group,
			window_start,
			GroupId::ROOT,
			&row_key(group, window_start),
			prior,
			new,
		)
	}

	fn seed_window(
		store: &mut MockStore,
		window_start: u64,
		contribution: i64,
	) -> WindowResult<u32, DateTime, i64> {
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert(
			(1u32, WindowSpan::new(at_millis(window_start), at_millis(window_start + 1))),
			vec![AccumulatorEvent::Add(contribution)],
		);
		let mut results = apply_sums(&mut engine, store, buckets).expect("apply");
		engine.flush(store).expect("flush");
		results.pop().expect("one window")
	}

	fn apply_event(store: &mut MockStore, window_start: u64, event: AccumulatorEvent<i64>) {
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert(
			(1u32, WindowSpan::new(at_millis(window_start), at_millis(window_start + 1))),
			vec![event],
		);
		apply_sums(&mut engine, store, buckets).expect("apply");
		engine.flush(store).expect("flush");
	}

	fn group_slot(group: &u32, window_start: DateTime) -> (GroupId, EncodedKey) {
		// The shape a sub-flow window driver installs: every window interns as its own
		// (partition, coord) group sharing one empty row key, so the group alone separates them.
		(GroupId(u64::from(*group) * 1_000_000 + window_start.to_order()), EncodedKey::new(Vec::new()))
	}

	fn apply_group_scoped(
		engine: &mut TumblingEngine<u32, DateTime, SumAccumulator>,
		store: &mut MockStore,
		buckets: TumblingBuckets<u32, DateTime, i64>,
	) -> Vec<WindowResult<u32, DateTime, i64>> {
		let order = order_of(&buckets);
		let out = engine.apply(store, buckets, &order, group_slot, SumAccumulator::default).expect("apply");
		engine.flush(store).expect("flush");
		out
	}

	fn one_bucket(group: u32, window_start: u64, contribution: i64) -> TumblingBuckets<u32, DateTime, i64> {
		let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert(
			(group, WindowSpan::new(at_millis(window_start), at_millis(window_start + 1))),
			vec![AccumulatorEvent::Add(contribution)],
		);
		buckets
	}

	#[test]
	fn group_scoped_windows_keep_separate_state_under_one_shared_row_key() {
		// A group-scoped driver addresses every window with the same empty row key and leans on the
		// group to separate them. If the group fell out of the accumulator key or the row-number
		// lookup, every window of the operator would fold into one accumulator under one row.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::group_scoped(test_config());

		let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(at_millis(0), at_millis(1))), vec![AccumulatorEvent::Add(5)]);
		buckets.insert((1u32, WindowSpan::new(at_millis(100), at_millis(101))), vec![AccumulatorEvent::Add(7)]);
		buckets.insert((2u32, WindowSpan::new(at_millis(0), at_millis(1))), vec![AccumulatorEvent::Add(9)]);
		let results = apply_group_scoped(&mut engine, &mut store, buckets);

		assert_eq!(results.len(), 3);
		let mut rows: Vec<u64> = results.iter().map(|r| r.row_number.0).collect();
		rows.sort_unstable();
		rows.dedup();
		assert_eq!(rows.len(), 3, "each window must own a row of its own despite the shared row key");
		let mut values: Vec<i64> = results.iter().map(|r| r.value).collect();
		values.sort_unstable();
		assert_eq!(values, vec![5, 7, 9], "no window may see another's contributions");

		// A restart reloads one window's accumulator; it must be that window's alone.
		let mut restarted = TumblingEngine::<u32, DateTime, SumAccumulator>::group_scoped(test_config());
		let out = apply_group_scoped(&mut restarted, &mut store, one_bucket(1, 0, 1));
		assert_eq!(out[0].value, 6, "the reloaded accumulator carries only window (1, 0)");
	}

	#[test]
	fn an_expired_window_names_the_group_its_state_lived_in() {
		// The expiry index lives in the root group ordered by due time, so the group rides in the key tail and
		// the entry - the driver needs that id to drop the per-window meta and release the row number, or it
		// strands both or erases another group's identity.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::group_scoped(test_config());
		let results = apply_group_scoped(&mut engine, &mut store, one_bucket(3, 40, 5));
		let published = &results[0];
		let (group, _) = group_slot(&published.group, published.span.start);
		let (_, slot) = group_slot(&published.group, published.span.start);
		engine.reindex_window(&mut store, &published.group, published.span.start, group, &slot, None, Some(10))
			.unwrap();

		let mut restarted = TumblingEngine::<u32, DateTime, SumAccumulator>::group_scoped(test_config());
		let expired = restarted.expire(&mut store, 10).unwrap();

		assert_eq!(expired.len(), 1);
		assert_eq!(expired[0].group_id, group, "the entry must name the group whose state it drained");
		assert!(
			store.contains_row_mapping(group, &EncodedKey::new(Vec::new())),
			"the identity the driver is about to release must still be resolvable from that group alone"
		);
		assert!(
			store.contains_row_mapping(group, &EncodedKey::new(Vec::new())),
			"sealing releases no identity of its own; the reaper collects it at or below the ledger"
		);
	}

	#[test]
	fn expire_returns_only_due_windows_and_drops_only_their_index_entries() {
		let mut store = MockStore::default();
		// Two live windows; the face indexes each by its last_event_time (10 and 90).
		let w0 = seed_window(&mut store, 0, 5);
		reindex_window(&mut store, &w0.group, w0.span.start, None, Some(10)).unwrap();
		let w100 = seed_window(&mut store, 100, 7);
		reindex_window(&mut store, &w100.group, w100.span.start, None, Some(90)).unwrap();
		assert_eq!(store.index_entry_count(), 2, "both live windows are indexed");

		// Threshold 10: only the window whose expiry (10) is at/under the threshold is due.
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let expired = engine.expire(&mut store, 10).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(expired.len(), 1, "exactly one window is due, not the whole population");
		assert_eq!(expired[0].window_start, at_millis(0));
		assert_eq!(store.index_entry_count(), 1, "the due window's index entry is gone, the other remains");

		// Expiry is the seal's own index and nothing else: the accumulators behind both windows are
		// still on disk here, and the reaper is what collects them at or below the ledger.
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let later = engine.expire(&mut store, 1000).unwrap();
		assert_eq!(later.len(), 1);
		assert_eq!(later[0].window_start, at_millis(100));
		assert_eq!(store.index_entry_count(), 0);
	}

	#[test]
	fn an_expiry_entry_whose_state_was_reclaimed_still_drains() {
		// The expiry index is ordered by due time, so a group's entries sit outside its key range
		// and survive reclamation of its data, left to drain on their own. An entry that refused to
		// drop because the state behind it was gone would sit in the index forever.
		let mut store = MockStore::default();
		let w = seed_window(&mut store, 0, 5);
		reindex_window(&mut store, &w.group, w.span.start, None, Some(10)).unwrap();
		assert_eq!(store.index_entry_count(), 1, "precondition: the window is indexed");
		assert_eq!(store.drop_accumulator_entries(), 1, "precondition: the reaper erased the accumulator");

		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let expired = engine.expire(&mut store, 10).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(expired.len(), 1, "the stale entry must still drain, or the index would grow forever");
		assert_eq!(store.index_entry_count(), 0, "a stale entry drops itself on the scan that finds it");
	}

	#[test]
	fn a_window_whose_accumulator_was_reclaimed_updates_its_row_rather_than_inserting_a_second() {
		// Phase-1 reclamation erases the accumulator and keeps the row-number mapping, so the
		// published row stays addressable and a later batch must update it in place. A second
		// insert lays a duplicate row over a live one, which a sink folding the stream cannot place.
		let mut store = MockStore::default();
		let published = seed_window(&mut store, 0, 5);
		assert_eq!(store.drop_accumulator_entries(), 1, "precondition: reclaim erased the accumulator");
		assert!(
			store.contains_row_mapping(GroupId::ROOT, &row_key(&1, at_millis(0))),
			"precondition: the identity half must survive the data phase"
		);

		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let results = apply_sums(&mut engine, &mut store, one_bucket(1, 0, 3)).expect("apply");

		assert_eq!(results.len(), 1);
		assert_eq!(results[0].row_number, published.row_number, "the woken window keeps the row it published");
		assert_eq!(
			results[0].kind,
			EmitKind::Update,
			"the published row survived the sweep, so this is an update and not a second insert"
		);
	}

	#[test]
	fn a_window_that_publishes_nothing_mints_no_identity() {
		// Minting a row number before the publish decision leaves a mapping addressing a row the
		// view never held. The window then looks published, so its next value goes out as an update
		// whose pre-image is absent.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());

		let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(at_millis(0), at_millis(1))), vec![AccumulatorEvent::Remove(5)]);
		let results = apply_sums(&mut engine, &mut store, buckets).expect("apply");
		engine.flush(&mut store).expect("flush");

		assert!(results.is_empty(), "a window that finalizes to nothing publishes nothing");
		assert!(
			!store.contains_row_mapping(GroupId::ROOT, &row_key(&1, at_millis(0))),
			"and must leave no identity behind for a row it never published"
		);

		let mut woken = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let out = apply_sums(&mut woken, &mut store, one_bucket(1, 0, 4)).expect("apply");

		assert_eq!(out[0].kind, EmitKind::Insert, "the first row this window publishes is an insert");
	}

	#[test]
	fn an_emptied_window_seals_without_releasing_anything() {
		// An accumulator drained to zero still exists and still owns its mapping, but finalizes to
		// none - indistinguishable by value from a group the reaper erased. Sealing releases no
		// identity at all, so the distinction it used to depend on cannot be got wrong.
		let mut store = MockStore::default();
		let w = seed_window(&mut store, 0, 5);
		apply_event(&mut store, 0, AccumulatorEvent::Remove(5));
		reindex_window(&mut store, &w.group, w.span.start, None, Some(10)).unwrap();

		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let expired = engine.expire(&mut store, 10).unwrap();
		engine.flush(&mut store).unwrap();

		assert_eq!(expired.len(), 1);
		assert_eq!(
			store.drop_accumulator_entries(),
			1,
			"the emptied accumulator outlives the seal and is the reaper's to collect"
		);
	}

	#[test]
	fn meta_reclaimed_when_group_stale_past_threshold() {
		// A group whose high water falls below the staleness threshold has stopped advancing and its
		// GroupMeta must be reclaimed. `persist_meta` writes one meta per group and never removes
		// it, so without the sweep one internal-state key leaks per distinct group forever.
		let mut store = MockStore::default();
		seed_window(&mut store, 0, 5);
		assert_eq!(store.meta_entry_count(), 1, "applying a window persisted the group's meta");

		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let dropped = engine.expire_meta(&mut store, 100).unwrap();
		assert_eq!(dropped, 1, "the group's high water (0) is below the threshold (100)");
		assert_eq!(store.meta_entry_count(), 0, "a stale group must not leak its GroupMeta");
	}

	#[test]
	fn meta_survives_while_group_high_water_at_or_after_threshold() {
		// A group whose high water is at or beyond the threshold is still live, its late-event
		// horizon not yet passed, and must keep its meta.
		let mut store = MockStore::default();
		seed_window(&mut store, 100, 7);
		assert_eq!(store.meta_entry_count(), 1);

		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let dropped = engine.expire_meta(&mut store, 50).unwrap();
		assert_eq!(dropped, 0, "high water (100) is not below the threshold (50)");
		assert_eq!(store.meta_entry_count(), 1, "a group within the staleness horizon keeps its meta");
	}

	#[test]
	fn meta_sweep_leaves_row_number_mappings_intact() {
		// The sweep targets only meta keys and must not touch the write-once row-number mappings
		// that share the same tier; deleting those corrupts the operator.
		let mut store = MockStore::default();
		seed_window(&mut store, 0, 5);
		store.seed_mapping_key(0x01);
		assert_eq!(store.mapping_entry_count(), 1);

		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		engine.expire_meta(&mut store, 100).unwrap();
		assert_eq!(store.meta_entry_count(), 0, "the stale group's meta is swept");
		assert_eq!(store.mapping_entry_count(), 1, "the sweep must not touch row-number mapping keys");
	}

	#[test]
	fn meta_sweep_skips_then_reclaims_as_threshold_advances() {
		// The low-water guard skips the scan while the smallest high water is at or above the
		// threshold, but must still reclaim once the threshold advances past it: it is an
		// optimization to avoid scanning every apply, never a correctness hole.
		let mut store = MockStore::default();
		seed_window(&mut store, 100, 7);

		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
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
		reindex_window(&mut store, &w.group, w.span.start, None, Some(50)).unwrap();

		// One below the expiry: not due, and the scan leaves the index intact.
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		assert!(engine.expire(&mut store, 49).unwrap().is_empty());
		engine.flush(&mut store).unwrap();
		assert_eq!(store.index_entry_count(), 1);

		// Exactly at the expiry: due (the face folds the strict close boundary into the threshold).
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		assert_eq!(engine.expire(&mut store, 50).unwrap().len(), 1);
	}

	#[test]
	fn expire_processes_at_most_expire_batch_then_resumes_next_tick() {
		// Node ticks run serialized, so draining every due window in one tick lets a burst on one
		// bloated operator stall the whole actor pass. The cap bounds one tick and the remainder
		// stays in the due index, which sorts by inverted expiry so the oldest backlog defers.
		let mut store = MockStore::default();
		for (start, due) in [(0u64, 10u64), (100, 20), (200, 30)] {
			let w = seed_window(&mut store, start, 1);
			reindex_window(&mut store, &w.group, w.span.start, None, Some(due)).unwrap();
		}
		assert_eq!(store.index_entry_count(), 3);

		let capped = WindowEngineConfig::builder().expire_batch(2).build();

		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(capped.clone());
		let first = engine.expire(&mut store, 1000).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(first.len(), 2, "one tick drains at most expire_batch windows");
		assert_eq!(first[0].window_start, at_millis(200), "inverted key order: newest due drains first");
		assert_eq!(first[1].window_start, at_millis(100));
		assert_eq!(store.index_entry_count(), 1, "the deferred window keeps its index entry");

		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(capped);
		let second = engine.expire(&mut store, 1000).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(second.len(), 1, "the next tick picks up the deferred backlog");
		assert_eq!(second[0].window_start, at_millis(0));
		assert_eq!(store.index_entry_count(), 0);
	}

	#[test]
	fn reindex_rekeys_without_leaving_a_stale_entry() {
		let mut store = MockStore::default();
		let w = seed_window(&mut store, 0, 9);
		// Index at 10, then a later event advances the window's expiry to 80.
		reindex_window(&mut store, &w.group, w.span.start, None, Some(10)).unwrap();
		reindex_window(&mut store, &w.group, w.span.start, Some(10), Some(80)).unwrap();
		assert_eq!(store.index_entry_count(), 1, "re-keying must not leave the old entry behind");

		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		assert!(engine.expire(&mut store, 10).unwrap().is_empty(), "no longer due at the old expiry");
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		assert_eq!(engine.expire(&mut store, 80).unwrap().len(), 1, "due at the new expiry");
	}

	#[test]
	fn accumulator_survives_restart() {
		// A window emptying under retraction emits a terminal Remove carrying the accumulator's
		// pre-batch finalize, read back from the store. Dropping the engine between publish and
		// retraction forces that read rather than serving it from the in-memory cache.
		let mut store = MockStore::default();

		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(at_millis(0), at_millis(1))), vec![AccumulatorEvent::Add(5)]);
		let published: Vec<WindowResult<u32, DateTime, i64>> =
			apply_sums(&mut engine, &mut store, buckets).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(published.len(), 1);
		assert!(matches!(published[0].kind, EmitKind::Insert));
		assert_eq!(published[0].value, 5);

		// A brand new engine with empty caches, forced to read the persisted accumulator back.
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(at_millis(0), at_millis(1))), vec![AccumulatorEvent::Remove(5)]);
		let withdrawn: Vec<WindowResult<u32, DateTime, i64>> =
			apply_sums(&mut engine, &mut store, buckets).unwrap();
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
		// The other way a read reaches the store is LRU eviction, with no restart: the accumulator
		// cache holds 8 windows, so more than that evicts the oldest and the next access re-reads it.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());

		let mut published_group_1: Vec<WindowResult<u32, DateTime, i64>> = Vec::new();
		for group in 1u32..=11u32 {
			let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
			buckets.insert(
				(group, WindowSpan::new(at_millis(0), at_millis(1))),
				vec![AccumulatorEvent::Add(i64::from(group))],
			);
			let out: Vec<WindowResult<u32, DateTime, i64>> =
				apply_sums(&mut engine, &mut store, buckets).unwrap();
			if group == 1 {
				published_group_1 = out;
			}
		}
		engine.flush(&mut store).unwrap();
		assert_eq!(published_group_1.len(), 1);
		assert!(matches!(published_group_1[0].kind, EmitKind::Insert));
		assert_eq!(published_group_1[0].value, 1);

		// Group 1's window was pushed out of the 8-slot cache by the later groups, so the same
		// engine must re-read its accumulator from the store to apply this retraction.
		let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(at_millis(0), at_millis(1))), vec![AccumulatorEvent::Remove(1)]);
		let withdrawn: Vec<WindowResult<u32, DateTime, i64>> =
			apply_sums(&mut engine, &mut store, buckets).unwrap();
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

	// Counts clones of the probe accumulator, so a test can prove expire() finalizes from the
	// cache-resident view instead of deep-cloning.
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
	fn expire_touches_no_accumulator_on_either_residency_path() {
		// expire() is the seal's index scan and nothing more: the seal publishes nothing, so it must
		// read no accumulator on either the same-engine Native path or the store-loaded archived
		// one, and the clone counter must stay at zero for both.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, DateTime, CountingAcc>::new(test_config());
		let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(at_millis(0), at_millis(1))), vec![AccumulatorEvent::Add(5)]);
		buckets.insert((1u32, WindowSpan::new(at_millis(100), at_millis(101))), vec![AccumulatorEvent::Add(7)]);
		let results = apply_counting(&mut engine, &mut store, buckets).unwrap();
		engine.flush(&mut store).unwrap();
		assert_eq!(results.len(), 2);
		for w in &results {
			let expiry = if w.span.start == at_millis(0) {
				10
			} else {
				90
			};
			reindex_window(&mut store, &w.group, w.span.start, None, Some(expiry)).unwrap();
		}

		let before = COUNTING_ACC_CLONES.load(Ordering::SeqCst);

		// Same engine: the window-0 accumulator is clean Native after flush.
		let expired = engine.expire(&mut store, 10).unwrap();
		assert_eq!(expired.len(), 1);
		assert_eq!(expired[0].window_start, at_millis(0), "Native-resident path");

		// Fresh engine: the window-100 entry would have loaded from the store and finalized through
		// the archived view.
		let mut fresh = TumblingEngine::<u32, DateTime, CountingAcc>::new(test_config());
		let expired = fresh.expire(&mut store, 1000).unwrap();
		assert_eq!(expired.len(), 1);
		assert_eq!(expired[0].window_start, at_millis(100), "archived path");

		assert_eq!(
			COUNTING_ACC_CLONES.load(Ordering::SeqCst) - before,
			0,
			"expire must not clone accumulators on either the Native or the archived path"
		);
	}

	fn read_high_water(
		engine: &mut TumblingEngine<u32, DateTime, SumAccumulator>,
		store: &mut MockStore,
		group: u32,
	) -> Option<u64> {
		engine.meta.get(store, &meta_key_for(&group)).unwrap().and_then(|meta| meta.high_water_order())
	}

	#[test]
	fn warmed_meta_high_water_advances_across_engine_restarts() {
		// A bump applied by one engine must be visible to the next, or a restart replays late events.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(at_millis(100), at_millis(101))), vec![AccumulatorEvent::Add(5)]);
		apply_sums(&mut engine, &mut store, buckets).unwrap();
		engine.flush(&mut store).unwrap();

		let mut fresh = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(at_millis(200), at_millis(201))), vec![AccumulatorEvent::Add(7)]);
		apply_sums(&mut fresh, &mut store, buckets).unwrap();

		assert_eq!(
			read_high_water(&mut fresh, &mut store, 1),
			Some(200),
			"the bump must be durable the moment it is applied"
		);
		fresh.flush(&mut store).unwrap();

		let mut third = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		assert_eq!(
			read_high_water(&mut third, &mut store, 1),
			Some(200),
			"the write round-trips through the store"
		);
	}

	#[test]
	fn a_persisted_none_high_water_still_accepts_a_bump() {
		// Retraction-only groups persist a none high water; refusing to advance it strands them.
		let mut store = MockStore::default();
		let mut engine = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
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

		let mut fresh = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		let mut buckets: TumblingBuckets<u32, DateTime, i64> = BTreeMap::new();
		buckets.insert((1u32, WindowSpan::new(at_millis(100), at_millis(101))), vec![AccumulatorEvent::Add(7)]);
		apply_sums(&mut fresh, &mut store, buckets).unwrap();
		fresh.flush(&mut store).unwrap();

		let mut third = TumblingEngine::<u32, DateTime, SumAccumulator>::new(test_config());
		assert_eq!(
			read_high_water(&mut third, &mut store, 1),
			Some(100),
			"a none high water must advance to the first observed coordinate"
		);
	}
}
