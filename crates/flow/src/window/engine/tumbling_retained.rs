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
	row::operator::state::{OperatorState, StateCodec, decode, encode},
};
use reifydb_core::{
	key::operator::{
		keyspace::expiry::TumblingExpiry,
		state::{GroupId, GroupStateKey, KeyspaceId, keyspace_inner_range},
	},
	metrics::heap::HeapSize,
	state::timer::StateStore,
};
use reifydb_macro::operator_state;
use reifydb_value::{Result, reifydb_assertions, value::row_number::RowNumber};

#[cfg(reifydb_assertions)]
use crate::operator::state::expiry::expiry_all;
use crate::{
	operator::{
		state::{
			expiry::{ExpiryIndex, expiry_drop, tumbling_expiry_key},
			reaper::Reaper,
		},
		state_access::{get_classified, put, remove},
	},
	window::{
		engine::{
			AccumulatorEvent, BatchMeta, EmitKind, GroupMeta, KeyspaceFamily, MetaSweep, RetainedEntryKey,
			WindowStateKey,
			config::WindowEngineConfig,
			group_hash, load_batch_meta, meta_key_for, note_when_expiry_capped, persist_batch_meta,
			tumbling::{ExpiredWindow, TumblingBuckets, TumblingIndexEntry},
		},
		span::{WindowAnchor, WindowSpan},
	},
};

type MetaLoaded<G, S> = HashMap<G, BatchMeta<S>>;

#[derive(Clone)]
struct ResolvedSlot {
	group: GroupId,
	key: EncodedKey,
}

type SlotResolved<G, S> = HashMap<(G, WindowSpan<S>), ResolvedSlot>;

struct PendingEmit<G, S> {
	group_id: GroupId,
	group: G,
	span: WindowSpan<S>,
	withdraw: bool,
}

pub struct RetainedResult<G, S> {
	pub row_number: RowNumber,
	pub group: G,
	pub group_id: GroupId,
	pub span: WindowSpan<S>,
	pub kind: EmitKind,
}

#[operator_state]
#[derive(Debug, Clone, Default, PartialEq, HeapSize)]
struct RetainedHeader {
	count: u64,
}

#[operator_state]
#[derive(Debug, Clone, PartialEq)]
pub struct RetainedEntry<K, V> {
	key: K,
	value: V,
}

impl<K: HeapSize, V: HeapSize> HeapSize for RetainedEntry<K, V> {
	fn heap_size(&self) -> usize {
		self.key.heap_size() + self.value.heap_size()
	}
}

impl<G, S, K, V> Reaper for RetainedTumblingEngine<G, S, K, V>
where
	S: WindowAnchor + Hash,
{
	fn reap(&mut self, store: &mut dyn StateStore, key: &GroupStateKey) -> Result<()> {
		store.state_remove(key)
	}
}

pub struct RetainedTumblingEngine<G, S, K, V> {
	meta_sweep: MetaSweep,
	expire_batch: usize,
	dropped_retractions: u64,
	expiry: ExpiryIndex<TumblingExpiry>,
	_pd: PhantomData<(G, S, K, V)>,
}

impl<G, S, K, V> RetainedTumblingEngine<G, S, K, V>
where
	G: Clone + Eq + Ord + Hash + Debug,
	S: WindowAnchor + Hash,
	K: Ord + Clone + Debug + HeapSize,
	V: Clone + Debug + PartialEq + HeapSize,
	G: StateCodec,
	K: StateCodec,
	GroupMeta<S>: OperatorState,
	TumblingIndexEntry<G, S>: OperatorState,
	RetainedEntry<K, V>: OperatorState,
{
	pub fn new(config: WindowEngineConfig) -> Self {
		Self {
			meta_sweep: MetaSweep::default(),
			expire_batch: config.expire_batch(),
			dropped_retractions: 0,
			expiry: ExpiryIndex::default(),
			_pd: PhantomData,
		}
	}

	pub fn dropped_retractions(&self) -> u64 {
		self.dropped_retractions
	}

	#[allow(clippy::too_many_arguments)]
	pub fn reindex_window(
		&mut self,
		store: &mut dyn StateStore,
		group: &G,
		window_start: S,
		id: GroupId,
		slot_key: &EncodedKey,
		prior: Option<u64>,
		new: Option<u64>,
	) -> Result<()> {
		if prior == new {
			return Ok(());
		}
		let order = window_start.order_key().to_order();
		if let Some(old) = prior {
			expiry_drop(store, &tumbling_expiry_key(old, group_hash(group)?, order))?;
		}
		if let Some(new) = new {
			let entry = TumblingIndexEntry {
				group: group.clone(),
				window_start,
				group_id: id,
				slot_key: slot_key.as_bytes().to_vec(),
			};
			self.expiry.set(store, tumbling_expiry_key(new, group_hash(group)?, order), entry)?;
		}
		Ok(())
	}

	pub fn apply<SK>(
		&mut self,
		store: &mut dyn StateStore,
		buckets: TumblingBuckets<G, S, (K, V)>,
		order: &[(G, WindowSpan<S>)],
		slot_key: SK,
	) -> Result<Vec<RetainedResult<G, S>>>
	where
		SK: Fn(&G, S) -> (GroupId, EncodedKey),
	{
		self.dropped_retractions = 0;
		if buckets.is_empty() {
			return Ok(Vec::new());
		}
		let mut meta_loaded = self.load_meta(store, &buckets)?;
		let slot_resolved = Self::resolve_slots(order, &slot_key);
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
		let results = self.apply_events(store, buckets, order, &slot_resolved, &mut meta_loaded)?;
		self.persist_meta(store, meta_loaded)?;
		Ok(results)
	}

	pub fn load_entries(&self, store: &mut dyn StateStore, group_id: GroupId) -> Result<BTreeMap<K, V>> {
		let range = keyspace_inner_range(group_id, KeyspaceId::GUEST_RETAINED_ENTRY);
		let mut entries = BTreeMap::new();
		for (_, row) in store.state_page(range, None)? {
			let entry: RetainedEntry<K, V> = decode(&row)?;
			entries.insert(entry.key, entry.value);
		}
		Ok(entries)
	}

	fn entry_key(&self, group: GroupId, key: &K) -> Result<RetainedEntryKey> {
		Ok(RetainedEntryKey::new(group, EncodedKey::new(encode(key)?.body().to_vec())))
	}

	fn load_meta(
		&mut self,
		store: &mut dyn StateStore,
		buckets: &TumblingBuckets<G, S, (K, V)>,
	) -> Result<MetaLoaded<G, S>> {
		let mut meta_loaded: MetaLoaded<G, S> = HashMap::new();
		for (group, _) in buckets.keys() {
			if !meta_loaded.contains_key(group) {
				let batch = load_batch_meta(store, &meta_key_for(group_hash(group)?))?;
				meta_loaded.insert(group.clone(), batch);
			}
		}
		Ok(meta_loaded)
	}

	fn resolve_slots<SK>(order: &[(G, WindowSpan<S>)], slot_key: &SK) -> SlotResolved<G, S>
	where
		SK: Fn(&G, S) -> (GroupId, EncodedKey),
	{
		let mut resolved: SlotResolved<G, S> = HashMap::with_capacity(order.len());
		for (group, span) in order {
			let (id, key) = slot_key(group, span.start);
			resolved.insert(
				(group.clone(), *span),
				ResolvedSlot {
					group: id,
					key,
				},
			);
		}
		resolved
	}

	fn apply_events(
		&mut self,
		store: &mut dyn StateStore,
		mut buckets: TumblingBuckets<G, S, (K, V)>,
		order: &[(G, WindowSpan<S>)],
		slot_resolved: &SlotResolved<G, S>,
		meta_loaded: &mut MetaLoaded<G, S>,
	) -> Result<Vec<RetainedResult<G, S>>> {
		let mut pending: Vec<PendingEmit<G, S>> = Vec::new();

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
			let state_key = WindowStateKey::new(KeyspaceFamily::Guest, id, key);

			let mut header: RetainedHeader = get_classified(store, &state_key)?.unwrap_or_default();
			let was_empty = header.count == 0;

			for event in events {
				match event {
					AccumulatorEvent::Add((k, v)) => {
						let entry_key = self.entry_key(id, &k)?;
						let old: Option<RetainedEntry<K, V>> = get_classified(store, &entry_key)?;
						put(
							store,
							&entry_key,
							RetainedEntry {
								key: k,
								value: v,
							},
						)?;
						if old.is_none() {
							header.count += 1;
						}
					}
					AccumulatorEvent::Remove((k, v)) => {
						if header.count == 0 {
							self.dropped_retractions += 1;
							continue;
						}
						let entry_key = self.entry_key(id, &k)?;
						let old: Option<RetainedEntry<K, V>> = get_classified(store, &entry_key)?;
						if let Some(entry) = old
							&& entry.value == v
						{
							remove(store, &entry_key)?;
							header.count -= 1;
						}
					}
				}
			}

			let is_empty = header.count == 0;
			put(store, &state_key, header)?;

			if !is_empty || !was_empty {
				pending.push(PendingEmit {
					group_id: id,
					group,
					span,
					withdraw: is_empty,
				});
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

		let groups: Vec<GroupId> = pending.iter().map(|p| p.group_id).collect();
		let rows = store.get_or_create_row_numbers_for_groups(&groups)?;
		reifydb_assertions! {
			let requested = groups.len();
			let returned = rows.len();
			assert!(
				returned == requested,
				"the identity batch must return one row per publishing window; a short batch makes the \
				 zip below drop the tail, so those windows publish nothing while their entries \
				 already advanced (requested={requested}, returned={returned})"
			);
		}

		let mut results: Vec<RetainedResult<G, S>> = Vec::with_capacity(pending.len());
		for (emit, (row_number, is_new)) in pending.into_iter().zip(rows) {
			let kind = if emit.withdraw {
				reifydb_assertions! {
					let group_id = emit.group_id;
					assert!(
						!is_new,
						"a window holding published entries must already own its mapping; minting \
						 one here means the identity was released while the row it addresses \
						 was still live, and this withdrawal names a row no sink can find \
						 (group={group_id:?}, row={row_number:?})"
					);
				}
				store.remove_row_number_for_group(emit.group_id)?;
				EmitKind::Remove
			} else if is_new {
				EmitKind::Insert
			} else {
				EmitKind::Update
			};
			results.push(RetainedResult {
				row_number,
				group: emit.group,
				group_id: emit.group_id,
				span: emit.span,
				kind,
			});
		}
		Ok(results)
	}

	pub fn expire(&mut self, store: &mut dyn StateStore, threshold: u64) -> Result<Vec<ExpiredWindow<G, S>>> {
		let due: Vec<(GroupStateKey, TumblingIndexEntry<G, S>)> =
			self.expiry.due(store, threshold, self.expire_batch)?;

		let mut out: Vec<ExpiredWindow<G, S>> = Vec::new();
		for (index_key, entry) in due {
			expiry_drop(store, &index_key)?;
			out.push(ExpiredWindow {
				group: entry.group,
				group_id: entry.group_id,
				window_start: entry.window_start,
			});
		}
		self.expiry.settle(store)?;
		reifydb_assertions! {
			for entry in expiry_all::<TumblingExpiry, TumblingIndexEntry<G, S>>(store)? {
				assert!(
					!out.iter().any(|window| window.group_id == entry.group_id),
					"the expiry index still holds a row for a group that is about to be queued for \
					 reaping; a window group must own exactly one index row, or reaping it orphans \
					 the rows left behind under a group id nothing resolves again"
				);
			}
		}
		note_when_expiry_capped(out.len(), self.expire_batch);
		Ok(out)
	}

	pub fn earliest_expiry(&mut self, store: &mut dyn StateStore) -> Result<Option<u64>> {
		self.expiry.earliest(store)
	}

	fn persist_meta(&mut self, store: &mut dyn StateStore, meta_loaded: MetaLoaded<G, S>) -> Result<()> {
		persist_batch_meta(store, meta_loaded)
	}

	pub fn expire_meta(&mut self, store: &mut dyn StateStore, threshold: u64) -> Result<usize> {
		self.meta_sweep.sweep::<GroupMeta<S>>(store, threshold)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use reifydb_codec::key::encoded::EncodedKey;
	use reifydb_core::key::operator::state::GroupId;
	use reifydb_value::{factory::time::at_millis, util::hash::Hash128, value::datetime::DateTime};

	use super::{RetainedHeader, RetainedResult, RetainedTumblingEngine};
	use crate::{
		operator::{
			state::{
				mock::MockStore,
				reaper::{StoreReaper, enqueue, reap_group},
			},
			state_access::get,
		},
		window::{
			accumulator::invertible::retained_map::RetainedAccumulator,
			engine::{
				AccumulatorEvent::{self, Add, Remove},
				EmitKind, KeyspaceFamily, WindowResult, WindowStateKey,
				config::WindowEngineConfig,
				tumbling::{TumblingBuckets, TumblingEngine},
			},
			span::WindowSpan,
		},
	};

	type Engine = RetainedTumblingEngine<u32, DateTime, u64, i64>;
	type Blob = TumblingEngine<u32, DateTime, RetainedAccumulator<u64, i64>>;
	type Event = AccumulatorEvent<(u64, i64)>;

	fn test_config() -> WindowEngineConfig {
		WindowEngineConfig::builder().family(KeyspaceFamily::Guest).build()
	}

	fn group_slot(group: &u32, window_start: DateTime) -> (GroupId, EncodedKey) {
		(GroupId::window(Hash128(u128::from(*group)), window_start.to_order()), EncodedKey::new(Vec::new()))
	}

	fn window_group() -> GroupId {
		group_slot(&1, at_millis(0)).0
	}

	fn bucket(events: Vec<Event>) -> TumblingBuckets<u32, DateTime, (u64, i64)> {
		BTreeMap::from([((1u32, WindowSpan::new(at_millis(0), at_millis(10))), events)])
	}

	fn apply(engine: &mut Engine, store: &mut MockStore, events: Vec<Event>) -> Vec<RetainedResult<u32, DateTime>> {
		let buckets = bucket(events);
		let order: Vec<_> = buckets.keys().cloned().collect();
		engine.apply(store, buckets, &order, group_slot).expect("apply")
	}

	fn apply_blob(
		engine: &mut Blob,
		store: &mut MockStore,
		events: Vec<Event>,
	) -> Vec<WindowResult<u32, DateTime, BTreeMap<u64, i64>>> {
		let buckets = bucket(events);
		let order: Vec<_> = buckets.keys().cloned().collect();
		engine.apply(store, buckets, &order, group_slot, RetainedAccumulator::default).expect("apply")
	}

	fn kinds(results: &[RetainedResult<u32, DateTime>]) -> Vec<EmitKind> {
		results.iter().map(|r| r.kind).collect()
	}

	#[test]
	fn parity_with_blob_engine_over_adds_updates_removes() {
		// The per-entry engine replaces the blob engine, so any drift in row number, emit kind or entries is a wrong row downstream.
		let batches: Vec<fn() -> Vec<Event>> = vec![
			|| vec![Add((1, 10)), Add((2, 20))],
			|| vec![Add((1, 11)), Remove((2, 20))],
			|| vec![Remove((1, 11))],
			|| vec![Add((3, 30))],
		];
		let (mut entries_store, mut blob_store) = (MockStore::default(), MockStore::default());
		let mut engine = Engine::new(test_config());
		let mut blob = Blob::new(test_config());
		for (step, batch) in batches.iter().enumerate() {
			let ours = apply(&mut engine, &mut entries_store, batch());
			let theirs = apply_blob(&mut blob, &mut blob_store, batch());
			assert_eq!(ours.len(), 1, "step {step}: one window publishes");
			assert_eq!(theirs.len(), 1, "step {step}: one window publishes");
			assert_eq!(ours[0].row_number, theirs[0].row_number, "step {step}");
			assert_eq!(ours[0].kind, theirs[0].kind, "step {step}");
			let expected = match theirs[0].kind {
				EmitKind::Remove => BTreeMap::new(),
				_ => theirs[0].value.clone(),
			};
			assert_eq!(engine.load_entries(&mut entries_store, window_group()).unwrap(), expected, "step {step}");
		}
	}

	#[test]
	fn overwriting_a_key_keeps_count() {
		// Counting an overwrite as a new entry keeps an emptied window alive, so its removal never publishes.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		assert_eq!(kinds(&apply(&mut engine, &mut store, vec![Add((1, 10))])), vec![EmitKind::Insert]);
		assert_eq!(kinds(&apply(&mut engine, &mut store, vec![Add((1, 99))])), vec![EmitKind::Update]);
		assert_eq!(kinds(&apply(&mut engine, &mut store, vec![Remove((1, 99))])), vec![EmitKind::Remove]);
	}

	#[test]
	fn removing_a_superseded_value_keeps_the_entry() {
		// A retraction of an overwritten value must not delete the newer value that replaced it.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		apply(&mut engine, &mut store, vec![Add((1, 10))]);
		apply(&mut engine, &mut store, vec![Add((1, 99))]);
		let out = apply(&mut engine, &mut store, vec![Remove((1, 10))]);
		assert_eq!(kinds(&out), vec![EmitKind::Update]);
		assert_eq!(engine.load_entries(&mut store, window_group()).unwrap(), BTreeMap::from([(1, 99)]));
	}

	#[test]
	fn remove_on_empty_window_counts_a_dropped_retraction() {
		// A retraction into an empty window has nothing to undo; it must be counted, never applied.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		let out = apply(&mut engine, &mut store, vec![Remove((1, 10))]);
		assert!(out.is_empty(), "an empty window publishes nothing");
		assert_eq!(engine.dropped_retractions(), 1);
	}

	#[test]
	fn apply_never_range_reads() {
		// A range read per batch brings back the whole-window cost this engine exists to remove.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		apply(&mut engine, &mut store, vec![Add((1, 10)), Add((2, 20)), Add((3, 30))]);
		assert_eq!(engine.load_entries(&mut store, window_group()).unwrap().len(), 3, "precondition: entries exist");
		let visited = store.rows_visited();
		apply(&mut engine, &mut store, vec![Add((4, 40)), Remove((1, 10))]);
		assert_eq!(store.rows_visited(), visited, "apply must touch entries by point reads only");
	}

	#[test]
	fn entries_and_header_are_reaped_with_the_window() {
		// Entries left behind after the reap leak one row per retained key per closed window.
		let mut store = MockStore::default();
		let mut engine = Engine::new(test_config());
		let out = apply(&mut engine, &mut store, vec![Add((1, 10)), Add((2, 20))]);
		let (group, slot) = group_slot(&1, at_millis(0));
		assert_eq!(out[0].group_id, group);
		assert_eq!(engine.load_entries(&mut store, group).unwrap().len(), 2, "precondition: entries exist");
		engine.reindex_window(&mut store, &1, at_millis(0), group, &slot, None, Some(10)).unwrap();

		let expired = engine.expire(&mut store, 10).unwrap();
		assert_eq!(expired.len(), 1);
		enqueue(&mut store, expired[0].group_id).unwrap();
		reap_group(&mut store, expired[0].group_id, &mut StoreReaper, 256).unwrap();

		assert!(engine.load_entries(&mut store, group).unwrap().is_empty(), "every entry is reaped");
		let header: Option<RetainedHeader> =
			get(&mut store, &WindowStateKey::new(KeyspaceFamily::Guest, group, slot)).unwrap();
		assert!(header.is_none(), "the header is reaped");
	}
}
